import json
from argparse import Namespace
from pathlib import Path
from typing import Optional, Any, Mapping, AbstractSet, List
from dbt.node_types import NodeType

from dbt.graph import ResourceTypeSelector

import os
import networkx as nx

from .utils import parse_command
from .constants import (
    MANIFEST_PATH,
    PROJECT_PATH
)
from .convertor import NodeTaskConvertor

from .exceptions import InvalidDbtCommandType

from .selector import (
    DbtTestSelector,
    DbtBuildSelector,
    DbtRunSelector,
    DbtSeedSelector,
    DbtSnapshotSelector
)

import dbt.graph.cli as graph_cli
import dbt.graph.selector as graph_selector
from dbt.contracts.graph.manifest import Manifest, WritableManifest, UniqueID
from dbt.contracts.state import PreviousState
from dbt.graph import SelectionSpec
from dbt.graph.selector_spec import IndirectSelection
from networkx import DiGraph


class DbtJobParser:

    DBT_COMMAND_SELECTOR_MAP = {
        'build': DbtBuildSelector,
        'run': DbtRunSelector,
        'test': DbtTestSelector,
        'seed': DbtSeedSelector,
        'snapshot': DbtSnapshotSelector
    }

    def __init__(self, original_task_conf, original_task_id):
        self.original_task_id = original_task_id
        self.original_task_conf = original_task_conf
        self.manifest = None
        self.full_graph = None
        self.job_specific_graph = None
        self.previous_state = None

        # self.parsed_args = None
        # self.select = None
        # self.exclude = None
        # self.manifest = None
        # self.graph = None
        # self.previous_state = None

    def get_node_selector(self, parsed_command):
        command_type = parsed_command.which
        selector_kwargs = {
            "manifest": self.manifest,
            "graph": self.full_graph,
            "previous_state": self.previous_state
        }
        if command_type not in self.DBT_COMMAND_SELECTOR_MAP:
            raise InvalidDbtCommandType(f"dbt command type {command_type} is not runnable. "
                                        f"Select from run, build, test, seed or snapshot")
        selector: ResourceTypeSelector = self.DBT_COMMAND_SELECTOR_MAP.get(command_type)
        if selector == DbtBuildSelector:
            selector_kwargs["parsed_args"] = parsed_command
        return selector(**selector_kwargs)

    def _runtime_initialize(
            self,
            state_path: Optional[str] = None,
            manifest_json_path: Optional[str] = None,
            manifest_json: Optional[Mapping[str, Any]] = None
    ):
        if state_path is not None:
            self.previous_state = PreviousState(
                path=Path(state_path),
                current_path=Path("/tmp/null")
                if manifest_json_path is None
                else Path(manifest_json_path),
            )

        if manifest_json_path is not None:
            self.manifest = WritableManifest.read_and_check_versions(manifest_json_path)
        elif manifest_json is not None:

            class _DictShim(dict):
                """Shim to enable hydrating a dictionary into a dot-accessible object."""

                def __getattr__(self, item):
                    ret = super().get(item)
                    # allow recursive access e.g. foo.bar.baz
                    return _DictShim(ret) if isinstance(ret, dict) else ret

            self.manifest = Manifest(
                # dbt expects dataclasses that can be accessed with dot notation, not bare dictionaries
                nodes={
                    unique_id: _DictShim(info) for unique_id, info in manifest_json["nodes"].items()
                },
                sources={
                    unique_id: _DictShim(info) for unique_id, info in manifest_json["sources"].items()
                },
                metrics={
                    unique_id: _DictShim(info) for unique_id, info in manifest_json["metrics"].items()
                },
                exposures={
                    unique_id: _DictShim(info) for unique_id, info in manifest_json["exposures"].items()
                },
            )
        else:
            check.failed("Must provide either a manifest_json_path or manifest_json.")
        child_map = self.manifest.child_map
        self.full_graph = graph_selector.Graph(DiGraph(incoming_graph_data=child_map))

    def _select_unique_ids_from_manifest(
            self,
            parsed_command,
            select: str,
            exclude: str,
    ) -> (AbstractSet[str], Manifest):
        """Method to apply a selection string to an existing manifest.json file."""
        # create a parsed selection from the select string
        try:
            from dbt.flags import GLOBAL_FLAGS
        except ImportError:
            # dbt < 1.5.0 compat
            import dbt.flags as GLOBAL_FLAGS
        setattr(GLOBAL_FLAGS, "INDIRECT_SELECTION", IndirectSelection.Eager)
        setattr(GLOBAL_FLAGS, "WARN_ERROR", True)
        parsed_spec: SelectionSpec = graph_cli.parse_union([select], True)

        if exclude:
            parsed_spec = graph_cli.SelectionDifference(
                components=[parsed_spec, graph_cli.parse_union([exclude], True)]
            )
        # execute this selection against the graph
        selector: ResourceTypeSelector = self.get_node_selector(
            parsed_command=parsed_command
        )
        selected = selector.get_selected(parsed_spec)
        return selected

    def expand_dbt_nodes(self):
        parsed: Namespace = parse_command(self.original_task_conf['dbt_command'])
        select = ' '.join(parsed.select) if parsed.select else None
        exclude = ' '.join(parsed.exclude) if parsed.exclude else None

        self._runtime_initialize(
            manifest_json_path=MANIFEST_PATH
        )

        os.chdir(PROJECT_PATH)
        selected_nodes = self._select_unique_ids_from_manifest(
            parsed_command=parsed,
            select=select,
            exclude=exclude
        )
        self.job_specific_graph = self.full_graph.get_subset_graph(selected_nodes)

        if parsed.which == 'build':
            self.add_test_edges()

        # return selected_nodes, self.manifest, self.job_specific_graph
        convertor = NodeTaskConvertor(
            manifest=self.manifest,
            graph=self.job_specific_graph,
            original_task_id=self.original_task_id,
            original_task_conf=self.original_task_conf
        )
        tasks = convertor.get_tasks()
        task_group = convertor.get_task_group()
        return tasks, task_group

    def add_test_edges(self) -> None:
        """This method adds additional edges to the DAG. For a given non-test
        executable node, add an edge from an upstream test to the given node if
        the set of nodes the test depends on is a subset of the upstream nodes
        for the given node."""

        # Given a graph:
        # model1 --> model2 --> model3
        #   |             |
        #   |            \/
        #  \/          test 2
        # test1
        #
        # Produce the following graph:
        # model1 --> model2 --> model3
        #   |       /\    |      /\
        #   |       |    \/      |
        #  \/       |  test2 ----|
        # test1 ----|

        for node_id in self.job_specific_graph:
            # If node is executable (in manifest.nodes) and does _not_
            # represent a test, continue.
            if (
                node_id in self.manifest.nodes
                and self.manifest.nodes[node_id].resource_type != NodeType.Test
            ):
                # Get *everything* upstream of the node
                all_upstream_nodes = nx.traversal.bfs_tree(self.job_specific_graph.graph, node_id, reverse=True)
                # Get the set of upstream nodes not including the current node.
                upstream_nodes = set([n for n in all_upstream_nodes if n != node_id])
                direct_upstream_nodes = self.job_specific_graph.graph.predecessors(node_id)

                # Get all tests that depend on direct upstream nodes.
                upstream_tests = []
                for upstream_node in direct_upstream_nodes:
                    upstream_tests += self._get_tests_for_node(upstream_node)

                for upstream_test in upstream_tests:
                    # Get the set of all nodes that the test depends on
                    # including the upstream_node itself. This is necessary
                    # because tests can depend on multiple nodes (ex:
                    # relationship tests). Test nodes do not distinguish
                    # between what node the test is "testing" and what
                    # node(s) it depends on.
                    test_depends_on = set(self.manifest.nodes[upstream_test].depends_on_nodes)

                    # If the set of nodes that an upstream test depends on
                    # is a subset of all upstream nodes of the current node,
                    # add an edge from the upstream test to the current node.
                    if test_depends_on.issubset(upstream_nodes):
                        self.job_specific_graph.graph.add_edge(upstream_test, node_id)

    def _get_tests_for_node(self, unique_id: UniqueID) -> List[UniqueID]:
        """Get a list of tests that depend on the node with the
        provided unique id"""

        tests = []
        if unique_id in self.manifest.child_map:
            for child_unique_id in self.manifest.child_map[unique_id]:
                if child_unique_id.startswith("test."):
                    tests.append(child_unique_id)

        return tests








