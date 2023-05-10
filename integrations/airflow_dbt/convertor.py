from typing import Dict, Any, Optional, List, Union

from dbt.contracts.graph.parsed import ParsedNode
from dbt.node_types import NodeType
# from dbt_cli_operator import DbtCLIOperator, DbtTestOperator, DbtCLISensorOperator
from airflow.utils.task_group import TaskGroup
from .exceptions import ConvertZeroTask
from dbt.contracts.graph.manifest import Manifest, WritableManifest

from .constants import DBT_OPERATOR_IMPORT_PATHS


class NodeTaskConvertor:
    """
    A class to convert each DBT node into an airflow task, and group all the tasks into a task_group
    """
    RESOURCE_TYPE_COMMAND_MAP = {
        NodeType.Model: "run",
        NodeType.Test: "test",
        NodeType.Seed: "seed",
        NodeType.Snapshot: "snapshot"
    }
    DISCARD_TASK_PARAMS = ('operator', 'dbt_command', 'dbt_threads', 'dependencies')

    def __init__(self, manifest, graph, original_task_conf, original_task_id):
        self.original_task_conf = original_task_conf
        self.original_task_id = original_task_id
        self.manifest: Union[Manifest, WritableManifest] = manifest.nodes
        self.graph = graph
        self.dbt_threads: Optional[int] = None
        self.dependencies: Optional[List[str]] = None

    def _clean_task_conf(self, task_conf: Dict[str, Any]):
        return {k: v for k, v in task_conf.items() if k not in self.DISCARD_TASK_PARAMS}

    def get_tasks(self):
        cleaned_task_conf = self._clean_task_conf(self.original_task_conf)
        nodes = self.graph.nodes()
        constructed_tasks = {}

        for node in nodes:
            base_task_conf = {**cleaned_task_conf}

            node_info: ParsedNode = self.manifest[node]
            node_type: NodeType = node_info.resource_type
            node_task_id: str = node_info.identifier
            base_task_conf['task_group_name'] = self.original_task_id
            base_task_conf['dbt_command'] = f"dbt {self.RESOURCE_TYPE_COMMAND_MAP[node_type]} --select {node_task_id}"
            node_dependencies: Optional[List[str]] = self.graph.graph.predecessors(node)

            test_dependencies = []
            other_dependencies = []
            for dependency in node_dependencies:
                dependency_info: ParsedNode = self.manifest[dependency]
                dependency_type = dependency_info.resource_type
                if dependency_type == NodeType.Test:
                    test_dependencies.append(dependency_info.identifier)
                else:
                    other_dependencies.append(dependency_info.identifier)
            base_task_conf['dependencies'] = other_dependencies

            if node_type == NodeType.Test:  # test nodes
                base_task_conf['operator'] = DBT_OPERATOR_IMPORT_PATHS["DbtTestOperator"]
            else:   # other nodes
                # other nodes that have upstream tests
                if test_dependencies:
                    base_task_conf['operator'] = DBT_OPERATOR_IMPORT_PATHS["DbtCLISensorOperator"]
                    base_task_conf['poke_tasks'] = test_dependencies
                else:
                    base_task_conf['operator'] = DBT_OPERATOR_IMPORT_PATHS['DbtCLIOperator']

            constructed_tasks[node_task_id] = base_task_conf
        return constructed_tasks

    def get_task_group(self):
        return {
            self.original_task_id: {
                'dependencies': self.original_task_conf.get('dependencies', {})
            }
        }


