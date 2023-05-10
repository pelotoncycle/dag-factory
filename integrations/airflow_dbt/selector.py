from dbt.graph import ResourceTypeSelector
from dbt.node_types import NodeType
from functools import partial

DbtTestSelector = partial(ResourceTypeSelector, resource_types=[NodeType.Test])

DbtRunSelector = partial(ResourceTypeSelector, resource_types=[NodeType.Model])

DbtSeedSelector = partial(ResourceTypeSelector, resource_types=[NodeType.Seed])

DbtSnapshotSelector = partial(ResourceTypeSelector, resource_types=[NodeType.Snapshot])


class DbtBuildSelector(ResourceTypeSelector):

    ALL_RESOURCE_VALUES = frozenset({
        NodeType.Test,
        NodeType.Model,
        NodeType.Seed,
        NodeType.Snapshot
    })

    def __init__(self, parsed_args, graph, manifest, previous_state):
        self.args = parsed_args
        resource_types = self.get_resource_types()
        if resource_types == [NodeType.Test]:
            super(DbtBuildSelector, self).__init__(
                graph=graph,
                manifest=manifest,
                previous_state=previous_state,
                resource_types=[NodeType.Test]
            )
        super(DbtBuildSelector, self).__init__(
            graph=graph,
            manifest=manifest,
            previous_state=previous_state,
            resource_types=resource_types
        )

    def get_resource_types(self):
        if not self.args.resource_types:
            return list(self.ALL_RESOURCE_VALUES)

        values = set(self.args.resource_types)

        if "all" in values:
            values.remove("all")
            values.update(self.ALL_RESOURCE_VALUES)

        return list(values)
