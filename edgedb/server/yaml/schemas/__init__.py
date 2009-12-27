from semantix.lang.yaml.loader import AttributeMappingNode
from .semantics import Semantics

class Semantics(Semantics):
    def check(self, node):
        node = super().check(node)
        return AttributeMappingNode.from_map_node(node)
