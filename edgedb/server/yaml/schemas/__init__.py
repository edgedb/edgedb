from semantix.caos.backends.meta import RealmMeta
from semantix.caos.backends.yaml import ImportContext
from semantix.lang.yaml.loader import AttributeMappingNode
from .semantics import Semantics
from .data import Data


class Semantics(Semantics):
    def check(self, node):
        node = super().check(node)
        return AttributeMappingNode.from_map_node(node)

    def get_import_context_class(self):
        return ImportContext
