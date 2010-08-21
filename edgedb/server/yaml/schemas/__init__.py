##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos.proto import ImportContext
from semantix.utils.lang.yaml.loader import AttributeMappingNode
from .semantics import Semantics
from .delta import Delta


class Semantics(Semantics):
    def check(self, node):
        node = super().check(node)
        return AttributeMappingNode.from_map_node(node)

    def get_import_context_class(self):
        return ImportContext


class Delta(Delta):
    def check(self, node):
        node = super().check(node)
        return AttributeMappingNode.from_map_node(node)
