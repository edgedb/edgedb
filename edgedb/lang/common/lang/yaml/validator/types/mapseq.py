##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import yaml

from .composite import CompositeType
from ..error import SchemaValidationError


class MappingSequenceType(CompositeType):
    __slots__ = ['mapping_type']

    def __init__(self, schema):
        super().__init__(schema)
        self.mapping_type = None

    def default_node_type(self):
        return yaml.nodes.SequenceNode

    def load(self, dct):
        super().load(dct)
        self._init_constrainrs(('max-length', 'min-length'), dct)
        self.mapping_type = self.schema._build(dct['mapping']['='])

    def check(self, node):
        super().check(node)

        if not isinstance(node, yaml.nodes.MappingNode):
            raise SchemaValidationError('series of mappings expected', node)

        self.check_constraints(node)

        self.mapping_type.begin_checks()
        new_list = []

        for key, value in node.value:
            new_list.append((key, self.mapping_type.check(value)))

        node.value = new_list
        self.mapping_type.end_checks()

        if (node.tag.startswith('tag:semantix.sprymix.com,2009/semantix/class/derive:')
                or node.tag.startswith('tag:semantix.sprymix.com,2009/semantix/object/create:')):
            node.tags.append('tag:semantix.sprymix.com,2009/semantix/mapseq')
        else:
            self.push_tag(node, 'tag:semantix.sprymix.com,2009/semantix/mapseq')

        return node
