##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import yaml

from .composite import CompositeType
from ..error import SchemaValidationError

class SequenceType(CompositeType):
    __slots__ = ['sequence_type']

    def __init__(self, schema):
        super(SequenceType, self).__init__(schema)
        self.sequence_type = None

    def default_node_type(self):
        return yaml.nodes.SequenceNode

    def load(self, dct):
        super(SequenceType, self).load(dct)

        self.sequence_type = self.schema._build(dct['sequence'][0])

    def check(self, node):
        super(SequenceType, self).check(node)

        """ XXX:
        did = id(node)
        if did in self.checked:
            return node
        self.checked[did] = True
        """

        if not isinstance(node, yaml.nodes.SequenceNode):
            raise SchemaValidationError('list expected', node)

        self.sequence_type.begin_checks()
        new_list = []
        for i, value in enumerate(node.value):
            new_list.append(self.sequence_type.check(value))

        node.value = new_list
        self.sequence_type.end_checks()

        return node
