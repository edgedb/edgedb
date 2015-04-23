##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import SchemaType
from ..error import SchemaValidationError


class CompositeType(SchemaType):
    __slots__ = ['checked']

    def __init__(self, schema):
        super().__init__(schema)
        self.checked = {}

    def check_constraints(self, node):
        if 'min-length' in self.constraints:
            if len(node.value) < self.constraints['min-length']:
                raise SchemaValidationError('the number of elements in mapping must not be less than %d'
                                            % self.constraints['min-length'], node)

        if 'max-length' in self.constraints:
            if len(node.value) > self.constraints['max-length']:
                raise SchemaValidationError('the number of elements in mapping must not exceed %d'
                                            % self.constraints['max-length'], node)

