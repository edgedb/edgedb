##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import SchemaScalarType
from ...error import SchemaValidationError

class FloatType(SchemaScalarType):
    def check(self, node):
        if node.tag != 'tag:yaml.org,2002:float':
            raise SchemaValidationError('expected float', node)

        return super().check(node)
