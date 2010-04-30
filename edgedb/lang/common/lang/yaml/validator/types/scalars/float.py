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
        if not self.check_tag(node, 'tag:yaml.org,2002:float'):
            raise SchemaValidationError('expected float', node)

        return super().check(node)
