##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import SchemaScalarType
from ...error import SchemaValidationError

class IntType(SchemaScalarType):
    def check(self, node):
        if not self.check_tag(node, 'tag:yaml.org,2002:int'):
            raise SchemaValidationError('expected integer', node)

        return super().check(node)
