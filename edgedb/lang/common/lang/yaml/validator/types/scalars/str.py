##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import SchemaTextType
from ...error import SchemaValidationError

class StringType(SchemaTextType):
    def check(self, node):
        result = super().check(node)

        if not self.check_tag(node, 'tag:yaml.org,2002:str'):
            raise SchemaValidationError('expected string, got {!r}'.format(node), node)

        return result
