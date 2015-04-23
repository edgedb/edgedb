##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import SchemaTextType
from ...error import SchemaValidationError

class TextType(SchemaTextType):
    def check(self, node):
        if not self.check_tag(node, 'tag:yaml.org,2002:int', 'tag:yaml.org,2002:float',
                                    'tag:yaml.org,2002:str'):
            raise SchemaValidationError('expected text (number or str)', node)

        super().check(node)

        node.tag = 'tag:yaml.org,2002:str'
        node.value = str(node.value)
        return node
