##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import copy

from .composite import CompositeType
from ..error import SchemaValidationError

class ChoiceType(CompositeType):
    __slots__ = ['choice', 'checked']

    def __init__(self, schema):
        super().__init__(schema)
        self.choice = None
        self.checked = {}

    def load(self, dct):
        super().load(dct)

        self.choice = []
        for choice in dct['choice']:
            self.choice.append(self.schema._build(choice))


    def check(self, node):
        super().check(node)

        """
        did = id(node)
        if did in self.checked:
            return node
        self.checked[did] = True
        """

        errors = []
        tmp = None

        for choice in self.choice:
            try:
                tmp = copy.deepcopy(node)
                tmp = choice.check(tmp)
            except SchemaValidationError as error:
                errors.append(str(error))
            else:
                break
        else:
            raise SchemaValidationError('Choice block errors:\n' + '\n'.join(errors), node)

        node.value = tmp.value
        node.tag = tmp.tag
        node.tags = getattr(tmp, 'tags', None)

        return node
