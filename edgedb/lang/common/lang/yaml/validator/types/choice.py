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
    __slots__ = 'choice',

    def __init__(self, schema):
        super().__init__(schema)
        self.choice = None

    def load(self, dct):
        super().load(dct)

        self.choice = []
        for choice in dct['choice']:
            self.choice.append(self.schema._build(choice))

    def check(self, node):
        try:
            choice_profile = self.schema._choice_profile
        except AttributeError:
            choice_profile = self.schema._choice_profile = {
                'level': 0,
                'choices': {
                    'sibling': -1,
                    'nodes': [],
                },
                'stack': []
            }

            choice_profile['stack'].append(choice_profile['choices'])

        tree_lvl = choice_profile['stack'][-1]
        tree_lvl['sibling'] += 1

        try:
            tree_node = tree_lvl['nodes'][tree_lvl['sibling']]
        except IndexError:
            tree_node = {
                'self': None,
                'children': {
                    'sibling': -1,
                    'nodes': []
                }
            }
            tree_lvl['nodes'].append(tree_node)

        errors = []
        tmp = None
        exc = None

        valid_choice = tree_node['self']
        subtree = tree_node['children']

        if valid_choice is not None:
            choice_profile['stack'].append(subtree)
            choice_profile['level'] += 1

            subtree['sibling'] = -1

            super().check(node)

            node = self.choice[valid_choice].check(node)

            subtree['sibling'] = -1
            choice_profile['level'] -= 1
            choice_profile['stack'].pop()
        else:
            choice_profile['stack'].append(subtree)
            choice_profile['level'] += 1

            for i, choice in enumerate(self.choice):
                subtree['sibling'] = -1
                subtree['nodes'][:] = []

                tmp = copy.deepcopy(node)

                try:
                    tmp = choice.check(tmp)
                except SchemaValidationError as error:
                    errors.append(str(error))
                else:
                    tree_node['self'] = i
                    break
            else:
                subtree['nodes'][:] = []
                msg = 'Choice block errors:\n' + '\n'.join(errors)
                exc = SchemaValidationError(msg, node)

            subtree['sibling'] = -1

            choice_profile['level'] -= 1
            choice_profile['stack'].pop()

            if exc is None:
                if choice_profile['level'] == 0:
                    tree_lvl['sibling'] -= 1
                    self.check(node)

        if exc is not None:
            raise exc
        else:
            return node
