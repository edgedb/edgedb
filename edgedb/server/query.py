##
# Copyright (c) 2011-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import collections

from edgedb.lang.schema import objects as s_obj


class Query:
    def __init__(self, text, *, argument_types):
        self.text = text
        self.argument_types = argument_types

    def first(self, **kwargs):
        raise NotImplementedError

    def rows(self, **kwargs):
        raise NotImplementedError

    def prepare(self, session):
        raise NotImplementedError

    def prepare_partial(self, session):
        raise NotImplementedError

    def _describe_arguments(self):
        return collections.OrderedDict(self.argument_types)

    def describe_arguments(self, session):
        result = self._describe_arguments()

        for field, expr_type in tuple(result.items()):
            if expr_type is None:  # XXX get_expr_type
                continue

            if isinstance(expr_type, tuple):
                if expr_type[1] == 'type':
                    expr_typ = s_obj.Object
                else:
                    expr_typ = session.schema.get(expr_type[1])

                expr_type = (expr_type[0], expr_typ)
            else:
                if expr_type[1] == 'type':
                    expr_type = s_obj.Object
                else:
                    expr_type = session.schema.get(expr_type)

            result[field] = expr_type

        return result
