##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections


class Query:
    def __init__(self, text, *, argument_types, result_types):
        self.text = text
        self.argument_types = argument_types
        self.result_types = result_types

    def first(self, **kwargs):
        raise NotImplementedError

    def rows(self, **kwargs):
        raise NotImplementedError

    def prepare(self, session):
        raise NotImplementedError

    def prepare_partial(self, session):
        raise NotImplementedError

    def _describe_output(self):
        return collections.OrderedDict(self.result_types)

    def describe_output(self, session):
        result = self._describe_output()

        for field, (expr_type, expr_kind) in tuple(result.items()):
            if expr_type is None: # XXX get_expr_type
                continue

            result[field] = (session.schema.get(expr_type), expr_kind)

        return result

    def _describe_arguments(self):
        return collections.OrderedDict(self.argument_types)

    def describe_arguments(self, session):
        result = self._describe_arguments()

        for field, expr_type in tuple(result.items()):
            if expr_type is None: # XXX get_expr_type
                continue

            if isinstance(expr_type, tuple):
                expr_type = (expr_type[0], session.schema.get(expr_type[1]))
            else:
                expr_type = session.schema.get(expr_type)

            result[field] = expr_type

        return result
