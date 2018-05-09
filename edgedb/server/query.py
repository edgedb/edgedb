#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


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
