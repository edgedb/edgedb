#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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


from edb.common import struct
from edb.common import typed
from edb.edgeql import ast as ql_ast

from . import objects as so


class Expression(struct.Struct):
    text = struct.Field(str, frozen=True)
    qlast = struct.Field(ql_ast.Base, default=None, frozen=True)
    irast = struct.Field(object, default=None, frozen=True)
    refs = struct.Field(so.ObjectSet, coerce=True, default=None, frozen=True)

    @classmethod
    def compare_values(cls, ours, theirs, *,
                       our_schema, their_schema, context, compcoef):
        if not ours and not theirs:
            return 1.0
        elif not ours or not theirs:
            return compcoef
        elif ours.text == theirs.text:
            return 1.0
        else:
            return compcoef


class ExpressionList(typed.FrozenTypedList, type=Expression):

    @classmethod
    def merge_values(cls, target, sources, field_name, *, schema):
        result = target.get_explicit_field_value(schema, field_name, None)
        for source in sources:
            theirs = source.get_explicit_field_value(schema, field_name, None)
            if theirs:
                if result is None:
                    result = theirs[:]
                else:
                    result.extend(theirs)

        return result
