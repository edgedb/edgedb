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

from edb.edgeql import ast as qlast
from edb.edgeql import codegen as qlcodegen

from . import objects as so


class Expression(struct.MixedStruct):
    text = struct.Field(str, frozen=True)
    origtext = struct.Field(str, default=None, frozen=True)
    irast = struct.Field(object, default=None, frozen=True)
    refs = struct.Field(so.ObjectSet, coerce=True, default=None, frozen=True)

    def __init__(self, *args, _qlast=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._qlast = _qlast

    @property
    def qlast(self):
        return self._qlast

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

    @classmethod
    def from_ast(cls, qltree, schema, modaliases):
        norm_tree = imprint_expr_context(qltree, modaliases)
        norm_text = qlcodegen.generate_source(norm_tree, pretty=False)

        return cls(
            text=norm_text,
            origtext=qlcodegen.generate_source(qltree),
            _qlast=norm_tree,
        )


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


def imprint_expr_context(qltree, modaliases):
    # Imprint current module aliases as explicit
    # alias declarations in the expression.

    if (isinstance(qltree, qlast.BaseConstant)
            or qltree is None
            or (isinstance(qltree, qlast.Array)
                and all(isinstance(el, qlast.BaseConstant)
                        for el in qltree.elements))):
        # Leave constants alone.
        return qltree

    if not isinstance(qltree, qlast.Statement):
        qltree = qlast.SelectQuery(result=qltree, implicit=True)

    existing_aliases = {}
    for alias in qltree.aliases:
        if isinstance(alias, qlast.ModuleAliasDecl):
            existing_aliases[alias.alias] = alias.module

    aliases_to_add = set(modaliases) - set(existing_aliases)
    for alias in aliases_to_add:
        qltree.aliases.append(
            qlast.ModuleAliasDecl(
                alias=alias,
                module=modaliases[alias],
            )
        )

    return qltree
