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


from __future__ import annotations

import copy

from edb.common import checked
from edb.common import struct

from edb.edgeql import ast as qlast
from edb.edgeql import codegen as qlcodegen
from edb.edgeql import parser as qlparser

from . import abc as s_abc
from . import objects as so


class Expression(struct.MixedStruct, s_abc.ObjectContainer):
    text = struct.Field(str, frozen=True)
    origtext = struct.Field(str, default=None, frozen=True)
    refs = struct.Field(so.ObjectSet, coerce=True, default=None, frozen=True)

    def __init__(self, *args, _qlast=None, _irast=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._qlast = _qlast
        self._irast = _irast

    def __getstate__(self):
        return {
            'text': self.text,
            'origtext': self.origtext,
            'refs': self.refs,
            '_qlast': None,
            '_irast': None,
        }

    @property
    def qlast(self):
        if self._qlast is None:
            self._qlast = qlparser.parse_fragment(self.text)
        return self._qlast

    @property
    def irast(self):
        return self._irast

    def is_compiled(self) -> bool:
        return self.refs is not None

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
    def from_ast(cls, qltree, schema, modaliases, *, as_fragment=False):
        orig_text = qlcodegen.generate_source(qltree, pretty=False)
        if not as_fragment:
            qltree = imprint_expr_context(qltree, modaliases)
        norm_text = qlcodegen.generate_source(qltree, pretty=False)

        return cls(
            text=norm_text,
            origtext=orig_text,
            _qlast=qltree,
        )

    @classmethod
    def compiled(cls, expr, schema, *,
                 as_fragment=False,
                 modaliases=None,
                 parent_object_type=None,
                 anchors=None,
                 path_prefix_anchor=None,
                 allow_generic_type_output=False,
                 func_params=None,
                 singletons=None) -> Expression:

        from edb.edgeql import compiler as qlcompiler

        if as_fragment:
            ir = qlcompiler.compile_ast_fragment_to_ir(
                expr.qlast,
                schema=schema,
                modaliases=modaliases,
                anchors=anchors,
                path_prefix_anchor=path_prefix_anchor,
            )
        else:
            ir = qlcompiler.compile_ast_to_ir(
                expr.qlast,
                schema=schema,
                modaliases=modaliases,
                anchors=anchors,
                path_prefix_anchor=path_prefix_anchor,
                func_params=func_params,
                parent_object_type=parent_object_type,
                allow_generic_type_output=allow_generic_type_output,
                singletons=singletons,
            )

        return cls(
            text=expr.text,
            origtext=expr.origtext,
            refs=so.ObjectSet.create(schema, ir.schema_refs),
            _qlast=expr.qlast,
            _irast=ir,
        )

    @classmethod
    def from_ir(cls, expr, ir, schema) -> Expression:
        return cls(
            text=expr.text,
            origtext=expr.origtext,
            refs=so.ObjectSet.create(schema, ir.schema_refs),
            _qlast=expr.qlast,
            _irast=ir,
        )

    @classmethod
    def from_expr(cls, expr, schema) -> Expression:
        return cls(
            text=expr.text,
            origtext=expr.origtext,
            refs=(
                so.ObjectSet.create(schema, expr.refs.objects(schema))
                if expr.refs is not None else None
            ),
            _qlast=expr._qlast,
            _irast=expr._irast,
        )

    def _reduce_to_ref(self, schema):
        return type(self)(
            text=self.text,
            origtext=self.origtext,
            refs=so.ObjectSet.create(
                schema,
                (scls._reduce_to_ref(schema)[0]
                 for scls in self.refs.objects(schema))
            ) if self.refs is not None else None
        ), self


class ExpressionList(checked.FrozenCheckedList[Expression]):

    @staticmethod
    def merge_values(target, sources, field_name, *, schema):
        result = target.get_explicit_field_value(schema, field_name, None)
        for source in sources:
            theirs = source.get_explicit_field_value(schema, field_name, None)
            if theirs:
                if result is None:
                    result = theirs[:]
                else:
                    result.extend(theirs)

        return result

    @classmethod
    def compare_values(cls, ours, theirs, *,
                       our_schema, their_schema, context, compcoef):
        """See the comment in Object.compare_values"""
        if not ours and not theirs:
            basecoef = 1.0
        elif (not ours or not theirs) or (len(ours) != len(theirs)):
            basecoef = 0.2
        else:
            similarity = []

            for expr1, expr2 in zip(ours, theirs):
                similarity.append(
                    Expression.compare_values(
                        expr1, expr2, our_schema=our_schema,
                        their_schema=their_schema, context=context,
                        compcoef=compcoef))

            basecoef = sum(similarity) / len(similarity)

        return basecoef + (1 - basecoef) * compcoef


def imprint_expr_context(qltree, modaliases):
    # Imprint current module aliases as explicit
    # alias declarations in the expression.

    if (isinstance(qltree, qlast.BaseConstant)
            or qltree is None
            or (isinstance(qltree, qlast.Set)
                and not qltree.elements)
            or (isinstance(qltree, qlast.Array)
                and all(isinstance(el, qlast.BaseConstant)
                        for el in qltree.elements))):
        # Leave constants alone.
        return qltree

    if not isinstance(qltree, qlast.Command):
        qltree = qlast.SelectQuery(result=qltree, implicit=True)
    else:
        qltree = copy.copy(qltree)
        qltree.aliases = list(qltree.aliases)

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


def get_expr_referrers(schema, obj):
    """Return schema referrers with refs in expressions."""

    refs = schema.get_referrers_ex(obj)
    result = {}

    for (mcls, fn), referrers in refs.items():
        field = mcls.get_field(fn)
        if issubclass(field.type, (Expression, ExpressionList)):
            result.update({ref: fn for ref in referrers})

    return result
