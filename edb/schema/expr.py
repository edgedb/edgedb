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

from edb.edgeql import ast as qlast_
from edb.edgeql import codegen as qlcodegen
from edb.edgeql import parser as qlparser
from typing import *

from . import abc as s_abc
from . import objects as so


if TYPE_CHECKING:
    from edb.schema import types as s_types
    from edb.schema import schema as s_schema
    from edb.schema import functions as s_func
    from edb.ir import ast as irast_


class Expression(struct.MixedStruct, s_abc.ObjectContainer, s_abc.Expression):
    text = struct.Field(str, frozen=True)
    origtext = struct.Field(str, default=None, frozen=True)
    # mypy wants an argument to the ObjectSet generic, but
    # that wouldn't work for struct.Field, since subscripted
    # generics are not types.
    refs = struct.Field(
        so.ObjectSet,  # type: ignore
        coerce=True,
        default=None,
        frozen=True,
    )

    def __init__(self,
                 *args: Any,
                 _qlast: Optional[qlast_.Base] = None,
                 _irast: Optional[irast_.Command] = None,
                 **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._qlast = _qlast
        self._irast = _irast

    def __getstate__(self) -> Dict[str, Any]:
        return {
            'text': self.text,
            'origtext': self.origtext,
            'refs': self.refs,
            '_qlast': None,
            '_irast': None,
        }

    @property
    def qlast(self) -> qlast_.Base:
        if self._qlast is None:
            self._qlast = qlparser.parse_fragment(self.text)
        return self._qlast

    @property
    def irast(self) -> Optional[irast_.Command]:
        return self._irast

    def is_compiled(self) -> bool:
        return self.refs is not None

    @classmethod
    def compare_values(cls: Type[Expression],
                       ours: Expression,
                       theirs: Expression,
                       *,
                       our_schema: s_schema.Schema,
                       their_schema: s_schema.Schema,
                       context: Any,
                       compcoef: float) -> float:
        if not ours and not theirs:
            return 1.0
        elif not ours or not theirs:
            return compcoef
        elif ours.text == theirs.text:
            return 1.0
        else:
            return compcoef

    @classmethod
    def from_ast(
        cls: Type[Expression],
        qltree: qlast_.Base,
        schema: s_schema.Schema,
        modaliases: Mapping[Optional[str], str],
        *,
        as_fragment: bool = False,
        orig_text: Optional[str] = None,
    ) -> Expression:
        if orig_text is None:
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
    def compiled(
        cls: Type[Expression],
        expr: Expression,
        schema: s_schema.Schema,
        *,
        as_fragment: bool = False,
        modaliases: Optional[Mapping[Optional[str], str]] = None,
        parent_object_type: Optional[so.ObjectMeta] = None,
        anchors: Optional[
            Mapping[irast_.AnchorsKeyType, irast_.AnchorsValueType]
        ] = None,
        path_prefix_anchor: Optional[irast_.AnchorsKeyType] = None,
        allow_generic_type_output: bool = False,
        func_params: Optional[s_func.ParameterLikeList] = None,
        singletons: Sequence[s_types.Type] = (),
    ) -> Expression:

        from edb.edgeql import compiler as qlcompiler
        from edb.ir import ast as irast_

        if as_fragment:
            ir: irast_.Command = qlcompiler.compile_ast_fragment_to_ir(
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

        assert isinstance(ir, irast_.Statement)

        return cls(
            text=expr.text,
            origtext=expr.origtext,
            refs=so.ObjectSet.create(schema, ir.schema_refs),
            _qlast=expr.qlast,
            _irast=ir,
        )

    @classmethod
    def from_ir(cls: Type[Expression],
                expr: Expression,
                ir: irast_.Statement,
                schema: s_schema.Schema) -> Expression:
        return cls(
            text=expr.text,
            origtext=expr.origtext,
            refs=so.ObjectSet.create(schema, ir.schema_refs),
            _qlast=expr.qlast,
            _irast=ir,
        )

    @classmethod
    def from_expr(cls: Type[Expression],
                  expr: Expression,
                  schema: s_schema.Schema) -> Expression:
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

    def _reduce_to_ref(self,
                       schema: s_schema.Schema) -> Tuple[Expression,
                                                         Expression]:
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
    def merge_values(target: so.Object,
                     sources: Sequence[so.Object],
                     field_name: str,
                     *,
                     schema: s_schema.Schema) -> Any:
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
    def compare_values(cls: Type[ExpressionList],
                       ours: Optional[ExpressionList],
                       theirs: Optional[ExpressionList],
                       *,
                       our_schema: s_schema.Schema,
                       their_schema: s_schema.Schema,
                       context: so.ComparisonContext,
                       compcoef: float) -> float:
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


def imprint_expr_context(
    qltree: qlast_.Base,
    modaliases: Mapping[Optional[str], str],
) -> qlast_.Base:
    # Imprint current module aliases as explicit
    # alias declarations in the expression.

    if (isinstance(qltree, qlast_.BaseConstant)
            or qltree is None
            or (isinstance(qltree, qlast_.Set)
                and not qltree.elements)
            or (isinstance(qltree, qlast_.Array)
                and all(isinstance(el, qlast_.BaseConstant)
                        for el in qltree.elements))):
        # Leave constants alone.
        return qltree

    if not isinstance(qltree, qlast_.Command):
        qltree = qlast_.SelectQuery(result=qltree, implicit=True)
    else:
        qltree = copy.copy(qltree)
        qltree.aliases = list(qltree.aliases)

    existing_aliases: Dict[Optional[str], str] = {}
    for alias in qltree.aliases:
        if isinstance(alias, qlast_.ModuleAliasDecl):
            existing_aliases[alias.alias] = alias.module

    aliases_to_add = set(modaliases) - set(existing_aliases)
    for alias_name in aliases_to_add:
        qltree.aliases.append(
            qlast_.ModuleAliasDecl(
                alias=alias_name,
                module=modaliases[alias_name],
            )
        )

    return qltree


def get_expr_referrers(schema: s_schema.Schema,
                       obj: so.Object) -> Dict[so.Object, str]:
    """Return schema referrers with refs in expressions."""

    refs = schema.get_referrers_ex(obj)
    result = {}

    for (mcls, fn), referrers in refs.items():
        field = mcls.get_field(fn)
        if issubclass(field.type, (Expression, ExpressionList)):
            result.update({ref: fn for ref in referrers})

    return result
