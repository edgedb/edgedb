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

import collections
import typing

from edb.common import parsing, span

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb import errors

from . import keywords
from . import precedence
from . import tokens

from .precedence import *  # NOQA
from .tokens import *  # NOQA


class Nonterm(parsing.Nonterm, is_internal=True):
    pass


class ListNonterm(parsing.ListNonterm, element=None, is_internal=True):
    pass


class ExprStmt(Nonterm):
    val: qlast.Query

    def reduce_WithBlock_ExprStmtCore(self, *kids):
        self.val = kids[1].val
        self.val.aliases = kids[0].val.aliases

    @parsing.inline(0)
    def reduce_ExprStmtCore(self, *kids):
        pass


class ExprStmtCore(Nonterm):
    val: qlast.Query

    @parsing.inline(0)
    def reduce_SimpleFor(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_SimpleSelect(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_SimpleGroup(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_InternalGroup(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_SimpleInsert(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_SimpleUpdate(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_SimpleDelete(self, *kids):
        pass


class AliasedExpr(Nonterm):
    val: qlast.AliasedExpr

    def reduce_Identifier_ASSIGN_Expr(self, *kids):
        self.val = qlast.AliasedExpr(alias=kids[0].val, expr=kids[2].val)


# NOTE: This is intentionally not an AST node, since this structure never
# makes it to the actual AST and exists solely for parser convenience.
AliasedExprSpec = collections.namedtuple(
    'AliasedExprSpec', ['alias', 'expr'], module=__name__)


class OptionallyAliasedExpr(Nonterm):
    val: AliasedExprSpec

    def reduce_AliasedExpr(self, *kids):
        val = kids[0].val
        self.val = AliasedExprSpec(alias=val.alias, expr=val.expr)

    def reduce_Expr(self, *kids):
        self.val = AliasedExprSpec(alias=None, expr=kids[0].val)


class AliasedExprList(ListNonterm, element=AliasedExpr,
                      separator=tokens.T_COMMA, allow_trailing_separator=True):
    val: typing.List[qlast.AliasedExpr]


class GroupingIdent(Nonterm):
    val: qlast.GroupingAtom

    def reduce_Identifier(self, *kids):
        self.val = qlast.ObjectRef(name=kids[0].val)

    def reduce_DOT_Identifier(self, *kids):
        self.val = qlast.Path(
            partial=True,
            steps=[qlast.Ptr(name=kids[1].val)],
        )

    def reduce_AT_Identifier(self, *kids):
        self.val = qlast.Path(
            partial=True,
            steps=[
                qlast.Ptr(
                    name=kids[1].val,
                    type='property',
                    span=kids[1].span,
                )
            ]
        )


class GroupingIdentList(ListNonterm, element=GroupingIdent,
                        separator=tokens.T_COMMA):
    val: typing.List[qlast.GroupingAtom]


class GroupingAtom(Nonterm):
    val: qlast.GroupingAtom

    @parsing.inline(0)
    def reduce_GroupingIdent(self, *kids):
        pass

    def reduce_LPAREN_GroupingIdentList_RPAREN(self, *kids):
        self.val = qlast.GroupingIdentList(elements=kids[1].val)


class GroupingAtomList(
        ListNonterm, element=GroupingAtom, separator=tokens.T_COMMA,
        allow_trailing_separator=True):
    val: typing.List[qlast.GroupingAtom]


class GroupingElement(Nonterm):
    val: qlast.GroupingElement

    def reduce_GroupingAtom(self, *kids):
        self.val = qlast.GroupingSimple(element=kids[0].val)

    def reduce_LBRACE_GroupingElementList_RBRACE(self, *kids):
        self.val = qlast.GroupingSets(sets=kids[1].val)

    def reduce_ROLLUP_LPAREN_GroupingAtomList_RPAREN(self, *kids):
        self.val = qlast.GroupingOperation(oper='rollup', elements=kids[2].val)

    def reduce_CUBE_LPAREN_GroupingAtomList_RPAREN(self, *kids):
        self.val = qlast.GroupingOperation(oper='cube', elements=kids[2].val)


class GroupingElementList(
        ListNonterm, element=GroupingElement, separator=tokens.T_COMMA,
        allow_trailing_separator=True):
    val: typing.List[qlast.GroupingElement]


class OptionalOptional(Nonterm):
    val: bool

    def reduce_OPTIONAL(self, *kids):
        self.val = True

    def reduce_empty(self, *kids):
        self.val = False


class SimpleFor(Nonterm):
    val: qlast.ForQuery

    def reduce_ForIn(self, *kids):
        r"%reduce FOR OptionalOptional Identifier IN AtomicExpr UNION Expr"
        _, optional, iterator_alias, _, iterator, _, body = kids
        self.val = qlast.ForQuery(
            optional=optional.val,
            iterator_alias=iterator_alias.val,
            iterator=iterator.val,
            result=body.val,
        )

    def reduce_ForInStmt(self, *kids):
        r"%reduce FOR OptionalOptional Identifier IN AtomicExpr ExprStmt"
        _, optional, iterator_alias, _, iterator, body = kids
        self.val = qlast.ForQuery(
            has_union=False,
            optional=optional.val,
            iterator_alias=iterator_alias.val,
            iterator=iterator.val,
            result=body.val,
        )


class SimpleSelect(Nonterm):
    val: qlast.SelectQuery

    def reduce_Select(self, *kids):
        r"%reduce SELECT OptionallyAliasedExpr \
                  OptFilterClause OptSortClause OptSelectLimit"

        offset, limit = kids[4].val

        if offset is not None or limit is not None:
            subj = qlast.SelectQuery(
                result=kids[1].val.expr,
                result_alias=kids[1].val.alias,
                where=kids[2].val,
                orderby=kids[3].val,
                implicit=True,
            )

            self.val = qlast.SelectQuery(
                result=subj,
                offset=offset,
                limit=limit,
            )
        else:
            self.val = qlast.SelectQuery(
                result=kids[1].val.expr,
                result_alias=kids[1].val.alias,
                where=kids[2].val,
                orderby=kids[3].val,
            )


class ByClause(Nonterm):
    val: typing.List[qlast.GroupingElement]

    @parsing.inline(1)
    def reduce_BY_GroupingElementList(self, *kids):
        pass


class UsingClause(Nonterm):
    val: typing.List[qlast.AliasedExpr]

    @parsing.inline(1)
    def reduce_USING_AliasedExprList(self, *kids):
        pass


class OptUsingClause(Nonterm):
    val: typing.List[qlast.AliasedExpr]

    @parsing.inline(0)
    def reduce_UsingClause(self, *kids):
        pass

    def reduce_empty(self, *kids):
        self.val = None


class SimpleGroup(Nonterm):
    val: qlast.GroupQuery

    def reduce_Group(self, *kids):
        r"%reduce GROUP OptionallyAliasedExpr \
                  OptUsingClause \
                  ByClause"
        self.val = qlast.GroupQuery(
            subject=kids[1].val.expr,
            subject_alias=kids[1].val.alias,
            using=kids[2].val,
            by=kids[3].val,
        )


class OptGroupingAlias(Nonterm):
    val: typing.Optional[qlast.GroupQuery]

    @parsing.inline(1)
    def reduce_COMMA_Identifier(self, *kids):
        pass

    def reduce_empty(self, *kids):
        self.val = None


class InternalGroup(Nonterm):
    val: qlast.InternalGroupQuery

    def reduce_InternalGroup(self, *kids):
        r"%reduce FOR GROUP OptionallyAliasedExpr \
                  UsingClause \
                  ByClause \
                  IN Identifier OptGroupingAlias \
                  UNION OptionallyAliasedExpr \
                  OptFilterClause OptSortClause \
        "
        self.val = qlast.InternalGroupQuery(
            subject=kids[2].val.expr,
            subject_alias=kids[2].val.alias,
            using=kids[3].val,
            by=kids[4].val,
            group_alias=kids[6].val,
            grouping_alias=kids[7].val,
            result_alias=kids[9].val.alias,
            result=kids[9].val.expr,
            where=kids[10].val,
            orderby=kids[11].val,
        )


class SimpleInsert(Nonterm):
    val: qlast.InsertQuery

    def reduce_Insert(self, *kids):
        r'%reduce INSERT Expr OptUnlessConflictClause'

        subj = kids[1].val
        unless_conflict = kids[2].val

        if isinstance(subj, qlast.Shape):
            if not subj.expr:
                raise errors.EdgeQLSyntaxError(
                    "insert shape expressions must have a type name",
                    span=subj.span
                )
            subj_path = subj.expr
            shape = subj.elements
        else:
            subj_path = subj
            shape = []

        if isinstance(subj_path, qlast.Path) and \
                len(subj_path.steps) == 1 and \
                isinstance(subj_path.steps[0], qlast.ObjectRef):
            objtype = subj_path.steps[0]
        elif isinstance(subj_path, qlast.IfElse):
            # Insert attempted on something that looks like a conditional
            # expression. Aside from it being an error, it also seems that
            # the intent was to insert something conditionally.
            raise errors.EdgeQLSyntaxError(
                f"INSERT only works with object types, not conditional "
                f"expressions",
                hint=(
                    f"To resolve this try surrounding the INSERT branch of "
                    f"the conditional expression with parentheses. This way "
                    f"the INSERT will be triggered conditionally in one of "
                    f"the branches."
                ),
                span=subj_path.span)
        else:
            raise errors.EdgeQLSyntaxError(
                f"INSERT only works with object types, not arbitrary "
                f"expressions",
                hint=(
                    f"To resolve this try to surround the entire INSERT "
                    f"statement with parentheses in order to separate it "
                    f"from the rest of the expression."
                ),
                span=subj_path.span)

        self.val = qlast.InsertQuery(
            subject=objtype,
            shape=shape,
            unless_conflict=unless_conflict,
        )


class SimpleUpdate(Nonterm):
    val: qlast.UpdateQuery

    def reduce_Update(self, *kids):
        "%reduce UPDATE Expr OptFilterClause SET Shape"
        self.val = qlast.UpdateQuery(
            subject=kids[1].val,
            where=kids[2].val,
            shape=kids[4].val,
        )


class SimpleDelete(Nonterm):
    val: qlast.DeleteQuery

    def reduce_Delete(self, *kids):
        r"%reduce DELETE Expr \
                  OptFilterClause OptSortClause OptSelectLimit"
        self.val = qlast.DeleteQuery(
            subject=kids[1].val,
            where=kids[2].val,
            orderby=kids[3].val,
            offset=kids[4].val[0],
            limit=kids[4].val[1],
        )


WithBlockData = collections.namedtuple(
    'WithBlockData', ['aliases'], module=__name__)


class WithBlock(Nonterm):
    def reduce_WITH_WithDeclList(self, *kids):
        aliases = []
        for w in kids[1].val:
            aliases.append(w)
        self.val = WithBlockData(aliases=aliases)


class AliasDecl(Nonterm):
    def reduce_MODULE_ModuleName(self, *kids):
        self.val = qlast.ModuleAliasDecl(
            module='::'.join(kids[1].val))

    def reduce_Identifier_AS_MODULE_ModuleName(self, *kids):
        self.val = qlast.ModuleAliasDecl(
            alias=kids[0].val,
            module='::'.join(kids[3].val))

    @parsing.inline(0)
    def reduce_AliasedExpr(self, *kids):
        pass


class WithDecl(Nonterm):
    @parsing.inline(0)
    def reduce_AliasDecl(self, *kids):
        pass


class WithDeclList(ListNonterm, element=WithDecl,
                   separator=tokens.T_COMMA, allow_trailing_separator=True):
    pass


class Shape(Nonterm):
    def reduce_LBRACE_RBRACE(self, *kids):
        self.val = []

    @parsing.inline(1)
    def reduce_LBRACE_ShapeElementList_RBRACE(self, *kids):
        pass


class FreeShape(Nonterm):
    def reduce_LBRACE_FreeComputableShapePointerList_RBRACE(self, *kids):
        self.val = qlast.Shape(elements=kids[1].val)


class OptAnySubShape(Nonterm):
    @parsing.inline(1)
    def reduce_COLON_Shape(self, *_):
        pass

    def reduce_empty(self, *kids):
        self.val = []


class ShapeElement(Nonterm):
    def reduce_ShapeElementWithSubShape(self, *kids):
        r"""%reduce ShapePointer \
             OptAnySubShape OptFilterClause OptSortClause OptSelectLimit \
        """
        self.val = kids[0].val
        self.val.elements = kids[1].val
        self.val.where = kids[2].val
        self.val.orderby = kids[3].val
        self.val.offset = kids[4].val[0]
        self.val.limit = kids[4].val[1]

    @parsing.inline(0)
    def reduce_ComputableShapePointer(self, *kids):
        pass


class ShapeElementList(ListNonterm, element=ShapeElement,
                       separator=tokens.T_COMMA, allow_trailing_separator=True):
    pass


class SimpleShapePath(Nonterm):

    def reduce_PathStepName(self, *kids):
        from edb.schema import pointers as s_pointers

        steps = [
            qlast.Ptr(
                name=kids[0].val.name,
                direction=s_pointers.PointerDirection.Outbound,
                span=kids[0].val.span,
            ),
        ]

        self.val = qlast.Path(steps=steps)

    def reduce_AT_PathNodeName(self, *kids):
        self.val = qlast.Path(
            steps=[
                qlast.Ptr(
                    name=kids[1].val.name,
                    type='property',
                    span=kids[1].val.span,
                )
            ]
        )


class SimpleShapePointer(Nonterm):

    def reduce_SimpleShapePath(self, *kids):
        self.val = qlast.ShapeElement(
            expr=kids[0].val
        )


# Shape pointers in free shapes are not allowed to be link
# properties. This is because we need to be able to distinguish
# free shapes from set literals with only one token of lookahead
# (since this is an LL(1) parser) and seeing the := after @ident would
# require two tokens of lookahead.
class FreeSimpleShapePointer(Nonterm):

    def reduce_FreeStepName(self, *kids):
        from edb.schema import pointers as s_pointers

        steps = [
            qlast.Ptr(
                name=kids[0].val.name,
                direction=s_pointers.PointerDirection.Outbound,
                span=kids[0].val.span,
            ),
        ]

        self.val = qlast.ShapeElement(
            expr=qlast.Path(steps=steps)
        )


class ShapePath(Nonterm):
    # A form of Path appearing as an element in shapes.
    #
    # one-of:
    #   __type__
    #   link
    #   @prop
    #   [IS ObjectType].link
    #   [IS Link]@prop - currently not supported
    #   <splat> (see Splat production for possible syntaxes)

    def reduce_PathStepName_OptTypeIntersection(self, *kids):
        from edb.schema import pointers as s_pointers

        steps = [
            qlast.Ptr(
                name=kids[0].val.name,
                direction=s_pointers.PointerDirection.Outbound,
                span=kids[0].val.span,
            ),
        ]

        if kids[1].val is not None:
            steps.append(kids[1].val)

        self.val = qlast.Path(steps=steps)

    @parsing.inline(0)
    def reduce_Splat(self, *kids):
        pass

    def reduce_AT_PathNodeName(self, *kids):
        self.val = qlast.Path(
            steps=[
                qlast.Ptr(
                    name=kids[1].val.name,
                    type='property',
                    span=kids[1].val.span,
                )
            ]
        )

    def reduce_TypeIntersection_DOT_PathStepName_OptTypeIntersection(
            self, *kids):
        from edb.schema import pointers as s_pointers

        steps = [
            kids[0].val,
            qlast.Ptr(
                name=kids[2].val.name,
                direction=s_pointers.PointerDirection.Outbound,
                span=kids[2].val.span,
            ),
        ]

        if kids[3].val is not None:
            steps.append(kids[3].val)

        self.val = qlast.Path(steps=steps)


# N.B. the production verbosity below is necessary due to conflicts,
#      as is the use of PathStepName in place of SimpleTypeName.
class Splat(Nonterm):
    def reduce_STAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(depth=1),
        ])

    def reduce_DOUBLESTAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(depth=2),
        ])

    # Type.*
    def reduce_PathStepName_DOT_STAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                depth=1,
                type=qlast.TypeName(maintype=kids[0].val),
            ),
        ])

    # Type.**
    def reduce_PathStepName_DOT_DOUBLESTAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                depth=2,
                type=qlast.TypeName(maintype=kids[0].val),
            ),
        ])

    # [is Foo].*
    def reduce_TypeIntersection_DOT_STAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                depth=1,
                intersection=kids[0].val,
            ),
        ])

    # [is Foo].**
    def reduce_TypeIntersection_DOT_DOUBLESTAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                depth=2,
                intersection=kids[0].val,
            ),
        ])

    # Type[is Foo].*
    def reduce_PathStepName_TypeIntersection_DOT_STAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                depth=1,
                type=qlast.TypeName(maintype=kids[0].val),
                intersection=kids[1].val,
            ),
        ])

    # Type[is Foo].**
    def reduce_PathStepName_TypeIntersection_DOT_DOUBLESTAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                depth=2,
                type=qlast.TypeName(maintype=kids[0].val),
                intersection=kids[1].val,
            ),
        ])

    # module::Type.*
    def reduce_PtrQualifiedNodeName_DOT_STAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                type=qlast.TypeName(maintype=kids[0].val),
                depth=1,
            ),
        ])

    # module::Type.**
    def reduce_PtrQualifiedNodeName_DOT_DOUBLESTAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                type=qlast.TypeName(maintype=kids[0].val),
                depth=2,
            ),
        ])

    # module::Type[is <type-expr>].*
    def reduce_PtrQualifiedNodeName_TypeIntersection_DOT_STAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                depth=1,
                type=qlast.TypeName(maintype=kids[0].val),
                intersection=kids[1].val,
            ),
        ])

    # module::Type[is <type-expr>].**
    def reduce_PtrQualifiedNodeName_TypeIntersection_DOT_DOUBLESTAR(
        self,
        *kids,
    ):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                depth=2,
                type=qlast.TypeName(maintype=kids[0].val),
                intersection=kids[1].val,
            ),
        ])

    # (<type-expr>).*
    def reduce_ParenTypeExpr_DOT_STAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                depth=1,
                type=kids[0].val,
            ),
        ])

    # (<type-expr>).**
    def reduce_ParenTypeExpr_TypeIntersection_DOT_STAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                depth=1,
                type=kids[0].val,
                intersection=kids[1].val,
            ),
        ])

    # (<type-expr>)[is <type-expr>].*
    def reduce_ParenTypeExpr_DOT_DOUBLESTAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                depth=2,
                type=kids[0].val,
            ),
        ])

    # (<type-expr>)[is <type-expr>].**
    def reduce_ParenTypeExpr_TypeIntersection_DOT_DOUBLESTAR(self, *kids):
        self.val = qlast.Path(steps=[
            qlast.Splat(
                depth=2,
                type=kids[0].val,
                intersection=kids[1].val,
            ),
        ])


class ShapePointer(Nonterm):
    def reduce_ShapePath(self, *kids):
        self.val = qlast.ShapeElement(
            expr=kids[0].val
        )


class PtrQualsSpec(typing.NamedTuple):
    required: typing.Optional[bool] = None
    cardinality: typing.Optional[qltypes.SchemaCardinality] = None


class PtrQuals(Nonterm):
    def reduce_OPTIONAL(self, *kids):
        self.val = PtrQualsSpec(required=False)

    def reduce_REQUIRED(self, *kids):
        self.val = PtrQualsSpec(required=True)

    def reduce_SINGLE(self, *kids):
        self.val = PtrQualsSpec(cardinality=qltypes.SchemaCardinality.One)

    def reduce_MULTI(self, *kids):
        self.val = PtrQualsSpec(cardinality=qltypes.SchemaCardinality.Many)

    def reduce_OPTIONAL_SINGLE(self, *kids):
        self.val = PtrQualsSpec(
            required=False, cardinality=qltypes.SchemaCardinality.One)

    def reduce_OPTIONAL_MULTI(self, *kids):
        self.val = PtrQualsSpec(
            required=False, cardinality=qltypes.SchemaCardinality.Many)

    def reduce_REQUIRED_SINGLE(self, *kids):
        self.val = PtrQualsSpec(
            required=True, cardinality=qltypes.SchemaCardinality.One)

    def reduce_REQUIRED_MULTI(self, *kids):
        self.val = PtrQualsSpec(
            required=True, cardinality=qltypes.SchemaCardinality.Many)


class OptPtrQuals(Nonterm):

    def reduce_empty(self, *kids):
        self.val = PtrQualsSpec()

    @parsing.inline(0)
    def reduce_PtrQuals(self, *kids):
        pass


# We have to inline the OptPtrQuals here because the parser generator
# fails to cope with a shift/reduce on a REQUIRED token, since PtrQuals
# are followed by an ident in this case (unlike in DDL, where it is followed
# by a keyword).
class ComputableShapePointer(Nonterm):

    def reduce_OPTIONAL_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.required = False
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[2].span,
        )

    def reduce_REQUIRED_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.required = True
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[2].span,
        )

    def reduce_MULTI_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.cardinality = qltypes.SchemaCardinality.Many
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[2].span,
        )

    def reduce_SINGLE_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.cardinality = qltypes.SchemaCardinality.One
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[2].span,
        )

    def reduce_OPTIONAL_MULTI_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = False
        self.val.cardinality = qltypes.SchemaCardinality.Many
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[3].span,
        )

    def reduce_OPTIONAL_SINGLE_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = False
        self.val.cardinality = qltypes.SchemaCardinality.One
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[3].span,
        )

    def reduce_REQUIRED_MULTI_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = True
        self.val.cardinality = qltypes.SchemaCardinality.Many
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[3].span,
        )

    def reduce_REQUIRED_SINGLE_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = True
        self.val.cardinality = qltypes.SchemaCardinality.One
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[3].span,
        )

    def reduce_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[0].val
        self.val.compexpr = kids[2].val
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[1].span,
        )

    def reduce_SimpleShapePointer_ADDASSIGN_Expr(self, *kids):
        self.val = kids[0].val
        self.val.compexpr = kids[2].val
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.APPEND,
            span=kids[1].span,
        )

    def reduce_SimpleShapePointer_REMASSIGN_Expr(self, *kids):
        self.val = kids[0].val
        self.val.compexpr = kids[2].val
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.SUBTRACT,
            span=kids[1].span,
        )


# This is the same as the above ComputableShapePointer, except using
# FreeSimpleShapePointer and not allowing +=/-=.
class FreeComputableShapePointer(Nonterm):
    def reduce_OPTIONAL_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.required = False
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[2].span,
        )

    def reduce_REQUIRED_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.required = True
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[2].span,
        )

    def reduce_MULTI_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.cardinality = qltypes.SchemaCardinality.Many
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[2].span,
        )

    def reduce_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.cardinality = qltypes.SchemaCardinality.One
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[2].span,
        )

    def reduce_OPTIONAL_MULTI_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = False
        self.val.cardinality = qltypes.SchemaCardinality.Many
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[3].span,
        )

    def reduce_OPTIONAL_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = False
        self.val.cardinality = qltypes.SchemaCardinality.One
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[3].span,
        )

    def reduce_REQUIRED_MULTI_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = True
        self.val.cardinality = qltypes.SchemaCardinality.Many
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[3].span,
        )

    def reduce_REQUIRED_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = True
        self.val.cardinality = qltypes.SchemaCardinality.One
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[3].span,
        )

    def reduce_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[0].val
        self.val.compexpr = kids[2].val
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            span=kids[1].span,
        )


class FreeComputableShapePointerList(ListNonterm,
                                     element=FreeComputableShapePointer,
                                     separator=tokens.T_COMMA,
                                     allow_trailing_separator=True):
    pass


class UnlessConflictSpecifier(Nonterm):
    def reduce_ON_Expr_ELSE_Expr(self, *kids):
        self.val = (kids[1].val, kids[3].val)

    def reduce_ON_Expr(self, *kids):
        self.val = (kids[1].val, None)

    def reduce_empty(self, *kids):
        self.val = (None, None)


class UnlessConflictCause(Nonterm):
    @parsing.inline(2)
    def reduce_UNLESS_CONFLICT_UnlessConflictSpecifier(self, *kids):
        pass


class OptUnlessConflictClause(Nonterm):
    @parsing.inline(0)
    def reduce_UnlessConflictCause(self, *kids):
        pass

    def reduce_empty(self, *kids):
        self.val = None


class FilterClause(Nonterm):
    @parsing.inline(1)
    def reduce_FILTER_Expr(self, *kids):
        pass


class OptFilterClause(Nonterm):
    @parsing.inline(0)
    def reduce_FilterClause(self, *kids):
        pass

    def reduce_empty(self, *kids):
        self.val = None


class SortClause(Nonterm):
    @parsing.inline(1)
    def reduce_ORDERBY_OrderbyList(self, *kids):
        pass


class OptSortClause(Nonterm):
    @parsing.inline(0)
    def reduce_SortClause(self, *kids):
        pass

    def reduce_empty(self, *kids):
        self.val = []


class OrderbyExpr(Nonterm):
    def reduce_Expr_OptDirection_OptNonesOrder(self, *kids):
        self.val = qlast.SortExpr(path=kids[0].val,
                                  direction=kids[1].val,
                                  nones_order=kids[2].val)


class OrderbyList(ListNonterm, element=OrderbyExpr,
                  separator=tokens.T_THEN):
    pass


class OptSelectLimit(Nonterm):
    @parsing.inline(0)
    def reduce_SelectLimit(self, *kids):
        pass

    def reduce_empty(self, *kids):
        self.val = (None, None)


class SelectLimit(Nonterm):
    def reduce_OffsetClause_LimitClause(self, *kids):
        self.val = (kids[0].val, kids[1].val)

    def reduce_OffsetClause(self, *kids):
        self.val = (kids[0].val, None)

    def reduce_LimitClause(self, *kids):
        self.val = (None, kids[0].val)


class OffsetClause(Nonterm):
    @parsing.inline(1)
    def reduce_OFFSET_Expr(self, *kids):
        pass


class LimitClause(Nonterm):
    @parsing.inline(1)
    def reduce_LIMIT_Expr(self, *kids):
        pass


class OptDirection(Nonterm):
    def reduce_ASC(self, *kids):
        self.val = qlast.SortAsc

    def reduce_DESC(self, *kids):
        self.val = qlast.SortDesc

    def reduce_empty(self, *kids):
        self.val = qlast.SortDefault


class OptNonesOrder(Nonterm):
    def reduce_EMPTY_FIRST(self, *kids):
        self.val = qlast.NonesFirst

    def reduce_EMPTY_LAST(self, *kids):
        self.val = qlast.NonesLast

    def reduce_empty(self, *kids):
        self.val = None


class IndirectionEl(Nonterm):
    def reduce_LBRACKET_Expr_RBRACKET(self, *kids):
        self.val = qlast.Index(index=kids[1].val)

    def reduce_LBRACKET_Expr_COLON_Expr_RBRACKET(self, *kids):
        self.val = qlast.Slice(start=kids[1].val, stop=kids[3].val)

    def reduce_LBRACKET_Expr_COLON_RBRACKET(self, *kids):
        self.val = qlast.Slice(start=kids[1].val, stop=None)

    def reduce_LBRACKET_COLON_Expr_RBRACKET(self, *kids):
        self.val = qlast.Slice(start=None, stop=kids[2].val)


class ParenExpr(Nonterm):
    @parsing.inline(1)
    def reduce_LPAREN_Expr_RPAREN(self, *kids):
        pass

    @parsing.inline(1)
    def reduce_LPAREN_ExprStmt_RPAREN(self, *kids):
        pass


class BaseAtomicExpr(Nonterm):
    # { ... } | Constant | '(' Expr ')' | FuncExpr
    # | Tuple | NamedTuple | Collection | Set
    # | '__source__' | '__subject__'
    # | '__new__' | '__old__' | '__specified__' | '__default__'
    # | NodeName | PathStep

    @parsing.inline(0)
    def reduce_FreeShape(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_Constant(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_StringInterpolation(self, *kids):
        pass

    def reduce_DUNDERSOURCE(self, *kids):
        self.val = qlast.Path(steps=[qlast.SpecialAnchor(name='__source__')])

    def reduce_DUNDERSUBJECT(self, *kids):
        self.val = qlast.Path(steps=[qlast.SpecialAnchor(name='__subject__')])

    def reduce_DUNDERNEW(self, *kids):
        self.val = qlast.Path(steps=[qlast.SpecialAnchor(name='__new__')])

    def reduce_DUNDEROLD(self, *kids):
        self.val = qlast.Path(steps=[qlast.SpecialAnchor(name='__old__')])

    def reduce_DUNDERSPECIFIED(self, _):
        self.val = qlast.Path(
            steps=[qlast.SpecialAnchor(name='__specified__')]
        )

    def reduce_DUNDERDEFAULT(self, *kids):
        self.val = qlast.Path(steps=[qlast.SpecialAnchor(name='__default__')])

    @parsing.precedence(precedence.P_UMINUS)
    @parsing.inline(0)
    def reduce_ParenExpr(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_FuncExpr(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_Tuple(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_Collection(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_Set(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_NamedTuple(self, *kids):
        pass

    @parsing.precedence(precedence.P_DOT)
    def reduce_NodeName(self, *kids):
        self.val = qlast.Path(
            steps=[qlast.ObjectRef(name=kids[0].val.name,
                                   module=kids[0].val.module)])

    @parsing.precedence(precedence.P_DOT)
    def reduce_PathStep(self, *kids):
        self.val = qlast.Path(steps=[kids[0].val], partial=True)


class Expr(Nonterm):
    # BaseAtomicExpr
    # Path | Expr { ... }

    # | Expr '[' Expr ']'
    # | Expr '[' Expr ':' Expr ']'
    # | Expr '[' ':' Expr ']'
    # | Expr '[' Expr ':' ']'
    # | Expr '[' IS NodeName ']'

    # | '+' Expr | '-' Expr | Expr '+' Expr | Expr '-' Expr
    # | Expr '*' Expr | Expr '/' Expr | Expr '%' Expr
    # | Expr '**' Expr | Expr '<' Expr | Expr '>' Expr
    # | Expr '=' Expr
    # | Expr AND Expr | Expr OR Expr | NOT Expr
    # | Expr LIKE Expr | Expr NOT LIKE Expr
    # | Expr ILIKE Expr | Expr NOT ILIKE Expr
    # | Expr IS TypeExpr | Expr IS NOT TypeExpr
    # | INTROSPECT TypeExpr
    # | Expr IN Expr | Expr NOT IN Expr
    # | '<' TypeName '>' Expr
    # | Expr IF Expr ELSE Expr
    # | Expr ?? Expr
    # | Expr UNION Expr | Expr UNION Expr
    # | DISTINCT Expr
    # | DETACHED Expr
    # | GLOBAL Name
    # | EXISTS Expr

    @parsing.inline(0)
    def reduce_BaseAtomicExpr(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_Path(self, *kids):
        pass

    def reduce_Expr_Shape(self, *kids):
        self.val = qlast.Shape(expr=kids[0].val, elements=kids[1].val)

    def reduce_EXISTS_Expr(self, *kids):
        self.val = qlast.UnaryOp(op='EXISTS', operand=kids[1].val)

    def reduce_DISTINCT_Expr(self, *kids):
        self.val = qlast.UnaryOp(op='DISTINCT', operand=kids[1].val)

    def reduce_DETACHED_Expr(self, *kids):
        self.val = qlast.DetachedExpr(expr=kids[1].val)

    def reduce_GLOBAL_NodeName(self, *kids):
        self.val = qlast.GlobalExpr(name=kids[1].val)

    def reduce_Expr_IndirectionEl(self, *kids):
        expr = kids[0].val
        if isinstance(expr, qlast.Indirection):
            self.val = expr
            expr.indirection.append(kids[1].val)
        else:
            self.val = qlast.Indirection(arg=expr,
                                         indirection=[kids[1].val])

    @parsing.precedence(precedence.P_UMINUS)
    def reduce_PLUS_Expr(self, *kids):
        self.val = qlast.UnaryOp(op=kids[0].val, operand=kids[1].val)

    @parsing.precedence(precedence.P_UMINUS)
    def reduce_MINUS_Expr(self, *kids):
        arg = kids[1].val
        if isinstance(arg, qlast.Constant) and arg.kind in {
            qlast.ConstantKind.INTEGER,
            qlast.ConstantKind.FLOAT,
            qlast.ConstantKind.BIGINT,
            qlast.ConstantKind.DECIMAL,
        }:
            self.val = type(arg)(value=f'-{arg.value}', kind=arg.kind)
        else:
            self.val = qlast.UnaryOp(op=kids[0].val, operand=arg)

    def reduce_Expr_PLUS_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    def reduce_Expr_DOUBLEPLUS_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    def reduce_Expr_MINUS_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    def reduce_Expr_STAR_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    def reduce_Expr_SLASH_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    def reduce_Expr_DOUBLESLASH_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    def reduce_Expr_PERCENT_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    def reduce_Expr_CIRCUMFLEX_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    @parsing.precedence(precedence.P_DOUBLEQMARK_OP)
    def reduce_Expr_DOUBLEQMARK_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    @parsing.precedence(precedence.P_COMPARE_OP)
    def reduce_Expr_CompareOp_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    def reduce_Expr_AND_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val.upper(),
                               right=kids[2].val)

    def reduce_Expr_OR_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val.upper(),
                               right=kids[2].val)

    def reduce_NOT_Expr(self, *kids):
        self.val = qlast.UnaryOp(op=kids[0].val.upper(), operand=kids[1].val)

    def reduce_Expr_LIKE_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op='LIKE',
                               right=kids[2].val)

    def reduce_Expr_NOT_LIKE_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op='NOT LIKE',
                               right=kids[3].val)

    def reduce_Expr_ILIKE_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op='ILIKE',
                               right=kids[2].val)

    def reduce_Expr_NOT_ILIKE_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op='NOT ILIKE',
                               right=kids[3].val)

    def reduce_Expr_IS_TypeExpr(self, *kids):
        self.val = qlast.IsOp(left=kids[0].val, op='IS',
                              right=kids[2].val)

    @parsing.precedence(precedence.P_IS)
    def reduce_Expr_IS_NOT_TypeExpr(self, *kids):
        self.val = qlast.IsOp(left=kids[0].val, op='IS NOT',
                              right=kids[3].val)

    def reduce_INTROSPECT_TypeExpr(self, *kids):
        self.val = qlast.Introspect(type=kids[1].val)

    def reduce_Expr_IN_Expr(self, *kids):
        inexpr = kids[2].val
        self.val = qlast.BinOp(left=kids[0].val, op='IN',
                               right=inexpr)

    @parsing.precedence(precedence.P_IN)
    def reduce_Expr_NOT_IN_Expr(self, *kids):
        inexpr = kids[3].val
        self.val = qlast.BinOp(left=kids[0].val, op='NOT IN',
                               right=inexpr)

    @parsing.precedence(precedence.P_TYPECAST)
    def reduce_LANGBRACKET_FullTypeExpr_RANGBRACKET_Expr(
            self, *kids):
        self.val = qlast.TypeCast(
            expr=kids[3].val,
            type=kids[1].val,
            cardinality_mod=None,
        )

    @parsing.precedence(precedence.P_TYPECAST)
    def reduce_LANGBRACKET_OPTIONAL_FullTypeExpr_RANGBRACKET_Expr(
            self, *kids):
        self.val = qlast.TypeCast(
            expr=kids[4].val,
            type=kids[2].val,
            cardinality_mod=qlast.CardinalityModifier.Optional,
        )

    @parsing.precedence(precedence.P_TYPECAST)
    def reduce_LANGBRACKET_REQUIRED_FullTypeExpr_RANGBRACKET_Expr(
            self, *kids):
        self.val = qlast.TypeCast(
            expr=kids[4].val,
            type=kids[2].val,
            cardinality_mod=qlast.CardinalityModifier.Required,
        )

    def reduce_Expr_IF_Expr_ELSE_Expr(self, *kids):
        if_expr, _, condition, _, else_expr = kids
        self.val = qlast.IfElse(
            if_expr=if_expr.val,
            condition=condition.val,
            else_expr=else_expr.val,
            python_style=True,
        )

    @parsing.inline(0)
    def reduce_IfThenElseExpr(self, _):
        pass

    def reduce_Expr_UNION_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op='UNION',
                               right=kids[2].val)

    def reduce_Expr_EXCEPT_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op='EXCEPT',
                               right=kids[2].val)

    def reduce_Expr_INTERSECT_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op='INTERSECT',
                               right=kids[2].val)


class IfThenElseExpr(Nonterm):
    def reduce_IF_Expr_THEN_Expr_ELSE_Expr(self, *kids):
        _, condition, _, if_expr, _, else_expr = kids
        self.val = qlast.IfElse(
            condition=condition.val,
            if_expr=if_expr.val,
            else_expr=else_expr.val,
        )


class CompareOp(Nonterm):
    @parsing.inline(0)
    @parsing.precedence(precedence.P_COMPARE_OP)
    def reduce_DISTINCTFROM(self, *_):
        pass

    @parsing.inline(0)
    @parsing.precedence(precedence.P_COMPARE_OP)
    def reduce_GREATEREQ(self, *_):
        pass

    @parsing.inline(0)
    @parsing.precedence(precedence.P_COMPARE_OP)
    def reduce_LESSEQ(self, *_):
        pass

    @parsing.inline(0)
    @parsing.precedence(precedence.P_COMPARE_OP)
    def reduce_NOTDISTINCTFROM(self, *_):
        pass

    @parsing.inline(0)
    @parsing.precedence(precedence.P_COMPARE_OP)
    def reduce_NOTEQ(self, *_):
        pass

    @parsing.inline(0)
    @parsing.precedence(precedence.P_COMPARE_OP)
    def reduce_LANGBRACKET(self, *_):
        pass

    @parsing.inline(0)
    @parsing.precedence(precedence.P_COMPARE_OP)
    def reduce_RANGBRACKET(self, *_):
        pass

    @parsing.inline(0)
    @parsing.precedence(precedence.P_COMPARE_OP)
    def reduce_EQUALS(self, *_):
        pass


class Tuple(Nonterm):
    def reduce_LPAREN_Expr_COMMA_OptExprList_RPAREN(self, *kids):
        self.val = qlast.Tuple(elements=[kids[1].val] + kids[3].val)

    def reduce_LPAREN_RPAREN(self, *kids):
        self.val = qlast.Tuple(elements=[])


class NamedTuple(Nonterm):
    def reduce_LPAREN_NamedTupleElementList_RPAREN(self, *kids):
        self.val = qlast.NamedTuple(elements=kids[1].val)


class NamedTupleElement(Nonterm):
    def reduce_ShortNodeName_ASSIGN_Expr(self, *kids):
        self.val = qlast.TupleElement(
            name=qlast.Ptr(name=kids[0].val.name, span=kids[0].val.span),
            val=kids[2].val
        )


class NamedTupleElementList(ListNonterm, element=NamedTupleElement,
                            separator=tokens.T_COMMA,
                            allow_trailing_separator=True):
    pass


class Set(Nonterm):
    def reduce_LBRACE_OptExprList_RBRACE(self, *kids):
        self.val = qlast.Set(elements=kids[1].val)


class Collection(Nonterm):
    def reduce_LBRACKET_OptExprList_RBRACKET(self, *kids):
        elements = kids[1].val
        self.val = qlast.Array(elements=elements)


class OptExprList(Nonterm):
    @parsing.inline(0)
    def reduce_ExprList(self, *kids):
        pass

    def reduce_empty(self, *kids):
        self.val = []


class ExprList(ListNonterm, element=Expr, separator=tokens.T_COMMA,
               allow_trailing_separator=True):
    pass


class Constant(Nonterm):
    # PARAMETER
    # | BaseNumberConstant
    # | BaseStringConstant
    # | BaseBooleanConstant
    # | BaseBytesConstant

    def reduce_PARAMETER(self, param):
        self.val = qlast.Parameter(name=param.val[1:])

    def reduce_PARAMETERANDTYPE(self, param):
        assert param.val.startswith('<lit ')
        type_name, param_name = param.val.removeprefix('<lit ').split('>$')
        self.val = qlast.TypeCast(
            type=qlast.TypeName(
                maintype=qlast.ObjectRef(
                    name=type_name,
                    module='__std__'
                )
            ),
            expr=qlast.Parameter(name=param_name),
        )

    @parsing.inline(0)
    def reduce_BaseNumberConstant(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_BaseStringConstant(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_BaseBooleanConstant(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_BaseBytesConstant(self, *kids):
        pass


class StringInterpolationTail(Nonterm):
    def reduce_Expr_STRINTERPEND(self, *kids):
        expr, lit = kids
        self.val = qlast.StrInterp(
            prefix='',
            interpolations=[
                qlast.StrInterpFragment(expr=expr.val, suffix=lit.clean_value),
            ]
        )

    def reduce_Expr_STRINTERPCONT_StringInterpolationTail(self, *kids):
        expr, lit, tail = kids
        self.val = tail.val
        self.val.interpolations.append(
            qlast.StrInterpFragment(expr=expr.val, suffix=lit.clean_value)
        )


class StringInterpolation(Nonterm):
    def reduce_STRINTERPSTART_StringInterpolationTail(self, *kids):
        # We produce somewhat malformed StrInterp values out of
        # StringInterpolationTail, for convenience and efficiency, and
        # fix them up here.
        # (In particular, we put the interpolations in backward.)
        lit, tail = kids
        self.val = tail.val
        self.val.prefix = lit.clean_value
        self.val.interpolations.reverse()


class BaseNumberConstant(Nonterm):
    def reduce_ICONST(self, *kids):
        self.val = qlast.Constant(
            value=kids[0].val, kind=qlast.ConstantKind.INTEGER
        )

    def reduce_FCONST(self, *kids):
        self.val = qlast.Constant(
            value=kids[0].val, kind=qlast.ConstantKind.FLOAT
        )

    def reduce_NICONST(self, *kids):
        self.val = qlast.Constant(
            value=kids[0].val, kind=qlast.ConstantKind.BIGINT
        )

    def reduce_NFCONST(self, *kids):
        self.val = qlast.Constant(
            value=kids[0].val, kind=qlast.ConstantKind.DECIMAL
        )


class BaseStringConstant(Nonterm):

    def reduce_SCONST(self, token):
        self.val = qlast.Constant.string(value=token.clean_value)


class BaseBytesConstant(Nonterm):

    def reduce_BCONST(self, bytes_tok):
        self.val = qlast.BytesConstant(value=bytes_tok.clean_value)


class BaseBooleanConstant(Nonterm):
    def reduce_TRUE(self, *kids):
        self.val = qlast.Constant.boolean(True)

    def reduce_FALSE(self, *kids):
        self.val = qlast.Constant.boolean(False)


def ensure_path(expr):
    if not isinstance(expr, qlast.Path):
        expr = qlast.Path(steps=[expr])
    return expr


class Path(Nonterm):
    @parsing.precedence(precedence.P_DOT)
    def reduce_Expr_PathStep(self, *kids):
        path = ensure_path(kids[0].val)
        path.steps.append(kids[1].val)
        self.val = path


class AtomicExpr(Nonterm):
    @parsing.inline(0)
    def reduce_BaseAtomicExpr(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_AtomicPath(self, *kids):
        pass

    @parsing.precedence(precedence.P_TYPECAST)
    def reduce_LANGBRACKET_FullTypeExpr_RANGBRACKET_AtomicExpr(
            self, *kids):
        self.val = qlast.TypeCast(
            expr=kids[3].val,
            type=kids[1].val,
            cardinality_mod=None,
        )


# Duplication of Path above, but with BasicExpr at the root
class AtomicPath(Nonterm):
    @parsing.precedence(precedence.P_DOT)
    def reduce_AtomicExpr_PathStep(self, *kids):
        path = ensure_path(kids[0].val)
        path.steps.append(kids[1].val)
        self.val = path


class PathStep(Nonterm):
    def reduce_DOT_PathStepName(self, *kids):
        from edb.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            name=kids[1].val.name,
            direction=s_pointers.PointerDirection.Outbound
        )

    def reduce_DOT_ICONST(self, *kids):
        # this is a valid link-like syntax for accessing unnamed tuples
        from edb.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            name=kids[1].val,
            direction=s_pointers.PointerDirection.Outbound
        )

    def reduce_DOTBW_PathStepName(self, *kids):
        from edb.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            name=kids[1].val.name,
            direction=s_pointers.PointerDirection.Inbound
        )

    def reduce_AT_PathNodeName(self, *kids):
        from edb.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            name=kids[1].val.name,
            direction=s_pointers.PointerDirection.Outbound,
            type='property'
        )

    @parsing.inline(0)
    def reduce_TypeIntersection(self, *kids):
        pass


class TypeIntersection(Nonterm):
    def reduce_LBRACKET_IS_FullTypeExpr_RBRACKET(self, *kids):
        self.val = qlast.TypeIntersection(
            type=kids[2].val,
        )


class OptTypeIntersection(Nonterm):
    @parsing.inline(0)
    def reduce_TypeIntersection(self, *kids):
        pass

    def reduce_empty(self):
        self.val = None


# Used in free shapes
class FreeStepName(Nonterm):
    @parsing.inline(0)
    def reduce_ShortNodeName(self, *kids):
        pass

    def reduce_DUNDERTYPE(self, *kids):
        self.val = qlast.ObjectRef(name=kids[0].val)


# Used in shapes, paths and in PROPERTY/LINK definitions.
class PathStepName(Nonterm):
    @parsing.inline(0)
    def reduce_PathNodeName(self, *kids):
        pass

    def reduce_DUNDERTYPE(self, *kids):
        self.val = qlast.ObjectRef(name=kids[0].val)


class FuncApplication(Nonterm):
    def reduce_NodeName_LPAREN_OptFuncArgList_RPAREN(self, *kids):
        module = kids[0].val.module
        func_name = kids[0].val.name
        name = func_name if not module else (module, func_name)

        last_named_seen = None
        args = []
        kwargs = {}
        for argname, argname_ctx, arg in kids[2].val:
            if argname is not None:
                if argname in kwargs:
                    raise errors.EdgeQLSyntaxError(
                        f"duplicate named argument `{argname}`",
                        span=argname_ctx)

                last_named_seen = argname
                kwargs[argname] = arg

            else:
                if last_named_seen is not None:
                    raise errors.EdgeQLSyntaxError(
                        f"positional argument after named "
                        f"argument `{last_named_seen}`",
                        span=arg.span)
                args.append(arg)

        self.val = qlast.FunctionCall(func=name, args=args, kwargs=kwargs)


class FuncExpr(Nonterm):
    @parsing.inline(0)
    def reduce_FuncApplication(self, *kids):
        pass


class FuncCallArgExpr(Nonterm):
    def reduce_Expr(self, *kids):
        self.val = (
            None,
            None,
            kids[0].val,
        )

    def reduce_AnyIdentifier_ASSIGN_Expr(self, *kids):
        self.val = (
            kids[0].val,
            kids[0].span,
            kids[2].val,
        )

    def reduce_PARAMETER_ASSIGN_Expr(self, *kids):
        if kids[0].val[1].isdigit():
            raise errors.EdgeQLSyntaxError(
                f"numeric named parameters are not supported",
                span=kids[0].span)
        else:
            raise errors.EdgeQLSyntaxError(
                f"named parameters do not need a '$' prefix, "
                f"rewrite as '{kids[0].val[1:]} := ...'",
                span=kids[0].span)


class FuncCallArg(Nonterm):
    def reduce_FuncCallArgExpr_OptFilterClause_OptSortClause(self, *kids):
        self.val = kids[0].val

        if kids[1].val or kids[2].val:
            qry = qlast.SelectQuery(
                result=self.val[2],
                where=kids[1].val,
                orderby=kids[2].val,
                implicit=True,
            )
            self.val = (self.val[0], self.val[1], qry)


class FuncArgList(ListNonterm, element=FuncCallArg, separator=tokens.T_COMMA,
                  allow_trailing_separator=True):
    pass


class OptFuncArgList(Nonterm):
    @parsing.inline(0)
    def reduce_FuncArgList(self, *kids):
        pass

    def reduce_empty(self, *kids):
        self.val = []


class PosCallArg(Nonterm):
    def reduce_Expr_OptFilterClause_OptSortClause(self, *kids):
        self.val = kids[0].val
        if kids[1].val or kids[2].val:
            self.val = qlast.SelectQuery(
                result=self.val,
                where=kids[1].val,
                orderby=kids[2].val,
                implicit=True,
            )


class PosCallArgList(ListNonterm, element=PosCallArg,
                     separator=tokens.T_COMMA):
    pass


class OptPosCallArgList(Nonterm):
    @parsing.inline(0)
    def reduce_PosCallArgList(self, *kids):
        pass

    def reduce_empty(self, *kids):
        self.val = []


class Identifier(Nonterm):
    val: str  # == Token.value

    def reduce_IDENT(self, ident):
        self.val = ident.clean_value

    @parsing.inline(0)
    def reduce_UnreservedKeyword(self, *_):
        pass


class PtrIdentifier(Nonterm):
    @parsing.inline(0)
    def reduce_Identifier(self, *_):
        pass

    @parsing.inline(0)
    def reduce_PartialReservedKeyword(self, *_):
        pass


class AnyIdentifier(Nonterm):
    @parsing.inline(0)
    def reduce_PtrIdentifier(self, *kids):
        pass

    def reduce_ReservedKeyword(self, *kids):
        name = kids[0].val
        if name[:2] == '__' and name[-2:] == '__':
            # There are a few reserved keywords like __std__ and __subject__
            # that can be used in paths but are prohibited to be used
            # anywhere else. So just as the tokenizer prohibits using
            # __names__ in general, we enforce the rule here for the
            # few remaining reserved __keywords__.
            raise errors.EdgeQLSyntaxError(
                "identifiers surrounded by double underscores are forbidden",
                span=kids[0].span)

        self.val = name


class DottedIdents(
        ListNonterm, element=AnyIdentifier, separator=tokens.T_DOT):
    pass


class DotName(Nonterm):
    def reduce_DottedIdents(self, *kids):
        self.val = '.'.join(part for part in kids[0].val)


class ModuleName(
        ListNonterm, element=DotName, separator=tokens.T_DOUBLECOLON):
    pass


class ColonedIdents(
        ListNonterm, element=AnyIdentifier, separator=tokens.T_DOUBLECOLON):
    pass


class QualifiedName(Nonterm):
    def reduce_Identifier_DOUBLECOLON_ColonedIdents(self, ident, _, idents):
        assert ident.val
        assert idents.val
        self.val = [ident.val, *idents.val]

    def reduce_DUNDERSTD_DOUBLECOLON_ColonedIdents(self, _s, _c, idents):
        assert idents.val
        self.val = ['__std__', *idents.val]


# this can appear anywhere
class BaseName(Nonterm):
    def reduce_Identifier(self, *kids):
        self.val = [kids[0].val]

    @parsing.inline(0)
    def reduce_QualifiedName(self, *kids):
        pass


# this can appear in link/property definitions
class PtrName(Nonterm):
    def reduce_PtrIdentifier(self, ptr_identifier):
        assert ptr_identifier.val
        self.val = [ptr_identifier.val]

    @parsing.inline(0)
    def reduce_QualifiedName(self, *_):
        pass


# Non-collection type.
class SimpleTypeName(Nonterm):
    def reduce_PtrNodeName(self, *kids):
        self.val = qlast.TypeName(maintype=kids[0].val)

    def reduce_ANYTYPE(self, *kids):
        self.val = qlast.TypeName(
            maintype=qlast.PseudoObjectRef(name='anytype')
        )

    def reduce_ANYTUPLE(self, *kids):
        self.val = qlast.TypeName(
            maintype=qlast.PseudoObjectRef(name='anytuple')
        )

    def reduce_ANYOBJECT(self, *kids):
        self.val = qlast.TypeName(
            maintype=qlast.PseudoObjectRef(name='anyobject')
        )


class SimpleTypeNameList(ListNonterm, element=SimpleTypeName,
                         separator=tokens.T_COMMA):
    pass


class CollectionTypeName(Nonterm):

    def validate_subtype_list(self, lst):
        has_nonstrval = has_strval = has_items = False
        for el in lst.val:
            if isinstance(el, qlast.TypeExprLiteral):
                has_strval = True
            elif isinstance(el, qlast.TypeName):
                if el.name:
                    has_items = True
                else:
                    has_nonstrval = True

        if (has_nonstrval or has_items) and has_strval:
            # Prohibit cases like `tuple<a: int64, 'aaaa'>` and
            # `enum<bbbb, 'aaaa'>`
            raise errors.EdgeQLSyntaxError(
                "mixing string type literals and type names is not supported",
                span=lst.span)

        if has_items and has_nonstrval:
            # Prohibit cases like `tuple<a: int64, int32>`
            raise errors.EdgeQLSyntaxError(
                "mixing named and unnamed subtype declarations "
                "is not supported",
                span=lst.span)

    def reduce_NodeName_LANGBRACKET_RANGBRACKET(self, *kids):
        # Constructs like `enum<>` or `array<>` aren't legal.
        raise errors.EdgeQLSyntaxError(
            'parametrized type must have at least one argument',
            span=kids[1].span,
        )

    def reduce_NodeName_LANGBRACKET_SubtypeList_RANGBRACKET(self, *kids):
        self.validate_subtype_list(kids[2])
        self.val = qlast.TypeName(
            maintype=kids[0].val,
            subtypes=kids[2].val,
        )


class TypeName(Nonterm):
    @parsing.inline(0)
    def reduce_SimpleTypeName(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_CollectionTypeName(self, *kids):
        pass


class TypeNameList(ListNonterm, element=TypeName,
                   separator=tokens.T_COMMA):
    pass


# A type expression that is not a simple type.
class NontrivialTypeExpr(Nonterm):
    def reduce_TYPEOF_Expr(self, *kids):
        self.val = qlast.TypeOf(expr=kids[1].val)

    @parsing.inline(1)
    def reduce_LPAREN_FullTypeExpr_RPAREN(self, *kids):
        pass

    def reduce_TypeExpr_PIPE_TypeExpr(self, *kids):
        self.val = qlast.TypeOp(left=kids[0].val, op='|',
                                right=kids[2].val)

    def reduce_TypeExpr_AMPER_TypeExpr(self, *kids):
        self.val = qlast.TypeOp(left=kids[0].val, op='&',
                                right=kids[2].val)


# This is a type expression without angle brackets, so it
# can be used without parentheses in a context where the
# angle bracket has a different meaning.
class TypeExpr(Nonterm):
    @parsing.inline(0)
    def reduce_SimpleTypeName(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_NontrivialTypeExpr(self, *kids):
        pass


# A type expression enclosed in parentheses
class ParenTypeExpr(Nonterm):
    @parsing.inline(1)
    def reduce_LPAREN_FullTypeExpr_RPAREN(self, *kids):
        pass


# This is a type expression which includes collection types,
# so it can only be directly used in a context where the
# angle bracket is unambiguous.
class FullTypeExpr(Nonterm):
    @parsing.inline(0)
    def reduce_TypeName(self, *kids):
        pass

    def reduce_TYPEOF_Expr(self, *kids):
        self.val = qlast.TypeOf(expr=kids[1].val)

    @parsing.inline(1)
    def reduce_LPAREN_FullTypeExpr_RPAREN(self, *kids):
        pass

    def reduce_FullTypeExpr_PIPE_FullTypeExpr(self, *kids):
        self.val = qlast.TypeOp(left=kids[0].val, op='|',
                                right=kids[2].val)

    def reduce_FullTypeExpr_AMPER_FullTypeExpr(self, *kids):
        self.val = qlast.TypeOp(left=kids[0].val, op='&',
                                right=kids[2].val)


class Subtype(Nonterm):
    @parsing.inline(0)
    def reduce_FullTypeExpr(self, *kids):
        pass

    def reduce_Identifier_COLON_FullTypeExpr(self, *kids):
        self.val = kids[2].val
        self.val.name = kids[0].val

    def reduce_BaseStringConstant(self, *kids):
        # TODO: Raise a DeprecationWarning once we have facility for that.
        self.val = qlast.TypeExprLiteral(
            val=kids[0].val,
        )

    def reduce_BaseNumberConstant(self, *kids):
        self.val = qlast.TypeExprLiteral(
            val=kids[0].val,
        )


class SubtypeList(ListNonterm, element=Subtype, separator=tokens.T_COMMA,
                  allow_trailing_separator=True):
    pass


class NodeName(Nonterm):
    # NOTE: Generic short of fully-qualified name.
    #
    # This name is safe to be used anywhere as it starts with IDENT only.

    def reduce_BaseName(self, base_name):
        self.val = qlast.ObjectRef(
            module='::'.join(base_name.val[:-1]) or None,
            name=base_name.val[-1])


class PtrNodeName(Nonterm):
    # NOTE: Generic short of fully-qualified name.
    #
    # This name is safe to be used in most DDL and SDL definitions.

    def reduce_PtrName(self, ptr_name):
        self.val = qlast.ObjectRef(
            module='::'.join(ptr_name.val[:-1]) or None,
            name=ptr_name.val[-1])


class PtrQualifiedNodeName(Nonterm):
    def reduce_QualifiedName(self, *kids):
        self.val = qlast.ObjectRef(
            module='::'.join(kids[0].val[:-1]),
            name=kids[0].val[-1])


class ShortNodeName(Nonterm):
    # NOTE: A non-qualified name that can be an identifier or
    # UNRESERVED_KEYWORD.
    #
    # This name is used as part of paths after the DOT. It can be an
    # identifier including UNRESERVED_KEYWORD and does not need to be
    # quoted or parenthesized.

    def reduce_Identifier(self, *kids):
        self.val = qlast.ObjectRef(
            module=None,
            name=kids[0].val)


# ShortNodeNameList is needed in DDL, but it's worthwhile to define it
# here, near ShortNodeName.
class ShortNodeNameList(ListNonterm, element=ShortNodeName,
                        separator=tokens.T_COMMA):
    pass


class PathNodeName(Nonterm):
    # NOTE: A non-qualified name that can be an identifier or
    # PARTIAL_RESERVED_KEYWORD.
    #
    # This name is used as part of paths after the DOT as well as in
    # definitions after LINK/POINTER. It can be an identifier including
    # PARTIAL_RESERVED_KEYWORD and does not need to be quoted or
    # parenthesized.

    def reduce_PtrIdentifier(self, *kids):
        self.val = qlast.ObjectRef(
            module=None,
            name=kids[0].val)


class AnyNodeName(Nonterm):
    # NOTE: A non-qualified name that can be ANY identifier.
    #
    # This name is used as part of paths after the DOT. It can be any
    # identifier including RESERVED_KEYWORD and UNRESERVED_KEYWORD and
    # does not need to be quoted or parenthesized.
    #
    # This is mainly used in DDL statements that have another keyword
    # completely disambiguating that what comes next is a name. It
    # CANNOT be used in Expr productions because it will cause
    # ambiguity with NodeName, etc.

    def reduce_AnyIdentifier(self, *kids):
        self.val = qlast.ObjectRef(
            module=None,
            name=kids[0].val)


class Keyword(parsing.Nonterm):
    """Base class for the different classes of keywords.

    Not a real nonterm on its own.
    """
    def __init_subclass__(
            cls, *, type, is_internal=False, **kwargs):
        super().__init_subclass__(is_internal=is_internal, **kwargs)

        if is_internal:
            return

        assert type in keywords.keyword_types

        for token in keywords.by_type[type].values():
            def method(inst, *kids):
                inst.val = kids[0].val
            method = span.wrap_function_to_infer_spans(method)
            method.__doc__ = "%%reduce %s" % token
            method.__name__ = 'reduce_%s' % token
            setattr(cls, method.__name__, method)


class UnreservedKeyword(Keyword,
                        type=keywords.UNRESERVED_KEYWORD):
    pass


class PartialReservedKeyword(Keyword,
                             type=keywords.PARTIAL_RESERVED_KEYWORD):
    pass


class ReservedKeyword(Keyword,
                      type=keywords.RESERVED_KEYWORD):
    pass


class SchemaObjectClassValue(typing.NamedTuple):

    itemclass: qltypes.SchemaObjectClass


class SchemaObjectClass(Nonterm):

    def reduce_ALIAS(self, *kids):
        self.val = SchemaObjectClassValue(
            itemclass=qltypes.SchemaObjectClass.ALIAS)

    def reduce_ANNOTATION(self, *kids):
        self.val = SchemaObjectClassValue(
            itemclass=qltypes.SchemaObjectClass.ANNOTATION)

    def reduce_CAST(self, *kids):
        self.val = SchemaObjectClassValue(
            itemclass=qltypes.SchemaObjectClass.CAST)

    def reduce_CONSTRAINT(self, *kids):
        self.val = SchemaObjectClassValue(
            itemclass=qltypes.SchemaObjectClass.CONSTRAINT)

    def reduce_FUNCTION(self, *kids):
        self.val = SchemaObjectClassValue(
            itemclass=qltypes.SchemaObjectClass.FUNCTION)

    def reduce_LINK(self, *kids):
        self.val = SchemaObjectClassValue(
            itemclass=qltypes.SchemaObjectClass.LINK)

    def reduce_MODULE(self, *kids):
        self.val = SchemaObjectClassValue(
            itemclass=qltypes.SchemaObjectClass.MODULE)

    def reduce_OPERATOR(self, *kids):
        self.val = SchemaObjectClassValue(
            itemclass=qltypes.SchemaObjectClass.OPERATOR)

    def reduce_PROPERTY(self, *kids):
        self.val = SchemaObjectClassValue(
            itemclass=qltypes.SchemaObjectClass.PROPERTY)

    def reduce_SCALAR_TYPE(self, *kids):
        self.val = SchemaObjectClassValue(
            itemclass=qltypes.SchemaObjectClass.SCALAR_TYPE)

    def reduce_TYPE(self, *kids):
        self.val = SchemaObjectClassValue(
            itemclass=qltypes.SchemaObjectClass.TYPE)


class SchemaItem(Nonterm):

    def reduce_SchemaObjectClass_NodeName(self, *kids):
        ref = kids[1].val
        ref.itemclass = kids[0].val.itemclass
        self.val = ref
