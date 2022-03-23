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

from edb.common import parsing, context

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb import errors

from . import keywords
from . import precedence
from . import tokens

from .precedence import *  # NOQA
from .tokens import *  # NOQA


class Nonterm(parsing.Nonterm):
    pass


class ListNonterm(parsing.ListNonterm, element=None):
    pass


class ExprStmt(Nonterm):
    def reduce_WithBlock_ExprStmtCore(self, *kids):
        self.val = kids[1].val
        self.val.aliases = kids[0].val.aliases

    def reduce_ExprStmtCore(self, *kids):
        self.val = kids[0].val


class ExprStmtCore(Nonterm):
    def reduce_SimpleFor(self, *kids):
        self.val = kids[0].val

    def reduce_SimpleSelect(self, *kids):
        self.val = kids[0].val

    def reduce_SimpleGroup(self, *kids):
        self.val = kids[0].val

    def reduce_InternalGroup(self, *kids):
        self.val = kids[0].val

    def reduce_SimpleInsert(self, *kids):
        self.val = kids[0].val

    def reduce_SimpleUpdate(self, *kids):
        self.val = kids[0].val

    def reduce_SimpleDelete(self, *kids):
        self.val = kids[0].val


class AliasedExpr(Nonterm):
    def reduce_Identifier_ASSIGN_Expr(self, *kids):
        self.val = qlast.AliasedExpr(alias=kids[0].val, expr=kids[2].val)


class OptionallyAliasedExpr(Nonterm):
    def reduce_AliasedExpr(self, *kids):
        val = kids[0].val
        self.val = AliasedExprSpec(alias=val.alias, expr=val.expr)

    def reduce_Expr(self, *kids):
        self.val = AliasedExprSpec(alias=None, expr=kids[0].val)


class AliasedExprList(ListNonterm, element=AliasedExpr,
                      separator=tokens.T_COMMA):
    pass


# NOTE: This is intentionally not an AST node, since this structure never
# makes it to the actual AST and exists solely for parser convenience.
AliasedExprSpec = collections.namedtuple(
    'AliasedExprSpec', ['alias', 'expr'], module=__name__)


class GroupingIdent(Nonterm):
    def reduce_Identifier(self, *kids):
        self.val = qlast.ObjectRef(name=kids[0].val)

    def reduce_DOT_Identifier(self, *kids):
        self.val = qlast.Path(
            partial=True,
            steps=[qlast.Ptr(ptr=qlast.ObjectRef(name=kids[1].val))],
        )


class GroupingIdentList(ListNonterm, element=GroupingIdent,
                        separator=tokens.T_COMMA):
    pass


class GroupingAtom(Nonterm):
    def reduce_GroupingIdent(self, *kids):
        self.val = kids[0].val

    def reduce_LPAREN_GroupingIdentList_RPAREN(self, *kids):
        self.val = qlast.GroupingIdentList(elements=kids[1].val)


class GroupingAtomList(ListNonterm, element=GroupingAtom,
                       separator=tokens.T_COMMA):
    pass


class GroupingElement(Nonterm):
    def reduce_GroupingAtom(self, *kids):
        self.val = qlast.GroupingSimple(element=kids[0].val)

    def reduce_LBRACE_GroupingElementList_RBRACE(self, *kids):
        self.val = qlast.GroupingSets(sets=kids[1].val)

    def reduce_ROLLUP_LPAREN_GroupingAtomList_RPAREN(self, *kids):
        self.val = qlast.GroupingOperation(oper='rollup', elements=kids[2].val)

    def reduce_CUBE_LPAREN_GroupingAtomList_RPAREN(self, *kids):
        self.val = qlast.GroupingOperation(oper='cube', elements=kids[2].val)


class GroupingElementList(
        ListNonterm, element=GroupingElement, separator=tokens.T_COMMA):
    pass


class SimpleFor(Nonterm):
    def reduce_For(self, *kids):
        r"%reduce FOR Identifier IN AtomicExpr \
                  UNION Expr"
        self.val = qlast.ForQuery(
            iterator_alias=kids[1].val,
            iterator=kids[3].val,
            result=kids[5].val,
        )


class SimpleSelect(Nonterm):
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
    def reduce_BY_GroupingElementList(self, *kids):
        self.val = kids[1].val


class UsingClause(Nonterm):
    def reduce_USING_AliasedExprList(self, *kids):
        self.val = kids[1].val

    def reduce_USING_AliasedExprList_COMMA(self, *kids):
        self.val = kids[1].val


class OptUsingClause(Nonterm):
    def reduce_UsingClause(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = None


class SimpleGroup(Nonterm):
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
    def reduce_COMMA_Identifier(self, *kids):
        self.val = kids[1].val

    def reduce_empty(self, *kids):
        self.val = None


class InternalGroup(Nonterm):
    def reduce_InternalGroup(self, *kids):
        r"%reduce FOR GROUP OptionallyAliasedExpr \
                  UsingClause \
                  ByClause \
                  INTO Identifier OptGroupingAlias \
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
    def reduce_Insert(self, *kids):
        r'%reduce INSERT Expr OptUnlessConflictClause'

        subj = kids[1].val

        # check that the insert subject is either a path or a shape
        if isinstance(subj, qlast.Shape):
            objtype = subj.expr
            shape = subj.elements
        else:
            objtype = subj
            shape = []

        unless_conflict = kids[2].val

        if not isinstance(objtype, qlast.Path):
            raise errors.EdgeQLSyntaxError(
                "insert expression must be an object type reference",
                context=subj.context)

        self.val = qlast.InsertQuery(
            subject=objtype,
            shape=shape,
            unless_conflict=unless_conflict,
        )


class SimpleUpdate(Nonterm):
    def reduce_Update(self, *kids):
        "%reduce UPDATE Expr OptFilterClause SET Shape"
        self.val = qlast.UpdateQuery(
            subject=kids[1].val,
            where=kids[2].val,
            shape=kids[4].val,
        )


class SimpleDelete(Nonterm):
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

    def reduce_WITH_WithDeclList_COMMA(self, *kids):
        aliases = []
        for w in kids[1].val:
            aliases.append(w)
        self.val = WithBlockData(aliases=aliases)


class AliasDecl(Nonterm):
    def reduce_MODULE_ModuleName(self, *kids):
        self.val = qlast.ModuleAliasDecl(
            module='.'.join(kids[1].val))

    def reduce_Identifier_AS_MODULE_ModuleName(self, *kids):
        self.val = qlast.ModuleAliasDecl(
            alias=kids[0].val,
            module='.'.join(kids[3].val))

    def reduce_AliasedExpr(self, *kids):
        self.val = kids[0].val


class WithDecl(Nonterm):
    def reduce_AliasDecl(self, *kids):
        self.val = kids[0].val


class WithDeclList(ListNonterm, element=WithDecl,
                   separator=tokens.T_COMMA):
    pass


class Shape(Nonterm):
    def reduce_LBRACE_RBRACE(self, *kids):
        self.val = None

    def reduce_LBRACE_ShapeElementList_RBRACE(self, *kids):
        self.val = kids[1].val

    def reduce_LBRACE_ShapeElementList_COMMA_RBRACE(self, *kids):
        self.val = kids[1].val


class OptShape(Nonterm):
    def reduce_Shape(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


class TypedShape(Nonterm):
    def reduce_NodeName_OptShape(self, *kids):
        self.val = qlast.Shape(
            expr=qlast.Path(
                steps=[qlast.ObjectRef(
                    name=kids[0].val.name,
                    module=kids[0].val.module,
                    context=kids[0].context)
                ]
            ),
            elements=kids[1].val
        )


class FreeShape(Nonterm):
    def reduce_LBRACE_FreeComputableShapePointerList_RBRACE(self, *kids):
        self.val = qlast.Shape(elements=kids[1].val)

    def reduce_LBRACE_FreeComputableShapePointerList_COMMA_RBRACE(self, *kids):
        self.val = qlast.Shape(elements=kids[1].val)


class OptAnySubShape(Nonterm):
    def reduce_COLON_Shape(self, *kids):
        self.val = kids[1].val

    def reduce_LBRACE(self, *kids):
        raise errors.EdgeQLSyntaxError(
            f"Missing ':' before '{{' in a sub-shape",
            context=kids[0].context)

    def reduce_Shape(self, *kids):
        raise errors.EdgeQLSyntaxError(
            f"Missing ':' before '{{' in a sub-shape",
            context=kids[0].context)

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

    def reduce_ComputableShapePointer(self, *kids):
        self.val = kids[0].val


class ShapeElementList(ListNonterm, element=ShapeElement,
                       separator=tokens.T_COMMA):
    pass


class VerySimpleShapePath(Nonterm):

    def reduce_PathStepName(self, *kids):
        from edb.schema import pointers as s_pointers

        steps = [
            qlast.Ptr(
                ptr=kids[0].val,
                direction=s_pointers.PointerDirection.Outbound
            ),
        ]

        self.val = qlast.Path(steps=steps)


class SimpleShapePath(Nonterm):

    def reduce_VerySimpleShapePath(self, *kids):
        self.val = kids[0].val

    def reduce_AT_ShortNodeName(self, *kids):
        self.val = qlast.Path(
            steps=[
                qlast.Ptr(
                    ptr=kids[1].val,
                    type='property'
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

    def reduce_VerySimpleShapePath(self, *kids):
        self.val = qlast.ShapeElement(
            expr=kids[0].val
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

    def reduce_PathStepName_OptTypeIntersection(self, *kids):
        from edb.schema import pointers as s_pointers

        steps = [
            qlast.Ptr(
                ptr=kids[0].val,
                direction=s_pointers.PointerDirection.Outbound
            ),
        ]

        if kids[1].val is not None:
            steps.append(kids[1].val)

        self.val = qlast.Path(steps=steps)

    def reduce_AT_ShortNodeName(self, *kids):
        self.val = qlast.Path(
            steps=[
                qlast.Ptr(
                    ptr=kids[1].val,
                    type='property'
                )
            ]
        )

    def reduce_TypeIntersection_DOT_PathStepName_OptTypeIntersection(
            self, *kids):
        from edb.schema import pointers as s_pointers

        steps = [
            kids[0].val,
            qlast.Ptr(
                ptr=kids[2].val,
                direction=s_pointers.PointerDirection.Outbound
            ),
        ]

        if kids[3].val is not None:
            steps.append(kids[3].val)

        self.val = qlast.Path(steps=steps)


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

    def reduce_PtrQuals(self, *kids):
        self.val = kids[0].val


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
            context=kids[2].context,
        )

    def reduce_REQUIRED_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.required = True
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[2].context,
        )

    def reduce_MULTI_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.cardinality = qltypes.SchemaCardinality.Many
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[2].context,
        )

    def reduce_SINGLE_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.cardinality = qltypes.SchemaCardinality.One
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[2].context,
        )

    def reduce_OPTIONAL_MULTI_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = False
        self.val.cardinality = qltypes.SchemaCardinality.Many
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[3].context,
        )

    def reduce_OPTIONAL_SINGLE_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = False
        self.val.cardinality = qltypes.SchemaCardinality.One
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[3].context,
        )

    def reduce_REQUIRED_MULTI_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = True
        self.val.cardinality = qltypes.SchemaCardinality.Many
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[3].context,
        )

    def reduce_REQUIRED_SINGLE_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = True
        self.val.cardinality = qltypes.SchemaCardinality.One
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[3].context,
        )

    def reduce_SimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[0].val
        self.val.compexpr = kids[2].val
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[1].context,
        )

    def reduce_SimpleShapePointer_ADDASSIGN_Expr(self, *kids):
        self.val = kids[0].val
        self.val.compexpr = kids[2].val
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.APPEND,
            context=kids[1].context,
        )

    def reduce_SimpleShapePointer_REMASSIGN_Expr(self, *kids):
        self.val = kids[0].val
        self.val.compexpr = kids[2].val
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.SUBTRACT,
            context=kids[1].context,
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
            context=kids[2].context,
        )

    def reduce_REQUIRED_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.required = True
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[2].context,
        )

    def reduce_MULTI_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.cardinality = qltypes.SchemaCardinality.Many
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[2].context,
        )

    def reduce_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.cardinality = qltypes.SchemaCardinality.One
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[2].context,
        )

    def reduce_OPTIONAL_MULTI_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = False
        self.val.cardinality = qltypes.SchemaCardinality.Many
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[3].context,
        )

    def reduce_OPTIONAL_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = False
        self.val.cardinality = qltypes.SchemaCardinality.One
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[3].context,
        )

    def reduce_REQUIRED_MULTI_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = True
        self.val.cardinality = qltypes.SchemaCardinality.Many
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[3].context,
        )

    def reduce_REQUIRED_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = True
        self.val.cardinality = qltypes.SchemaCardinality.One
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[3].context,
        )

    def reduce_FreeSimpleShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[0].val
        self.val.compexpr = kids[2].val
        self.val.operation = qlast.ShapeOperation(
            op=qlast.ShapeOp.ASSIGN,
            context=kids[1].context,
        )


class FreeComputableShapePointerList(ListNonterm,
                                     element=FreeComputableShapePointer,
                                     separator=tokens.T_COMMA):
    pass


class UnlessConflictSpecifier(Nonterm):
    def reduce_ON_Expr_ELSE_Expr(self, *kids):
        self.val = (kids[1].val, kids[3].val)

    def reduce_ON_Expr(self, *kids):
        self.val = (kids[1].val, None)

    def reduce_empty(self, *kids):
        self.val = (None, None)


class UnlessConflictCause(Nonterm):
    def reduce_UNLESS_CONFLICT_UnlessConflictSpecifier(self, *kids):
        self.val = kids[2].val


class OptUnlessConflictClause(Nonterm):
    def reduce_UnlessConflictCause(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = None


class FilterClause(Nonterm):
    def reduce_FILTER_Expr(self, *kids):
        self.val = kids[1].val


class OptFilterClause(Nonterm):
    def reduce_FilterClause(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = None


class SortClause(Nonterm):
    def reduce_ORDERBY_OrderbyList(self, *kids):
        self.val = kids[1].val


class OptSortClause(Nonterm):
    def reduce_SortClause(self, *kids):
        self.val = kids[0].val

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
    def reduce_SelectLimit(self, *kids):
        self.val = kids[0].val

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
    def reduce_OFFSET_Expr(self, *kids):
        self.val = kids[1].val


class LimitClause(Nonterm):
    def reduce_LIMIT_Expr(self, *kids):
        self.val = kids[1].val


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
    def reduce_LPAREN_Expr_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_ExprStmt_RPAREN(self, *kids):
        self.val = kids[1].val


class BaseAtomicExpr(Nonterm):
    # { ... } | Constant | '(' Expr ')' | FuncExpr
    # | Tuple | NamedTuple | Collection | Set
    # | '__source__' | '__subject__'
    # | NodeName | PathStep

    def reduce_FreeShape(self, *kids):
        self.val = kids[0].val

    def reduce_Constant(self, *kids):
        self.val = kids[0].val

    def reduce_DUNDERSOURCE(self, *kids):
        self.val = qlast.Path(steps=[qlast.Source()])

    def reduce_DUNDERSUBJECT(self, *kids):
        self.val = qlast.Path(steps=[qlast.Subject()])

    @parsing.precedence(precedence.P_UMINUS)
    def reduce_ParenExpr(self, *kids):
        self.val = kids[0].val

    def reduce_FuncExpr(self, *kids):
        self.val = kids[0].val

    def reduce_Tuple(self, *kids):
        self.val = kids[0].val

    def reduce_Collection(self, *kids):
        self.val = kids[0].val

    def reduce_Set(self, *kids):
        self.val = kids[0].val

    def reduce_NamedTuple(self, *kids):
        self.val = kids[0].val

    @parsing.precedence(precedence.P_DOT)
    def reduce_NodeName(self, *kids):
        self.val = qlast.Path(
            steps=[qlast.ObjectRef(name=kids[0].val.name,
                                   module=kids[0].val.module)])

    @parsing.precedence(precedence.P_DOT)
    def reduce_PathStep(self, *kids):
        self.val = qlast.Path(steps=[kids[0].val], partial=True)

    @parsing.precedence(precedence.P_DOT)
    def reduce_DOT_FCONST(self, *kids):
        # this is a valid link-like syntax for accessing unnamed tuples
        self.val = qlast.Path(
            steps=_float_to_path(kids[1], kids[0].context),
            partial=True)


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
    # | EXISTS Expr

    def reduce_BaseAtomicExpr(self, *kids):
        self.val = kids[0].val

    def reduce_Path(self, *kids):
        self.val = kids[0].val

    def reduce_Expr_Shape(self, *kids):
        self.val = qlast.Shape(expr=kids[0].val, elements=kids[1].val)

    def reduce_EXISTS_Expr(self, *kids):
        self.val = qlast.UnaryOp(op='EXISTS', operand=kids[1].val)

    def reduce_DISTINCT_Expr(self, *kids):
        self.val = qlast.UnaryOp(op='DISTINCT', operand=kids[1].val)

    def reduce_DETACHED_Expr(self, *kids):
        self.val = qlast.DetachedExpr(expr=kids[1].val)

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
        if isinstance(arg, qlast.BaseRealConstant):
            # Special case for -<real_const> so that type inference based
            # on literal size works correctly in the case of INT_MIN and
            # friends.
            self.val = type(arg)(value=arg.value, is_negative=True)
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

    def reduce_Expr_LANGBRACKET_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    def reduce_Expr_RANGBRACKET_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    @parsing.precedence(precedence.P_DOUBLEQMARK_OP)
    def reduce_Expr_DOUBLEQMARK_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    def reduce_Expr_EQUALS_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=kids[1].val,
                               right=kids[2].val)

    @parsing.precedence(precedence.P_OP)
    def reduce_Expr_OP_Expr(self, *kids):
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
        self.val = qlast.IfElse(
            if_expr=kids[0].val, condition=kids[2].val, else_expr=kids[4].val)

    def reduce_Expr_UNION_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op='UNION',
                               right=kids[2].val)


class Tuple(Nonterm):
    def reduce_LPAREN_Expr_COMMA_OptExprList_RPAREN(self, *kids):
        self.val = qlast.Tuple(elements=[kids[1].val] + kids[3].val)

    def reduce_LPAREN_RPAREN(self, *kids):
        self.val = qlast.Tuple(elements=[])


class NamedTuple(Nonterm):
    def reduce_LPAREN_NamedTupleElementList_RPAREN(self, *kids):
        self.val = qlast.NamedTuple(elements=kids[1].val)

    def reduce_LPAREN_NamedTupleElementList_COMMA_RPAREN(self, *kids):
        self.val = qlast.NamedTuple(elements=kids[1].val)


class NamedTupleElement(Nonterm):
    def reduce_ShortNodeName_ASSIGN_Expr(self, *kids):
        self.val = qlast.TupleElement(
            name=kids[0].val,
            val=kids[2].val
        )


class NamedTupleElementList(ListNonterm, element=NamedTupleElement,
                            separator=tokens.T_COMMA):
    pass


class Set(Nonterm):
    def reduce_LBRACE_OptExprList_RBRACE(self, *kids):
        self.val = qlast.Set(elements=kids[1].val)


class Collection(Nonterm):
    def reduce_LBRACKET_OptExprList_RBRACKET(self, *kids):
        elements = kids[1].val
        self.val = qlast.Array(elements=elements)


class OptExprList(Nonterm):
    def reduce_ExprList_COMMA(self, *kids):
        self.val = kids[0].val

    def reduce_ExprList(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


class ExprList(ListNonterm, element=Expr, separator=tokens.T_COMMA):
    pass


class Constant(Nonterm):
    # ARGUMENT
    # | BaseNumberConstant
    # | BaseStringConstant
    # | BaseBooleanConstant
    # | BaseBytesConstant

    def reduce_ARGUMENT(self, *kids):
        self.val = qlast.Parameter(name=kids[0].val[1:], optional=False)

    def reduce_BaseNumberConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseStringConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseBooleanConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseBytesConstant(self, *kids):
        self.val = kids[0].val


class BaseNumberConstant(Nonterm):
    def reduce_ICONST(self, *kids):
        self.val = qlast.IntegerConstant(value=kids[0].val)

    def reduce_FCONST(self, *kids):
        self.val = qlast.FloatConstant(value=kids[0].val)

    def reduce_NICONST(self, *kids):
        self.val = qlast.BigintConstant(value=kids[0].val)

    def reduce_NFCONST(self, *kids):
        self.val = qlast.DecimalConstant(value=kids[0].val)


class BaseStringConstant(Nonterm):

    def reduce_SCONST(self, token):
        self.val = qlast.StringConstant(value=token.clean_value)


class BaseBytesConstant(Nonterm):

    def reduce_BCONST(self, bytes_tok):
        self.val = qlast.BytesConstant(value=bytes_tok.clean_value)


class BaseBooleanConstant(Nonterm):
    def reduce_TRUE(self, *kids):
        self.val = qlast.BooleanConstant(value='true')

    def reduce_FALSE(self, *kids):
        self.val = qlast.BooleanConstant(value='false')


def ensure_path(expr):
    if not isinstance(expr, qlast.Path):
        expr = qlast.Path(steps=[expr])
    return expr


def _float_to_path(self, token, context):
    from edb.schema import pointers as s_pointers

    # make sure that the float is of the type 0.1
    parts = token.val.split('.')
    if not (len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit()):
        raise errors.EdgeQLSyntaxError(
            f"Unexpected {token.val!r}",
            context=token.context)

    # context for the AST is established manually here
    return [
        qlast.Ptr(
            ptr=qlast.ObjectRef(
                name=parts[0],
                context=token.context,
            ),
            direction=s_pointers.PointerDirection.Outbound,
            context=context,
        ),
        qlast.Ptr(
            ptr=qlast.ObjectRef(
                name=parts[1],
                context=token.context,
            ),
            direction=s_pointers.PointerDirection.Outbound,
            context=token.context,
        )
    ]


class Path(Nonterm):
    @parsing.precedence(precedence.P_DOT)
    def reduce_Expr_PathStep(self, *kids):
        path = ensure_path(kids[0].val)
        path.steps.append(kids[1].val)
        self.val = path

    # special case of Path.0.1 etc.
    @parsing.precedence(precedence.P_DOT)
    def reduce_Expr_DOT_FCONST(self, *kids):
        # this is a valid link-like syntax for accessing unnamed tuples
        path = ensure_path(kids[0].val)
        path.steps.extend(_float_to_path(kids[2], kids[1].context))
        self.val = path


class AtomicExpr(Nonterm):
    def reduce_BaseAtomicExpr(self, *kids):
        self.val = kids[0].val

    def reduce_AtomicPath(self, *kids):
        self.val = kids[0].val

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

    # special case of Path.0.1 etc.
    @parsing.precedence(precedence.P_DOT)
    def reduce_AtomicExpr_DOT_FCONST(self, *kids):
        # this is a valid link-like syntax for accessing unnamed tuples
        path = ensure_path(kids[0].val)
        path.steps.extend(_float_to_path(kids[2], kids[1].context))
        self.val = path


class PathStep(Nonterm):
    def reduce_DOT_PathStepName(self, *kids):
        from edb.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            ptr=kids[1].val,
            direction=s_pointers.PointerDirection.Outbound
        )

    def reduce_DOT_ICONST(self, *kids):
        # this is a valid link-like syntax for accessing unnamed tuples
        from edb.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            ptr=qlast.ObjectRef(name=kids[1].val),
            direction=s_pointers.PointerDirection.Outbound
        )

    def reduce_DOTBW_PathStepName(self, *kids):
        from edb.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            ptr=kids[1].val,
            direction=s_pointers.PointerDirection.Inbound
        )

    def reduce_AT_ShortNodeName(self, *kids):
        from edb.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            ptr=kids[1].val,
            direction=s_pointers.PointerDirection.Outbound,
            type='property'
        )

    def reduce_TypeIntersection(self, *kids):
        self.val = kids[0].val


class TypeIntersection(Nonterm):
    def reduce_LBRACKET_IS_FullTypeExpr_RBRACKET(self, *kids):
        self.val = qlast.TypeIntersection(
            type=kids[2].val,
        )


class OptTypeIntersection(Nonterm):
    def reduce_TypeIntersection(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self):
        self.val = None


class PathStepName(Nonterm):
    def reduce_ShortNodeName(self, *kids):
        self.val = kids[0].val

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
                        context=argname_ctx)

                last_named_seen = argname
                kwargs[argname] = arg

            else:
                if last_named_seen is not None:
                    raise errors.EdgeQLSyntaxError(
                        f"positional argument after named "
                        f"argument `{last_named_seen}`",
                        context=arg.context)
                args.append(arg)

        self.val = qlast.FunctionCall(func=name, args=args, kwargs=kwargs)


class FuncExpr(Nonterm):
    def reduce_FuncApplication(self, *kids):
        self.val = kids[0].val


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
            kids[0].context,
            kids[2].val,
        )

    def reduce_ARGUMENT_ASSIGN_Expr(self, *kids):
        if kids[0].val[1].isdigit():
            raise errors.EdgeQLSyntaxError(
                f"numeric named arguments are not supported",
                context=kids[0].context)
        else:
            raise errors.EdgeQLSyntaxError(
                f"named arguments do not need a '$' prefix, "
                f"rewrite as '{kids[0].val[1:]} := ...'",
                context=kids[0].context)


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


class FuncArgList(ListNonterm, element=FuncCallArg, separator=tokens.T_COMMA):
    pass


class OptFuncArgList(Nonterm):
    def reduce_FuncArgList_COMMA(self, *kids):
        self.val = kids[0].val

    def reduce_FuncArgList(self, *kids):
        self.val = kids[0].val

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
    def reduce_PosCallArgList(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


class Identifier(Nonterm):
    def reduce_IDENT(self, *kids):
        self.val = kids[0].clean_value

    def reduce_UnreservedKeyword(self, *kids):
        self.val = kids[0].val


class AnyIdentifier(Nonterm):
    def reduce_Identifier(self, *kids):
        self.val = kids[0].val

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
                context=kids[0].context)

        self.val = name


class ModuleName(ListNonterm, element=AnyIdentifier, separator=tokens.T_DOT):
    pass


# this can appear anywhere
class BaseName(Nonterm):
    def reduce_Identifier(self, *kids):
        self.val = [kids[0].val]

    def reduce_Identifier_DOUBLECOLON_AnyIdentifier(self, *kids):
        self.val = [kids[0].val, kids[2].val]

    def reduce_DUNDERSTD_DOUBLECOLON_AnyIdentifier(self, *kids):
        self.val = ['__std__', kids[2].val]


# Non-collection type.
class SimpleTypeName(Nonterm):
    def reduce_NodeName(self, *kids):
        self.val = qlast.TypeName(maintype=kids[0].val)

    def reduce_ANYTYPE(self, *kids):
        self.val = qlast.TypeName(maintype=qlast.AnyType())

    def reduce_ANYTUPLE(self, *kids):
        self.val = qlast.TypeName(maintype=qlast.AnyTuple())


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
                context=lst.context)

        if has_items and has_nonstrval:
            # Prohibit cases like `tuple<a: int64, int32>`
            raise errors.EdgeQLSyntaxError(
                "mixing named and unnamed subtype declarations "
                "is not supported",
                context=lst.context)

    def reduce_NodeName_LANGBRACKET_RANGBRACKET(self, *kids):
        # Constructs like `enum<>` or `array<>` aren't legal.
        raise errors.EdgeQLSyntaxError(
            'parametrized type must have at least one argument',
            context=kids[1].context,
        )

    def reduce_NodeName_LANGBRACKET_SubtypeList_RANGBRACKET(self, *kids):
        self.validate_subtype_list(kids[2])
        self.val = qlast.TypeName(
            maintype=kids[0].val,
            subtypes=kids[2].val,
        )

    def reduce_NodeName_LANGBRACKET_SubtypeList_COMMA_RANGBRACKET(self, *kids):
        self.validate_subtype_list(kids[2])
        self.val = qlast.TypeName(
            maintype=kids[0].val,
            subtypes=kids[2].val,
        )


class TypeName(Nonterm):
    def reduce_SimpleTypeName(self, *kids):
        self.val = kids[0].val

    def reduce_CollectionTypeName(self, *kids):
        self.val = kids[0].val


class TypeNameList(ListNonterm, element=TypeName,
                   separator=tokens.T_COMMA):
    pass


# This is a type expression without angle brackets, so it
# can be used without parentheses in a context where the
# angle bracket has a different meaning.
class TypeExpr(Nonterm):
    def reduce_SimpleTypeName(self, *kids):
        self.val = kids[0].val

    def reduce_TYPEOF_Expr(self, *kids):
        self.val = qlast.TypeOf(expr=kids[1].val)

    def reduce_LPAREN_FullTypeExpr_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_TypeExpr_PIPE_TypeExpr(self, *kids):
        self.val = qlast.TypeOp(left=kids[0].val, op='|',
                                right=kids[2].val)

    def reduce_TypeExpr_AMPER_TypeExpr(self, *kids):
        self.val = qlast.TypeOp(left=kids[0].val, op='&',
                                right=kids[2].val)


# This is a type expression which includes collection types,
# so it can only be directly used in a context where the
# angle bracket is unambiguous.
class FullTypeExpr(Nonterm):
    def reduce_TypeName(self, *kids):
        self.val = kids[0].val

    def reduce_TYPEOF_Expr(self, *kids):
        self.val = qlast.TypeOf(expr=kids[1].val)

    def reduce_LPAREN_FullTypeExpr_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_FullTypeExpr_PIPE_FullTypeExpr(self, *kids):
        self.val = qlast.TypeOp(left=kids[0].val, op='|',
                                right=kids[2].val)

    def reduce_FullTypeExpr_AMPER_FullTypeExpr(self, *kids):
        self.val = qlast.TypeOp(left=kids[0].val, op='&',
                                right=kids[2].val)


class Subtype(Nonterm):
    def reduce_FullTypeExpr(self, *kids):
        self.val = kids[0].val

    def reduce_Identifier_COLON_FullTypeExpr(self, *kids):
        self.val = kids[2].val
        self.val.name = kids[0].val

    def reduce_BaseStringConstant(self, *kids):
        # TODO: Raise a DeprecationWarning once we have facility for that.
        self.val = qlast.TypeExprLiteral(
            val=kids[0].val,
        )


class SubtypeList(ListNonterm, element=Subtype, separator=tokens.T_COMMA):
    pass


class NodeName(Nonterm):
    # NOTE: Generic short of fully-qualified name.
    #
    # This name is safe to be used anywhere as it starts with IDENT only.

    def reduce_BaseName(self, *kids):
        self.val = qlast.ObjectRef(
            module='.'.join(kids[0].val[:-1]) or None,
            name=kids[0].val[-1])


class NodeNameList(ListNonterm, element=NodeName, separator=tokens.T_COMMA):
    pass


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


class KeywordMeta(parsing.NontermMeta):
    def __new__(mcls, name, bases, dct, *, type):
        result = super().__new__(mcls, name, bases, dct)

        assert type in keywords.keyword_types

        for token in keywords.by_type[type].values():
            def method(inst, *kids):
                inst.val = kids[0].val
            method = context.has_context(method)
            method.__doc__ = "%%reduce %s" % token
            method.__name__ = 'reduce_%s' % token
            setattr(result, method.__name__, method)

        return result

    def __init__(cls, name, bases, dct, *, type):
        super().__init__(name, bases, dct)


class UnreservedKeyword(Nonterm, metaclass=KeywordMeta,
                        type=keywords.UNRESERVED_KEYWORD):
    pass


class ReservedKeyword(Nonterm, metaclass=KeywordMeta,
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
