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
import re
import typing

from edb.common import parsing, context

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.errors import EdgeQLSyntaxError

from . import keywords
from . import lexutils
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


# ByExpr will eventually be expanded to include more than just
# Identifiers as its members (such as CUBE, ROLLUP and grouping sets).
class ByExpr(Nonterm):
    def reduce_Identifier(self, *kids):
        self.val = qlast.Path(steps=[qlast.ObjectRef(name=kids[0].val)])


class ByExprList(ListNonterm, element=ByExpr, separator=tokens.T_COMMA):
    pass


class SimpleFor(Nonterm):
    def reduce_For(self, *kids):
        r"%reduce FOR Identifier IN Set \
                  UNION OptionallyAliasedExpr"
        self.val = qlast.ForQuery(
            iterator_alias=kids[1].val,
            iterator=kids[3].val,
            result=kids[5].val.expr,
            result_alias=kids[5].val.alias,
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


class SimpleGroup(Nonterm):
    def reduce_Group(self, *kids):
        r"%reduce GROUP OptionallyAliasedExpr \
                  USING AliasedExprList \
                  BY ByExprList \
                  INTO Identifier \
                  UNION OptionallyAliasedExpr \
                  OptFilterClause OptSortClause OptSelectLimit"
        self.val = qlast.GroupQuery(
            subject=kids[1].val.expr,
            subject_alias=kids[1].val.alias,
            using=kids[3].val,
            by=kids[5].val,
            into=kids[7].val,
            result=kids[9].val.expr,
            result_alias=kids[9].val.alias,
            where=kids[10].val,
            orderby=kids[11].val,
            offset=kids[12].val[0],
            limit=kids[12].val[1],
        )


class SimpleInsert(Nonterm):
    def reduce_Insert(self, *kids):
        r'%reduce INSERT OptionallyAliasedExpr'

        subj = kids[1].val.expr
        subj_alias = kids[1].val.alias

        # check that the insert subject is either a path or a shape
        if isinstance(subj, qlast.Shape):
            objtype = subj.expr
            shape = subj.elements
        else:
            objtype = subj
            shape = []

        if not isinstance(objtype, qlast.Path):
            raise EdgeQLSyntaxError(
                "insert expression must be an object type or a view",
                context=subj.context)

        self.val = qlast.InsertQuery(
            subject=objtype,
            subject_alias=subj_alias,
            shape=shape,
        )


class SimpleUpdate(Nonterm):
    def reduce_Update(self, *kids):
        "%reduce UPDATE OptionallyAliasedExpr OptFilterClause SET Shape"
        self.val = qlast.UpdateQuery(
            subject=kids[1].val.expr,
            subject_alias=kids[1].val.alias,
            where=kids[2].val,
            shape=kids[4].val,
        )


class SimpleDelete(Nonterm):
    def reduce_Delete(self, *kids):
        r"%reduce DELETE OptionallyAliasedExpr \
                  OptFilterClause OptSortClause OptSelectLimit"
        self.val = qlast.DeleteQuery(
            subject=kids[1].val.expr,
            subject_alias=kids[1].val.alias,
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


class OptAnySubShape(Nonterm):
    def reduce_COLON_Shape(self, *kids):
        self.val = kids[1].val

    def reduce_COLON_TypedShape(self, *kids):
        # typed shape needs to be transformed here into a different
        # expression
        shape = kids[1].val
        self.val = [qlast.TypeName(
            maintype=shape.expr.steps[0],
            context=context.get_context(shape.expr.steps[0])
        )]
        self.val += shape.elements

    def reduce_LBRACE(self, *kids):
        raise EdgeQLSyntaxError(
            f"Missing ':' before '{{' in a sub-shape",
            context=kids[0].context)

    def reduce_Shape(self, *kids):
        raise EdgeQLSyntaxError(
            f"Missing ':' before '{{' in a sub-shape",
            context=kids[0].context)

    def reduce_empty(self, *kids):
        self.val = None


class ShapeElement(Nonterm):
    def reduce_ShapeElementWithSubShape(self, *kids):
        r"""%reduce ShapePointer \
             OptAnySubShape OptFilterClause OptSortClause OptSelectLimit \
        """
        self.val = kids[0].val

        shape = kids[1].val

        # shape elements can have a path starting with a TypeExpr,
        # this indicates a polymorphic shape and TypeExpr must be
        # extracted from the path steps
        if shape and isinstance(shape[0], qlast.TypeExpr):
            self.val.expr.steps.append(qlast.TypeIndirection(
                type=shape[0], context=shape[0].context))
            self.val.elements = shape[1:]
        else:
            self.val.elements = shape or []

        self.val.where = kids[2].val
        self.val.orderby = kids[3].val
        self.val.offset = kids[4].val[0]
        self.val.limit = kids[4].val[1]

    def reduce_ComputableShapePointer(self, *kids):
        self.val = kids[0].val


class ShapeElementList(ListNonterm, element=ShapeElement,
                       separator=tokens.T_COMMA):
    pass


class ShapePath(Nonterm):
    # A form of Path appearing as an element in shapes.
    #
    # one-of:
    #   __type__
    #   link
    #   @prop
    #   [IS ObjectType].link
    #   [IS Link]@prop - currently not supported

    def reduce_PathStepName(self, *kids):
        from edb.schema import pointers as s_pointers

        self.val = qlast.Path(
            steps=[
                qlast.Ptr(
                    ptr=kids[0].val,
                    direction=s_pointers.PointerDirection.Outbound
                )
            ]
        )

    def reduce_AT_ShortNodeName(self, *kids):
        self.val = qlast.Path(
            steps=[
                qlast.Ptr(
                    ptr=kids[1].val,
                    type='property'
                )
            ]
        )

    def reduce_ShapePathPtr_DOT_PathStepName(self, *kids):
        from edb.schema import pointers as s_pointers

        self.val = qlast.Path(
            steps=[
                qlast.TypeIndirection(
                    type=kids[0].val,
                ),
                qlast.Ptr(
                    ptr=kids[2].val,
                    direction=s_pointers.PointerDirection.Outbound
                ),
            ]
        )


class ShapePathPtr(Nonterm):
    def reduce_LBRACKET_IS_FullTypeExpr_RBRACKET(self, *kids):
        self.val = kids[2].val


class ShapePointer(Nonterm):
    def reduce_ShapePath(self, *kids):
        self.val = qlast.ShapeElement(
            expr=kids[0].val
        )


class PtrQualsSpec(typing.NamedTuple):
    required: typing.Optional[bool] = None
    cardinality: typing.Optional[qltypes.Cardinality] = None


class PtrQuals(Nonterm):
    def reduce_REQUIRED(self, *kids):
        self.val = PtrQualsSpec(required=True)

    def reduce_SINGLE(self, *kids):
        self.val = PtrQualsSpec(cardinality=qltypes.Cardinality.ONE)

    def reduce_MULTI(self, *kids):
        self.val = PtrQualsSpec(cardinality=qltypes.Cardinality.MANY)

    def reduce_REQUIRED_SINGLE(self, *kids):
        self.val = PtrQualsSpec(
            required=True, cardinality=qltypes.Cardinality.ONE)

    def reduce_REQUIRED_MULTI(self, *kids):
        self.val = PtrQualsSpec(
            required=True, cardinality=qltypes.Cardinality.MANY)


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

    def reduce_REQUIRED_ShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.required = True

    def reduce_MULTI_ShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.cardinality = qltypes.Cardinality.MANY

    def reduce_SINGLE_ShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[1].val
        self.val.compexpr = kids[3].val
        self.val.cardinality = qltypes.Cardinality.ONE

    def reduce_REQUIRED_MULTI_ShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = True
        self.val.cardinality = qltypes.Cardinality.MANY

    def reduce_REQUIRED_SINGLE_ShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[2].val
        self.val.compexpr = kids[4].val
        self.val.required = True
        self.val.cardinality = qltypes.Cardinality.ONE

    def reduce_ShapePointer_ASSIGN_Expr(self, *kids):
        self.val = kids[0].val
        self.val.compexpr = kids[2].val


class FilterClause(Nonterm):
    def reduce_FILTER_Expr(self, *kids):
        self.val = kids[1].val


class OptFilterClause(Nonterm):
    def reduce_FilterClause(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = None


class SortClause(Nonterm):
    def reduce_ORDER_BY_OrderbyList(self, *kids):
        self.val = kids[2].val


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


class Expr(Nonterm):
    # Path | Expr { ... } | Constant | '(' Expr ')' | FuncExpr
    # | Tuple | NamedTuple | Collection | Set
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
    # | Expr '[' Expr ']'
    # | Expr '[' Expr ':' Expr ']'
    # | Expr '[' ':' Expr ']'
    # | Expr '[' Expr ':' ']'
    # | Expr '[' IS NodeName ']'
    # | '<' TypeName '>' Expr
    # | Expr IF Expr ELSE Expr
    # | Expr ?? Expr
    # | Expr UNION Expr | Expr UNION Expr
    # | DISTINCT Expr
    # | DETACHED Expr
    # | EXISTS Expr
    # | '__source__' | '__subject__'

    def reduce_Path(self, *kids):
        self.val = kids[0].val

    def reduce_Expr_Shape(self, *kids):
        self.val = qlast.Shape(expr=kids[0].val, elements=kids[1].val)

    def reduce_Constant(self, *kids):
        self.val = kids[0].val

    def reduce_DUNDERSOURCE(self, *kids):
        self.val = qlast.Path(steps=[qlast.Source()])

    def reduce_DUNDERSUBJECT(self, *kids):
        self.val = qlast.Path(steps=[qlast.Subject()])

    @parsing.precedence(precedence.P_UMINUS)
    def reduce_ParenExpr(self, *kids):
        self.val = kids[0].val

    def reduce_Expr_IndirectionEl(self, *kids):
        expr = kids[0].val
        if isinstance(expr, qlast.Indirection):
            self.val = expr
            expr.indirection.append(kids[1].val)
        else:
            self.val = qlast.Indirection(arg=expr,
                                         indirection=[kids[1].val])

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

    def reduce_EXISTS_Expr(self, *kids):
        self.val = qlast.UnaryOp(op='EXISTS', operand=kids[1].val)

    def reduce_DISTINCT_Expr(self, *kids):
        self.val = qlast.UnaryOp(op='DISTINCT', operand=kids[1].val)

    def reduce_DETACHED_Expr(self, *kids):
        self.val = qlast.DetachedExpr(expr=kids[1].val)

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
        self.val = qlast.TypeCast(expr=kids[3].val, type=kids[1].val)

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
    # ArgConstant
    # | BaseNumberConstant
    # | BaseStringConstant
    # | BaseBooleanConstant
    # | BaseBytesConstant

    def reduce_ArgConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseNumberConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseStringConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseBooleanConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseBytesConstant(self, *kids):
        self.val = kids[0].val


class ArgConstant(Nonterm):
    def reduce_DOLLAR_ICONST(self, *kids):
        self.val = qlast.Parameter(name=str(kids[1].val))

    def reduce_DOLLAR_AnyIdentifier(self, *kids):
        self.val = qlast.Parameter(name=kids[1].val)


class BaseNumberConstant(Nonterm):
    def reduce_ICONST(self, *kids):
        self.val = qlast.IntegerConstant(value=kids[0].val)

    def reduce_FCONST(self, *kids):
        self.val = qlast.FloatConstant(value=kids[0].val)

    def reduce_NICONST(self, *kids):
        self.val = qlast.BigintConstant(value=kids[0].val)

    def reduce_NFCONST(self, *kids):
        self.val = qlast.DecimalConstant(value=kids[0].val)


class RawStringConstant(Nonterm):

    def reduce_RSCONST(self, str_tok):
        match = lexutils.VALID_RAW_STRING_RE.match(str_tok.val)
        if not match:
            raise EdgeQLSyntaxError(
                f"invalid raw string literal", context=str_tok.context)

        quote = match.group('Q')
        val = match.group('body')

        self.val = qlast.RawStringConstant(value=val, quote=quote)


class EscapedStringConstant(Nonterm):

    def reduce_SCONST(self, str_tok):
        match = lexutils.VALID_STRING_RE.match(str_tok.val)

        if not match:
            raise EdgeQLSyntaxError(
                f"invalid string literal", context=str_tok.context)
        if match.group('err_esc'):
            raise EdgeQLSyntaxError(
                f"invalid string literal: invalid escape sequence "
                f"'{match.group('err_esc')}'",
                context=str_tok.context)

        quote = match.group('Q')
        val = match.group('body')

        # handle line continuations
        val = re.sub(r'\\\n', '', val)

        self.val = qlast.StringConstant(value=val, quote=quote)


class BaseStringConstant(Nonterm):

    def reduce_EscapedStringConstant(self, *kids):
        self.val = kids[0].val

    def reduce_RawStringConstant(self, *kids):
        self.val = kids[0].val


class BaseBytesConstant(Nonterm):

    def reduce_BCONST(self, bytes_tok):
        val = bytes_tok.val
        match = lexutils.VALID_BYTES_RE.match(val)

        if not match:
            raise EdgeQLSyntaxError(
                f"invalid bytes literal", context=bytes_tok.context)
        if match.group('err_esc'):
            raise EdgeQLSyntaxError(
                f"invalid bytes literal: invalid escape sequence "
                f"'{match.group('err_esc')}'",
                context=bytes_tok.context)
        if match.group('err'):
            raise EdgeQLSyntaxError(
                f"invalid bytes literal: character '{match.group('err')}' "
                f"is outside of the ASCII range",
                context=bytes_tok.context)

        self.val = qlast.BytesConstant(
            value=match.group('body'),
            quote=match.group('BQ'))


class BaseBooleanConstant(Nonterm):
    def reduce_TRUE(self, *kids):
        self.val = qlast.BooleanConstant(value='true')

    def reduce_FALSE(self, *kids):
        self.val = qlast.BooleanConstant(value='false')


class Path(Nonterm):
    @parsing.precedence(precedence.P_DOT)
    def reduce_NodeName(self, *kids):
        self.val = qlast.Path(
            steps=[qlast.ObjectRef(name=kids[0].val.name,
                                   module=kids[0].val.module)])

    @parsing.precedence(precedence.P_DOT)
    def reduce_Expr_PathStep(self, *kids):
        path = kids[0].val
        if not isinstance(path, qlast.Path):
            path = qlast.Path(steps=[path])

        path.steps.append(kids[1].val)
        self.val = path

    @parsing.precedence(precedence.P_DOT)
    def reduce_PathStep(self, *kids):
        self.val = qlast.Path(steps=[kids[0].val], partial=True)

    # special case of Path.0.1 etc.
    @parsing.precedence(precedence.P_DOT)
    def reduce_Expr_DOT_FCONST(self, *kids):
        # this is a valid link-like syntax for accessing unnamed tuples
        path = kids[0].val
        if not isinstance(path, qlast.Path):
            path = qlast.Path(steps=[path])

        path.steps.extend(self._float_to_path(kids[2], kids[1].context))
        self.val = path

    @parsing.precedence(precedence.P_DOT)
    def reduce_DOT_FCONST(self, *kids):
        # this is a valid link-like syntax for accessing unnamed tuples
        self.val = qlast.Path(
            steps=self._float_to_path(kids[1], kids[0].context),
            partial=True)

    @parsing.precedence(precedence.P_DOT)
    def reduce_Expr_DOTFW_FCONST(self, *kids):
        # this is a valid link-like syntax for accessing unnamed tuples
        path = kids[0].val
        if not isinstance(path, qlast.Path):
            path = qlast.Path(steps=[path])

        path.steps.extend(self._float_to_path(kids[2], kids[1].context))
        self.val = path

    @parsing.precedence(precedence.P_DOT)
    def reduce_DOTFW_FCONST(self, *kids):
        # this is a valid link-like syntax for accessing unnamed tuples
        self.val = qlast.Path(
            steps=self._float_to_path(kids[1], kids[0].context),
            partial=True)

    def _float_to_path(self, token, context):
        from edb.schema import pointers as s_pointers

        # make sure that the float is of the type 0.1
        parts = token.val.split('.')
        if not (len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit()):
            raise EdgeQLSyntaxError(
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

    def reduce_DOTFW_ICONST(self, *kids):
        # this is a valid link-like syntax for accessing unnamed tuples
        from edb.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            ptr=qlast.ObjectRef(name=kids[1].val),
            direction=s_pointers.PointerDirection.Outbound
        )

    def reduce_DOTFW_PathStepName(self, *kids):
        from edb.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            ptr=kids[1].val,
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

    def reduce_LBRACKET_IS_FullTypeExpr_RBRACKET(self, *kids):
        self.val = qlast.TypeIndirection(
            type=kids[2].val,
        )


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
                    raise EdgeQLSyntaxError(
                        f"duplicate named argument `{argname}`",
                        context=argname_ctx)

                last_named_seen = argname
                kwargs[argname] = arg

            else:
                if last_named_seen is not None:
                    raise EdgeQLSyntaxError(
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

    def reduce_DOLLAR_ICONST_ASSIGN_Expr(self, *kids):
        raise EdgeQLSyntaxError(
            f"numeric named arguments are not supported",
            context=kids[0].context)

    def reduce_DOLLAR_AnyIdentifier_ASSIGN_Expr(self, *kids):
        raise EdgeQLSyntaxError(
            f"named arguments do not need a '$' prefix: "
            f"rewrite as '{kids[1].val} := ...'",
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
        self.val = kids[0].val

    def reduce_UnreservedKeyword(self, *kids):
        self.val = kids[0].val


class AnyIdentifier(Nonterm):
    def reduce_Identifier(self, *kids):
        self.val = kids[0].val

    def reduce_ReservedKeyword(self, *kids):
        self.val = kids[0].val


class ModuleName(ListNonterm, element=AnyIdentifier, separator=tokens.T_DOT):
    pass


# this can appear anywhere
class BaseName(Nonterm):
    def reduce_Identifier(self, *kids):
        self.val = [kids[0].val]

    def reduce_Identifier_DOUBLECOLON_AnyIdentifier(self, *kids):
        self.val = [kids[0].val, kids[2].val]


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
    def reduce_NodeName_LANGBRACKET_RANGBRACKET(self, *kids):
        self.val = qlast.TypeName(
            maintype=kids[0].val,
            subtypes=[],
        )

    def reduce_NodeName_LANGBRACKET_SubtypeList_RANGBRACKET(self, *kids):
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

    def reduce_EscapedStringConstant(self, *kids):
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
