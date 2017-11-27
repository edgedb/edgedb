##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from edgedb.lang.common import ast
from edgedb.lang.common import parsing, context

from edgedb.lang.edgeql import ast as qlast

from ...errors import EdgeQLSyntaxError

from . import keywords, precedence, tokens

from .precedence import *  # NOQA
from .tokens import *  # NOQA


class Nonterm(parsing.Nonterm):
    pass


class ListNonterm(parsing.ListNonterm, element=None):
    pass


class SetStmt(Nonterm):
    def reduce_AliasBlock_SetStmtCore(self, *kids):
        self.val = kids[1].val
        self.val.aliases = kids[0].val.aliases
        self.val.cardinality = kids[0].val.cardinality

    def reduce_SetStmtCore(self, *kids):
        self.val = kids[0].val


class SetStmtCore(Nonterm):
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
    def reduce_Identifier_TURNSTILE_Expr(self, *kids):
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
        self.val = qlast.Path(steps=[qlast.ClassRef(name=kids[0].val)])


class ByExprList(ListNonterm, element=ByExpr, separator=tokens.T_COMMA):
    pass


class SimpleFor(Nonterm):
    def reduce_For(self, *kids):
        r"%reduce FOR Identifier IN Set \
                  UNION OptionallyAliasedExpr \
                  OptFilterClause OptSortClause OptSelectLimit"
        self.val = qlast.ForQuery(
            iterator_alias=kids[1].val,
            iterator=kids[3].val,
            result=kids[5].val.expr,
            result_alias=kids[5].val.alias,
            where=kids[6].val,
            orderby=kids[7].val,
            offset=kids[8].val[0],
            limit=kids[8].val[1],
        )


class SimpleSelect(Nonterm):
    def reduce_Select(self, *kids):
        r"%reduce SELECT OptionallyAliasedExpr \
                  OptFilterClause OptSortClause OptSelectLimit"
        self.val = qlast.SelectQuery(
            result=kids[1].val.expr,
            result_alias=kids[1].val.alias,
            where=kids[2].val,
            orderby=kids[3].val,
            offset=kids[4].val[0],
            limit=kids[4].val[1],
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
            concept = subj.expr
            shape = subj.elements
        else:
            concept = subj
            shape = []

        if not isinstance(concept, qlast.Path):
            raise EdgeQLSyntaxError(
                "insert expression must be a concept or a view",
                context=subj.context)

        self.val = qlast.InsertQuery(
            subject=concept,
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
        "%reduce DELETE OptionallyAliasedExpr"
        self.val = qlast.DeleteQuery(
            subject=kids[1].val.expr,
            subject_alias=kids[1].val.alias,
        )


class OptAliasBlock(Nonterm):
    def reduce_AliasBlock(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = WithBlock(aliases=[], cardinality=None)


WithBlock = collections.namedtuple(
    'WithBlock', ['aliases', 'cardinality'], module=__name__)


class AliasBlock(Nonterm):
    def reduce_WITH_AliasDeclList(self, *kids):
        aliases = []
        cardinality = None
        for w in kids[1].val:
            if isinstance(w, str):
                cardinality = w
            else:
                aliases.append(w)
        self.val = WithBlock(aliases=aliases, cardinality=cardinality)


class AliasDecl(Nonterm):
    def reduce_MODULE_ModuleName(self, *kids):
        self.val = qlast.NamespaceAliasDecl(
            namespace='.'.join(kids[1].val))

    def reduce_CARDINALITY_SCONST(self, *kids):
        assert kids[1].val in {'1', '*'}
        self.val = kids[1].val

    def reduce_Identifier_TURNSTILE_MODULE_ModuleName(self, *kids):
        self.val = qlast.NamespaceAliasDecl(
            alias=kids[0].val,
            namespace='.'.join(kids[3].val))

    def reduce_AliasedExpr(self, *kids):
        self.val = kids[0].val


class AliasDeclList(ListNonterm, element=AliasDecl,
                    separator=tokens.T_COMMA):
    pass


class Shape(Nonterm):
    def reduce_LBRACE_ShapeElementList_RBRACE(self, *kids):
        self.val = kids[1].val

    def reduce_LBRACE_ShapeElementList_COMMA_RBRACE(self, *kids):
        self.val = kids[1].val


class TypedShape(Nonterm):
    def reduce_NodeName_Shape(self, *kids):
        self.val = qlast.Shape(
            expr=qlast.Path(
                steps=[qlast.ClassRef(
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
        self.val = [shape.expr.steps[0]] + shape.elements

    def reduce_COLON_TypeName(self, *kids):
        # this kind of shape definition appears in function signatures
        self.val = [kids[1].val]

    def reduce_empty(self, *kids):
        self.val = None


class ShapeElement(Nonterm):
    def reduce_ShapeElementWithSubShape(self, *kids):
        r"""%reduce ShapePointer \
             OptAnySubShape OptFilterClause OptSortClause OptSelectLimit \
        """
        self.val = kids[0].val

        shape = kids[1].val

        if shape and isinstance(shape[0], qlast.ClassRef):
            self.val.expr.steps[-1].target = shape[0]
            self.val.elements = shape[1:]
        elif shape and isinstance(shape[0], qlast.TypeName):
            self.val.expr.steps[-1].target = shape[0]
        else:
            self.val.elements = shape or []

        self.val.where = kids[2].val
        self.val.orderby = kids[3].val
        self.val.offset = kids[4].val[0]
        self.val.limit = kids[4].val[1]

    def reduce_ShapePointer_TURNSTILE_Expr(self, *kids):
        self.val = kids[0].val
        expr = kids[2].val
        if isinstance(expr, qlast.Shape):
            compexpr = expr.expr
            subshape = expr.elements
        else:
            compexpr = expr
            subshape = []
        self.val.compexpr = compexpr
        self.val.elements = subshape


class ShapeElementList(ListNonterm, element=ShapeElement,
                       separator=tokens.T_COMMA):
    pass


class ShapePath(Nonterm):
    # A form of Path appearing as an element in shapes.
    #
    # one-of:
    #   link
    #   <link
    #   >link
    #   @prop
    #   Concept.link
    #   Concept.>link
    #   Concept.<link
    #   Link@prop

    def reduce_ShapePathPtr(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers

        self.val = qlast.Path(
            steps=[
                qlast.Ptr(
                    ptr=kids[0].val,
                    direction=s_pointers.PointerDirection.Outbound
                )
            ]
        )

    def reduce_LinkDirection_ShapePathPtr(self, *kids):
        self.val = qlast.Path(
            steps=[
                qlast.Ptr(
                    ptr=kids[1].val,
                    direction=kids[0].val
                )
            ]
        )

    def reduce_AT_ShapePathPtr(self, *kids):
        self.val = qlast.Path(
            steps=[
                qlast.Ptr(
                    ptr=kids[1].val,
                    type='property'
                )
            ]
        )

    def reduce_ShapePathPtr_AT_ShapePathPtr(self, *kids):
        self.val = qlast.Path(
            steps=[
                kids[0].val,
                qlast.Ptr(
                    ptr=kids[2].val,
                    type='property'
                )
            ]
        )

    def reduce_ShapePathPtr_DOT_ShapePathPtr(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers

        self.val = qlast.Path(
            steps=[
                kids[0].val,
                qlast.Ptr(
                    ptr=kids[2].val,
                    direction=s_pointers.PointerDirection.Outbound
                )
            ]
        )

    def reduce_ShapePathPtr_DOTFW_ShapePathPtr(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers

        self.val = qlast.Path(
            steps=[
                kids[0].val,
                qlast.Ptr(
                    ptr=kids[2].val,
                    direction=s_pointers.PointerDirection.Outbound
                )
            ]
        )

    def reduce_ShapePathPtr_DOTBW_ShapePathPtr(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers

        self.val = qlast.Path(
            steps=[
                kids[0].val,
                qlast.Ptr(
                    ptr=kids[2].val,
                    direction=s_pointers.PointerDirection.Inbound
                )
            ]
        )


class ShapePathPtr(Nonterm):
    def reduce_NodeName(self, *kids):
        self.val = qlast.ClassRef(name=kids[0].val.name,
                                  module=kids[0].val.module)

    def reduce_LPAREN_ShapePathPtr_RPAREN(self, *kids):
        self.val = kids[1].val


class ShapePointer(Nonterm):
    def reduce_ShapePath(self, *kids):
        self.val = qlast.ShapeElement(
            expr=kids[0].val
        )


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

    def reduce_LPAREN_SetStmt_RPAREN(self, *kids):
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
    # | Expr IS Expr | Expr IS NOT Expr
    # | Expr IN Expr | Expr NOT IN Expr
    # | Expr '[' Expr ']'
    # | Expr '[' Expr ':' Expr ']'
    # | Expr '[' ':' Expr ']'
    # | Expr '[' Expr ':' ']'
    # | Expr '[' IS NodeName ']'
    # | '<' TypeName '>' Expr
    # | Expr IF Expr ELSE Expr
    # | Expr ?? Expr
    # | Expr UNION Expr | Expr UNION ALL Expr
    # | DISTINCT Expr
    # | 'self' | '__subject__'

    def reduce_Path(self, *kids):
        self.val = kids[0].val

    def reduce_Expr_Shape(self, *kids):
        self.val = qlast.Shape(expr=kids[0].val, elements=kids[1].val)

    def reduce_Constant(self, *kids):
        self.val = kids[0].val

    def reduce_SELF(self, *kids):
        self.val = qlast.Path(steps=[qlast.Self()])

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

    def reduce_Expr_LBRACKET_IS_NodeName_RBRACKET(self, *kids):
        # The path filter rule is here to resolve ambiguity with
        # indexes and slices, so Expr needs to be enforced as a path.
        #
        # NOTE: We specifically disallow "Foo.(bar[IS Baz])"" because
        # it is incorrect logical grouping. The example where the
        # incorrect grouping is more obvious is: "Foo.<(bar[IS Baz])"
        path = kids[0].val

        if (isinstance(path, qlast.Path) and
                isinstance(path.steps[-1], qlast.Ptr)):
            # filtering a longer path
            path.steps[-1].target = kids[3].val
            self.val = path

        else:
            # any other expression is a path with a filter
            self.val = qlast.Path(
                steps=[qlast.TypeFilter(
                    expr=path,
                    type=qlast.TypeName(maintype=kids[3].val))
                ])

    def reduce_FuncExpr(self, *kids):
        self.val = kids[0].val

    @parsing.precedence(precedence.P_UMINUS)
    def reduce_EXISTS_Expr(self, *kids):
        self.val = qlast.ExistsPredicate(expr=kids[1].val)

    def reduce_Tuple(self, *kids):
        self.val = kids[0].val

    def reduce_Collection(self, *kids):
        self.val = kids[0].val

    def reduce_Set(self, *kids):
        self.val = kids[0].val

    def reduce_NamedTuple(self, *kids):
        self.val = kids[0].val

    @parsing.precedence(precedence.P_UMINUS)
    def reduce_PLUS_Expr(self, *kids):
        self.val = qlast.UnaryOp(op=ast.ops.UPLUS, operand=kids[1].val)

    @parsing.precedence(precedence.P_UMINUS)
    def reduce_MINUS_Expr(self, *kids):
        self.val = qlast.UnaryOp(op=ast.ops.UMINUS, operand=kids[1].val)

    def reduce_Expr_PLUS_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.ADD,
                               right=kids[2].val)

    def reduce_Expr_MINUS_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.SUB,
                               right=kids[2].val)

    def reduce_Expr_STAR_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.MUL,
                               right=kids[2].val)

    def reduce_Expr_SLASH_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.DIV,
                               right=kids[2].val)

    def reduce_Expr_PERCENT_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.MOD,
                               right=kids[2].val)

    def reduce_Expr_CIRCUMFLEX_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.POW,
                               right=kids[2].val)

    def reduce_Expr_LANGBRACKET_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.LT,
                               right=kids[2].val)

    def reduce_Expr_RANGBRACKET_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.GT,
                               right=kids[2].val)

    @parsing.precedence(precedence.P_DOUBLEQMARK_OP)
    def reduce_Expr_DOUBLEQMARK_Expr(self, *kids):
        left = kids[0].val
        right = kids[2].val

        args = [left]
        if isinstance(right, qlast.Coalesce):
            args += right.args
        else:
            args.append(right)

        self.val = qlast.Coalesce(args=args)

    def reduce_Expr_EQUALS_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.EQ,
                               right=kids[2].val)

    @parsing.precedence(precedence.P_OP)
    def reduce_Expr_OP_Expr(self, *kids):
        op = kids[1].val
        if op == '!=':
            op = ast.ops.NE
        elif op == '=':
            op = ast.ops.EQ
        elif op == '>=':
            op = ast.ops.GE
        elif op == '<=':
            op = ast.ops.LE
        elif op == '?=':
            op = qlast.EQUIVALENT
        elif op == '?!=':
            op = qlast.NEQUIVALENT

        self.val = qlast.BinOp(left=kids[0].val, op=op, right=kids[2].val)

    def reduce_Expr_AND_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.AND,
                               right=kids[2].val)

    def reduce_Expr_OR_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.OR,
                               right=kids[2].val)

    def reduce_NOT_Expr(self, *kids):
        self.val = qlast.UnaryOp(op=ast.ops.NOT, operand=kids[1].val)

    def reduce_Expr_LIKE_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=qlast.LIKE,
                               right=kids[2].val)

    def reduce_Expr_NOT_LIKE_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=qlast.NOT_LIKE,
                               right=kids[3].val)

    def reduce_Expr_ILIKE_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=qlast.ILIKE,
                               right=kids[2].val)

    def reduce_Expr_NOT_ILIKE_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=qlast.NOT_ILIKE,
                               right=kids[3].val)

    def reduce_Expr_IS_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.IS,
                               right=kids[2].val)

    @parsing.precedence(precedence.P_IS)
    def reduce_Expr_IS_NOT_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.IS_NOT,
                               right=kids[3].val)

    def reduce_Expr_IN_Expr(self, *kids):
        inexpr = kids[2].val
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.IN,
                               right=inexpr)

    @parsing.precedence(precedence.P_IN)
    def reduce_Expr_NOT_IN_Expr(self, *kids):
        inexpr = kids[3].val
        self.val = qlast.BinOp(left=kids[0].val, op=ast.ops.NOT_IN,
                               right=inexpr)

    @parsing.precedence(precedence.P_TYPECAST)
    def reduce_LANGBRACKET_TypeName_RANGBRACKET_Expr(
            self, *kids):
        self.val = qlast.TypeCast(expr=kids[3].val, type=kids[1].val)

    def reduce_Expr_IF_Expr_ELSE_Expr(self, *kids):
        self.val = qlast.IfElse(
            if_expr=kids[0].val, condition=kids[2].val, else_expr=kids[4].val)

    def reduce_DISTINCT_Expr(self, *kids):
        self.val = qlast.UnaryOp(op=qlast.DISTINCT, operand=kids[1].val)

    def reduce_Expr_UNION_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=qlast.UNION,
                               right=kids[2].val)

    def reduce_Expr_DISTINCTUNION_Expr(self, *kids):
        self.val = qlast.BinOp(left=kids[0].val, op=qlast.DISTINCT_UNION,
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
    def reduce_ShortNodeName_TURNSTILE_Expr(self, *kids):
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
    def reduce_LBRACKET_OptCollectionItemList_RBRACKET(self, *kids):
        items = kids[1].val
        if not items:
            self.val = qlast.EmptyCollection()
            return

        typ = items[0][0]

        if typ == 'array':
            elements = []
            for item in items:
                if item[0] != typ:
                    raise EdgeQLSyntaxError("unexpected map item in array",
                                            context=item[1].context)
                elements.append(item[1].val)

            self.val = qlast.Array(elements=elements)
        else:
            keys = []
            values = []
            for item in items:
                if item[0] != typ:
                    raise EdgeQLSyntaxError("unexpected array item in map",
                                            context=item[1].context)
                keys.append(item[1].val)
                values.append(item[2].val)

            self.val = qlast.Mapping(keys=keys, values=values)


class CollectionItem(Nonterm):
    def reduce_Expr(self, *kids):
        self.val = ('array', kids[0])

    def reduce_Expr_ARROW_Expr(self, *kids):
        self.val = ('map', kids[0], kids[2])


class CollectionItemList(ListNonterm,
                         element=CollectionItem,
                         separator=tokens.T_COMMA):
    pass


class OptCollectionItemList(Nonterm):
    def reduce_CollectionItemList_COMMA(self, *kids):
        self.val = kids[0].val

    def reduce_CollectionItemList(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


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

    def reduce_ArgConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseNumberConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseStringConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseBooleanConstant(self, *kids):
        self.val = kids[0].val


class ArgConstant(Nonterm):
    def reduce_DOLLAR_ICONST(self, *kids):
        self.val = qlast.Parameter(name=str(kids[1].val))

    def reduce_DOLLAR_Identifier(self, *kids):
        self.val = qlast.Parameter(name=kids[1].val)


class BaseNumberConstant(Nonterm):
    def reduce_ICONST(self, *kids):
        self.val = qlast.Constant(value=int(kids[0].val))

    def reduce_FCONST(self, *kids):
        self.val = qlast.Constant(value=float(kids[0].val))


class BaseStringConstant(Nonterm):
    def reduce_SCONST(self, *kids):
        self.val = qlast.Constant(value=str(kids[0].val))


class BaseBooleanConstant(Nonterm):
    def reduce_TRUE(self, *kids):
        self.val = qlast.Constant(value=True)

    def reduce_FALSE(self, *kids):
        self.val = qlast.Constant(value=False)


class Path(Nonterm):
    @parsing.precedence(precedence.P_PATHSTART)
    def reduce_NodeName(self, *kids):
        self.val = qlast.Path(
            steps=[qlast.ClassRef(name=kids[0].val.name,
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

    def _float_to_path(self, token, context):
        from edgedb.lang.schema import pointers as s_pointers

        # make sure that the float is of the type 0.1
        parts = token.val.split('.')
        if not (len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit()):
            raise EdgeQLSyntaxError(
                'Unexpected token: {!r}'.format(token),
                context=token.context)

        # context for the AST is established manually here
        return [
            qlast.Ptr(
                ptr=qlast.ClassRef(
                    name=parts[0],
                    context=token.context,
                ),
                direction=s_pointers.PointerDirection.Outbound,
                context=context,
            ),
            qlast.Ptr(
                ptr=qlast.ClassRef(
                    name=parts[1],
                    context=token.context,
                ),
                direction=s_pointers.PointerDirection.Outbound,
                context=token.context,
            )
        ]


class PathStep(Nonterm):
    def reduce_DOT_PathPtr(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            ptr=kids[1].val,
            direction=s_pointers.PointerDirection.Outbound
        )

    def reduce_DOT_ICONST(self, *kids):
        # this is a valid link-like syntax for accessing unnamed tuples
        from edgedb.lang.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            ptr=qlast.ClassRef(name=kids[1].val),
            direction=s_pointers.PointerDirection.Outbound
        )

    def reduce_DOTFW_PathPtr(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            ptr=kids[1].val,
            direction=s_pointers.PointerDirection.Outbound
        )

    def reduce_DOTBW_PathPtr(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            ptr=kids[1].val,
            direction=s_pointers.PointerDirection.Inbound
        )

    def reduce_AT_PathPtr(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers

        self.val = qlast.Ptr(
            ptr=kids[1].val,
            direction=s_pointers.PointerDirection.Outbound,
            type='property'
        )


class PathPtr(Nonterm):
    def reduce_ShortNodeName(self, *kids):
        self.val = kids[0].val

    def reduce_PathPtrParen(self, *kids):
        self.val = kids[0].val


class PathPtrParen(Nonterm):
    def reduce_LPAREN_PathPtrParen_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_NodeName_RPAREN(self, *kids):
        self.val = qlast.ClassRef(name=kids[1].val.name,
                                  module=kids[1].val.module)


class LinkDirection(Nonterm):
    def reduce_LANGBRACKET(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers
        self.val = s_pointers.PointerDirection.Inbound

    def reduce_RANGBRACKET(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers
        self.val = s_pointers.PointerDirection.Outbound


class FuncApplication(Nonterm):
    def reduce_NodeName_LPAREN_OptFuncArgList_RPAREN(self, *kids):
        module = kids[0].val.module
        func_name = kids[0].val.name
        name = func_name if not module else (module, func_name)
        self.val = qlast.FunctionCall(func=name,
                                      args=kids[2].val)


class FuncExpr(Nonterm):
    def reduce_FuncApplication(self, *kids):
        self.val = kids[0].val


class FuncArgExpr(Nonterm):
    def reduce_Expr(self, *kids):
        self.val = qlast.FuncArg(arg=kids[0].val)

    def reduce_Identifier_TURNSTILE_Expr(self, *kids):
        self.val = qlast.FuncArg(name=kids[0].val, arg=kids[2].val)


class FuncArg(Nonterm):
    def reduce_FuncArgExpr_OptFilterClause_OptSortClause(self, *kids):
        self.val = kids[0].val
        self.val.filter = kids[1].val
        self.val.sort = kids[2].val


class FuncArgList(ListNonterm, element=FuncArg, separator=tokens.T_COMMA):
    pass


class OptFuncArgList(Nonterm):
    def reduce_FuncArgList(self, *kids):
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
        # the identifier following a '::' cannot start with '@'
        if kids[2].val[0] == '@':
            raise EdgeQLSyntaxError("name cannot start with '@'",
                                    context=kids[2].context)

        self.val = [kids[0].val, kids[2].val]


class NonArrayTypeName(Nonterm):
    def reduce_NodeName(self, *kids):
        maintype = kids[0].val

        # maintype cannot be 'map' or 'array'
        if maintype.module is None:
            if maintype.name in ('array', 'map'):
                raise EdgeQLSyntaxError(
                    'Unexpected token: {!r}'.format(maintype.name),
                    context=kids[0].context)

        self.val = qlast.TypeName(maintype=maintype)

    def reduce_MAP_LANGBRACKET_TypeName_COMMA_TypeName_RANGBRACKET(self,
                                                                   *kids):
        self.val = qlast.TypeName(
            maintype=qlast.ClassRef(name='map'),
            subtypes=[kids[2].val, kids[4].val],
        )

    def reduce_TUPLE_LANGBRACKET_TypeNameList_RANGBRACKET(self, *kids):
        self.val = qlast.TypeName(
            maintype=qlast.ClassRef(name='tuple'),
            subtypes=kids[2].val,
        )

    def reduce_TUPLE_LANGBRACKET_NamedTupleTypeList_RANGBRACKET(self, *kids):
        self.val = qlast.TypeName(
            maintype=qlast.ClassRef(name='tuple'),
            subtypes=kids[2].val,
        )


class TypeName(Nonterm):
    def reduce_NonArrayTypeName(self, *kids):
        self.val = kids[0].val

    def reduce_ARRAY_LANGBRACKET_NonArrayTypeName_OptDimensions_RANGBRACKET(
            self, *kids):
        subtype = kids[2].val
        dimensions = kids[3].val

        self.val = qlast.TypeName(
            maintype=qlast.ClassRef(name='array'),
            subtypes=[subtype],
            dimensions=dimensions,
        )


class TypeNameList(ListNonterm, element=TypeName, separator=tokens.T_COMMA):
    pass


class NamedTupleType(Nonterm):
    def reduce_Identifier_COLON_TypeName(self, *kids):
        self.val = kids[2].val
        self.val.name = kids[0].val


class NamedTupleTypeList(ListNonterm, element=NamedTupleType,
                         separator=tokens.T_COMMA):
    pass


class Dimension(Nonterm):
    def reduce_LBRACKET_RBRACKET(self, *kids):
        # special value, impossible to get through any other production
        self.val = -1

    def reduce_LBRACKET_ICONST_RBRACKET(self, *kids):
        self.val = int(kids[1].val)


class Dimensions(ListNonterm, element=Dimension):
    pass


class OptDimensions(Nonterm):
    def reduce_Dimensions(self, *kids):
        self.val = [None if v == -1 else v for v in kids[0].val]

    def reduce_empty(self):
        self.val = None


class NodeName(Nonterm):
    # NOTE: Generic short of fully-qualified name.
    #
    # This name is safe to be used anywhere as it starts with IDENT only.

    def reduce_BaseName(self, *kids):
        # NodeName must not start with a '@' in any way
        if kids[0].val[-1][0] == '@':
            raise EdgeQLSyntaxError("name cannot start with '@'",
                                    context=kids[0].context)
        self.val = qlast.ClassRef(
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
        # ShortNodeName cannot start with a '@' in any way
        if kids[0].val[0] == '@':
            raise EdgeQLSyntaxError("name cannot start with '@'",
                                    context=kids[0].context)
        self.val = qlast.ClassRef(
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
        # AnyNodeName cannot start with a '@' in any way
        if kids[0].val[0] == '@':
            raise EdgeQLSyntaxError("name cannot start with '@'",
                                    context=kids[0].context)
        self.val = qlast.ClassRef(
            module=None,
            name=kids[0].val)


class KeywordMeta(parsing.NontermMeta):
    def __new__(mcls, name, bases, dct, *, type):
        result = super().__new__(mcls, name, bases, dct)

        assert type in keywords.keyword_types

        for val, token in keywords.by_type[type].items():
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
