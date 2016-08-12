##
# Copyright (c) 2008-2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast
from edgedb.lang.common import parsing, context

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.ir import ast as irast

from ..errors import EdgeQLSyntaxError

from . import keywords

from .precedence import *
from .tokens import *


class Nonterm(context.Nonterm):
    pass


class SelectExpr(Nonterm):
    @parsing.precedence(P_UMINUS)
    def reduce_SelectNoParens(self, *kids):
        self.val = kids[0].val

    @parsing.precedence(P_UMINUS)
    def reduce_SelectWithParens(self, *kids):
        self.val = kids[0].val


class SelectWithParens(Nonterm):
    def reduce_LPAREN_SelectNoParens_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_SelectWithParens_RPAREN(self, *kids):
        self.val = kids[1].val


class SelectNoParens(Nonterm):
    def reduce_AliasBlock_SelectClause_OptSortClause_OptSelectLimit(
            self, *kids):
        qry = kids[1].val
        qry.orderby = kids[2].val
        qry.offset = kids[3].val[0]
        qry.limit = kids[3].val[1]
        (qry.namespaces, qry.aliases) = kids[0].val

        self.val = qry

    def reduce_AliasBlock_WithClause_SelectClause_OptSortClause_OptSelectLimit(
            self, *kids):
        qry = kids[2].val
        qry.orderby = kids[3].val
        qry.offset = kids[4].val[0]
        qry.limit = kids[4].val[1]
        (qry.namespaces, qry.aliases) = kids[0].val
        qry.cges = kids[1].val

        self.val = qry

    def reduce_WithClause_SelectClause_OptSortClause_OptSelectLimit(
            self, *kids):
        qry = kids[1].val
        qry.orderby = kids[2].val
        qry.offset = kids[3].val[0]
        qry.limit = kids[3].val[1]
        qry.cges = kids[0].val

        self.val = qry

    def reduce_SimpleSelect_OptSelectLimit(self, *kids):
        qry = kids[0].val
        qry.offset = kids[1].val[0]
        qry.limit = kids[1].val[1]
        self.val = qry

    def reduce_SelectClause_SortClause_OptSelectLimit(self, *kids):
        qry = kids[0].val
        qry.orderby = kids[1].val
        qry.offset = kids[2].val[0]
        qry.limit = kids[2].val[1]

        self.val = qry


class WithClause(Nonterm):
    def reduce_WITH_CgeList(self, *kids):
        self.val = kids[1].val


class CgeList(Nonterm):
    def reduce_Cge(self, *kids):
        self.val = [kids[0].val]

    def reduce_CgeList_COMMA_Cge(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class Cge(Nonterm):
    def reduce_AliasName_AS_LPAREN_SelectExpr_RPAREN(self, *kids):
        self.val = qlast.CGENode(expr=kids[3].val, alias=kids[0].val)


class SelectClause(Nonterm):
    def reduce_SimpleSelect(self, *kids):
        self.val = kids[0].val

    def reduce_SelectWithParens(self, *kids):
        self.val = kids[0].val


class SimpleSelect(Nonterm):
    def reduce_Select(self, *kids):
        r"%reduce SELECT OptDistinct SelectTargetList \
                  OptWhereClause OptGroupClause"
        self.val = qlast.SelectQueryNode(
            distinct=kids[1].val,
            targets=kids[2].val,
            where=kids[3].val,
            groupby=kids[4].val
        )

    def reduce_SelectClause_UNION_OptAll_SelectClause(self, *kids):
        self.val = qlast.SelectQueryNode(
            op=qlast.UNION,
            op_larg=kids[0].val,
            op_rarg=kids[3].val
        )

    def reduce_SelectClause_INTERSECT_OptAll_SelectClause(self, *kids):
        self.val = qlast.SelectQueryNode(
            op=qlast.INTERSECT,
            op_larg=kids[0].val,
            op_rarg=kids[3].val
        )

    def reduce_SelectClause_EXCEPT_OptAll_SelectClause(self, *kids):
        self.val = qlast.SelectQueryNode(
            op=qlast.EXCEPT,
            op_larg=kids[0].val,
            op_rarg=kids[3].val
        )


class OptAliasBlock(Nonterm):
    def reduce_AliasBlock(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = ([], [])


class AliasBlock(Nonterm):
    def reduce_USING_AliasDeclList(self, *kids):
        nsaliases = []
        expraliases = []

        for alias in kids[1].val:
            if isinstance(alias, qlast.NamespaceAliasDeclNode):
                nsaliases.append(alias)
            else:
                expraliases.append(alias)

        self.val = (nsaliases, expraliases)


class AliasDeclList(Nonterm):
    def reduce_AliasDecl(self, *kids):
        self.val = [kids[0].val]

    def reduce_AliasDeclList_COMMA_AliasDecl(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class AliasDecl(Nonterm):
    def reduce_NAMESPACE_FqName(self, *kids):
        self.val = qlast.NamespaceAliasDeclNode(
            namespace='.'.join(kids[1].val))

    def reduce_AliasName_COLONEQUALS_NAMESPACE_FqName(self, *kids):
        self.val = qlast.NamespaceAliasDeclNode(
            alias=kids[0].val,
            namespace='.'.join(kids[3].val))

    def reduce_AliasName_COLONEQUALS_Expr(self, *kids):
        self.val = qlast.ExpressionAliasDeclNode(
            alias=kids[0].val,
            expr=kids[2].val)


class OptDistinct(Nonterm):
    def reduce_DISTINCT(self, *kids):
        self.val = True

    def reduce_empty(self, *kids):
        self.val = False


class SelectTargetList(Nonterm):
    def reduce_SelectTargetEl(self, *kids):
        self.val = [kids[0].val]

    def reduce_SelectTargetList_COMMA_SelectTargetEl(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class SelectTargetEl(Nonterm):
    def reduce_Expr_AS_LabelExpr(self, *kids):
        self.val = qlast.SelectExprNode(expr=kids[0].val, alias=kids[2].val)

    def reduce_Expr(self, *kids):
        self.val = qlast.SelectExprNode(expr=kids[0].val)

    def reduce_Path_SelectPathSpec(self, *kids):
        kids[0].val.pathspec = kids[1].val
        self.val = qlast.SelectExprNode(expr=kids[0].val)


class SelectPathSpec(Nonterm):
    def reduce_LBRACE_SelectPointerSpecList_RBRACE(self, *kids):
        self.val = kids[1].val


class OptSelectPathSpec(Nonterm):
    def reduce_SelectPathSpec(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = None


class SelectPointerSpecList(Nonterm):
    def reduce_SelectPointerSpec(self, *kids):
        self.val = [kids[0].val]

    def reduce_SelectPointerSpecList_COMMA_SelectPointerSpec(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class OptSelectPathCompExpr(Nonterm):
    def reduce_COLONEQUALS_Expr(self, *kids):
        self.val = kids[1].val

    def reduce_empty(self, *kids):
        self.val = None


class SelectPointerSpec(Nonterm):
    def reduce_PointerGlob(self, *kids):
        self.val = kids[0].val

    def reduce_AT_AnyFqLinkPropName(self, *kids):
        self.val = qlast.SelectPathSpecNode(
            expr=qlast.LinkExprNode(expr=kids[1].val)
        )

    def reduce_PointerSpecSetExpr_OptPointerRecursionSpec_OptSelectPathSpec_OptSelectPathCompExpr(
            self, *kids):
        self.val = kids[0].val
        self.val.recurse = kids[1].val
        self.val.pathspec = kids[2].val
        self.val.compexpr = kids[3].val


class PointerSpecSetExpr(Nonterm):
    def reduce_ParenthesizedTransformedLinkedSetExpr(self, *kids):
        self.val = kids[0].val

    def reduce_SimpleFqLinkExpr(self, *kids):
        self.val = qlast.SelectPathSpecNode(
            expr=qlast.LinkExprNode(expr=kids[0].val)
        )

    def reduce_TYPEINDIRECTION_PathStepList(self, *kids):
        self.val = qlast.SelectTypeRefNode(
            attrs=kids[1].val
        )


class QualifiedLinkedSetExpr(Nonterm):
    def reduce_SimpleFqLinkExpr(self, *kids):
        self.val = qlast.SelectPathSpecNode(
            expr=qlast.LinkExprNode(expr=kids[0].val)
        )

    def reduce_SimpleFqLinkExpr_WhereClause(self, *kids):
        self.val = qlast.SelectPathSpecNode(
            expr=qlast.LinkExprNode(expr=kids[0].val),
            where=kids[1].val
        )


class TransformedLinkedSetExpr(Nonterm):
    def reduce_QualifiedLinkedSetExpr_OptSortClause_OptSelectLimit(
            self, *kids):
        self.val = kids[0].val
        self.val.orderby = kids[1].val
        self.val.offset = kids[2].val[0]
        self.val.limit = kids[2].val[1]


class ParenthesizedTransformedLinkedSetExpr(Nonterm):
    def reduce_LPAREN_TransformedLinkedSetExpr_RPAREN(self, *kids):
        self.val = kids[1].val


class OptPointerRecursionSpec(Nonterm):
    def reduce_STAR(self, *kids):
        self.val = qlast.ConstantNode(value=0)

    def reduce_STAR_NumberConstant(self, *kids):
        self.val = kids[1].val

    def reduce_empty(self, *kids):
        self.val = None


class PointerGlob(Nonterm):
    def reduce_STAR(self, *kids):
        flt = qlast.PointerGlobFilter(property='loading', value='eager')
        self.val = qlast.PointerGlobNode(filters=[flt], type='link')

    def reduce_STAR_LPAREN_PointerGlobFilterList_RPAREN(self, *kids):
        self.val = qlast.PointerGlobNode(filters=kids[2].val, type='link')

    def reduce_AT_STAR(self, *kids):
        flt = qlast.PointerGlobFilter(property='loading', value='eager')
        self.val = qlast.PointerGlobNode(filters=[flt], type='property')

    def reduce_AT_STAR_LPAREN_PointerGlobFilterList_RPAREN(self, *kids):
        self.val = qlast.PointerGlobNode(filters=kids[2].val, type='property')


class PointerGlobFilterList(Nonterm):
    def reduce_PointerGlobFilter(self, *kids):
        self.val = [kids[0].val]

    def reduce_PointerGlobFilterList_COMMA_PointerGlobFilter(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class PointerGlobFilter(Nonterm):
    def reduce_LabelExpr_EQUALS_LabelExpr(self, *kids):
        self.val = qlast.PointerGlobFilter(property=kids[0].val,
                                           value=kids[2].val)

    def reduce_ANY_LabelExpr(self, *kids):
        self.val = qlast.PointerGlobFilter(property=kids[1].val, any=True)


class WhereClause(Nonterm):
    def reduce_WHERE_Expr(self, *kids):
        self.val = kids[1].val


class OptWhereClause(Nonterm):
    def reduce_WhereClause(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = None


class OptGroupClause(Nonterm):
    def reduce_GROUP_BY_ExprList(self, *kids):
        self.val = kids[2].val

    def reduce_empty(self, *kids):
        self.val = None


class SortClause(Nonterm):
    def reduce_ORDER_BY_OrderbyList(self, *kids):
        self.val = kids[2].val


class OptSortClause(Nonterm):
    def reduce_SortClause(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = None


class OrderbyList(Nonterm):
    def reduce_OrderbyExpr(self, *kids):
        self.val = [kids[0].val]

    def reduce_OrderbyList_COMMA_OrderbyExpr(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class OrderbyExpr(Nonterm):
    def reduce_Expr_OptDirection_OptNonesOrder(self, *kids):
        self.val = qlast.SortExprNode(path=kids[0].val,
                                      direction=kids[1].val,
                                      nones_order=kids[2].val)


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
    def reduce_OFFSET_NumberConstant(self, *kids):
        self.val = kids[1].val


class LimitClause(Nonterm):
    def reduce_LIMIT_NumberConstant(self, *kids):
        self.val = kids[1].val


class OptDirection(Nonterm):
    def reduce_ASC(self, *kids):
        self.val = qlast.SortAsc

    def reduce_DESC(self, *kids):
        self.val = qlast.SortDesc

    def reduce_empty(self, *kids):
        self.val = qlast.SortDefault


class OptNonesOrder(Nonterm):
    def reduce_NONES_FIRST(self, *kids):
        self.val = qlast.NonesFirst

    def reduce_NONES_LAST(self, *kids):
        self.val = qlast.NonesLast

    def reduce_empty(self, *kids):
        self.val = None


class OptAll(Nonterm):
    def reduce_ALL(self, *kids):
        self.val = True

    def reduce_empty(self, *kids):
        self.val = None


class IndirectionEl(Nonterm):
    def reduce_LBRACKET_Expr_RBRACKET(self, *kids):
        self.val = qlast.IndexNode(index=kids[1].val)

    def reduce_LBRACKET_Expr_COLON_Expr_RBRACKET(self, *kids):
        self.val = qlast.SliceNode(start=kids[1].val, stop=kids[3].val)

    def reduce_LBRACKET_Expr_COLON_RBRACKET(self, *kids):
        self.val = qlast.SliceNode(start=kids[1].val, stop=None)

    def reduce_LBRACKET_COLON_Expr_RBRACKET(self, *kids):
        self.val = qlast.SliceNode(start=None, stop=kids[2].val)


class OptIndirection(Nonterm):
    def reduce_OptIndirection_IndirectionEl(self, *kids):
        if kids[0].val is not None:
            self.val = kids[0].val + [kids[1].val]
        else:
            self.val = [kids[1].val]

    def reduce_empty(self, *kids):
        self.val = None


class Expr(Nonterm):
    # Path | Constant | '(' Expr ')' | FuncExpr | Sequence | Mapping
    # | '+' Expr | '-' Expr | Expr '+' Expr | Expr '-' Expr
    # | Expr '*' Expr | Expr '/' Expr | Expr '%' Expr
    # | Expr '^' Expr | Expr '<' Expr | Expr '>' Expr
    # | Expr '=' Expr
    # | Expr AND Expr | Expr OR Expr | NOT Expr
    # | Expr LIKE Expr | Expr NOT LIKE Expr
    # | Expr ILIKE Expr | Expr NOT ILIKE Expr
    # | Expr IS NONE | Expr IS NOT NONE
    # | Expr IS OF '(' NodeNameList ')'
    # | Expr IS NOT OF '(' NodeNameList ')'
    # | Expr IN InExpr
    # |

    def reduce_Path(self, *kids):
        self.val = kids[0].val

    def reduce_Constant(self, *kids):
        self.val = kids[0].val

    def reduce_LPAREN_Expr_RPAREN_OptIndirection(self, *kids):
        if kids[3].val:
            self.val = qlast.IndirectionNode(arg=kids[1].val,
                                             indirection=kids[3].val)
        else:
            self.val = kids[1].val

    def reduce_FuncExpr(self, *kids):
        self.val = kids[0].val

    @parsing.precedence(P_UMINUS)
    def reduce_SelectWithParens(self, *kids):
        self.val = kids[0].val

    def reduce_EXISTS_SelectWithParens(self, *kids):
        self.val = qlast.ExistsPredicateNode(expr=kids[1].val)

    def reduce_EXISTS_LPAREN_Expr_RPAREN(self, *kids):
        self.val = qlast.ExistsPredicateNode(expr=kids[2].val)

    def reduce_Sequence(self, *kids):
        self.val = kids[0].val

    def reduce_Mapping(self, *kids):
        self.val = kids[0].val

    @parsing.precedence(P_UMINUS)
    def reduce_PLUS_Expr(self, *kids):
        self.val = qlast.UnaryOpNode(op=ast.ops.UPLUS, operand=kids[1].val)

    @parsing.precedence(P_UMINUS)
    def reduce_MINUS_Expr(self, *kids):
        self.val = qlast.UnaryOpNode(op=ast.ops.UMINUS, operand=kids[1].val)

    def reduce_Expr_PLUS_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.ADD,
                                   right=kids[2].val)

    def reduce_Expr_MINUS_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.SUB,
                                   right=kids[2].val)

    def reduce_Expr_STAR_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.MUL,
                                   right=kids[2].val)

    def reduce_Expr_SLASH_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.DIV,
                                   right=kids[2].val)

    def reduce_Expr_PERCENT_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.MOD,
                                   right=kids[2].val)

    def reduce_Expr_STARSTAR_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.POW,
                                   right=kids[2].val)

    def reduce_Expr_LANGBRACKET_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.LT,
                                   right=kids[2].val)

    def reduce_Expr_RANGBRACKET_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.GT,
                                   right=kids[2].val)

    def reduce_Expr_EQUALS_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.EQ,
                                   right=kids[2].val)

    @parsing.precedence(P_OP)
    def reduce_Expr_OP_Expr(self, *kids):
        op = kids[1].val
        if op == '!=':
            op = ast.ops.NE
        elif op == '==':
            op = ast.ops.EQ
        elif op == '>=':
            op = ast.ops.GE
        elif op == '<=':
            op = ast.ops.LE
        elif op == '@@':
            op = irast.SEARCH
        elif op == '@@!':
            op = irast.SEARCHEX
        elif op == '~':
            op = qlast.REMATCH
        elif op == '~*':
            op = qlast.REIMATCH

        self.val = qlast.BinOpNode(left=kids[0].val, op=op, right=kids[2].val)

    @parsing.precedence(P_OP)
    def reduce_OP_Expr(self, *kids):
        self.val = qlast.UnaryOpNode(op=kids[0].val, operand=kids[1].val)

    # @parsing.precedence(P_POSTFIXOP)
    # def reduce_Expr_OP(self, *kids):
    #     self.val = qlast.PostfixOpNode(op=kids[1].val, operand=kids[0].val)

    def reduce_Expr_AND_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.AND,
                                   right=kids[2].val)

    def reduce_Expr_OR_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.OR,
                                   right=kids[2].val)

    def reduce_NOT_Expr(self, *kids):
        self.val = qlast.UnaryOpNode(op=ast.ops.NOT, operand=kids[1].val)

    def reduce_Expr_LIKE_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=irast.LIKE,
                                   right=kids[2].val)

    def reduce_Expr_NOT_LIKE_Expr(self, *kids):
        val = qlast.BinOpNode(left=kids[0].val, op=irast.LIKE,
                              right=kids[2].val)
        self.val = qlast.UnaryOpNode(op=ast.ops.NOT, operand=val)

    def reduce_Expr_ILIKE_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=irast.ILIKE,
                                   right=kids[2].val)

    def reduce_Expr_NOT_ILIKE_Expr(self, *kids):
        val = qlast.BinOpNode(left=kids[0].val, op=irast.ILIKE,
                              right=kids[2].val)
        self.val = qlast.UnaryOpNode(op=ast.ops.NOT, operand=val)

    def reduce_Expr_IS_NONE(self, *kids):
        self.val = qlast.NoneTestNode(expr=kids[0].val)

    def reduce_Expr_IS_NOT_NONE(self, *kids):
        nt = qlast.NoneTestNode(expr=kids[0].val)
        self.val = qlast.UnaryOpNode(op=ast.ops.NOT, operand=nt)

    @parsing.precedence(P_IS)
    def reduce_Expr_IS_IsExpr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.IS,
                                   right=kids[2].val)

    @parsing.precedence(P_IS)
    def reduce_Expr_IS_NOT_IsExpr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.IS_NOT,
                                   right=kids[3].val)

    def reduce_Expr_IN_InExpr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.IN,
                                   right=kids[2].val)

    def reduce_Expr_NOT_IN_InExpr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.NOT_IN,
                                   right=kids[3].val)


class Sequence(Nonterm):
    def reduce_LPAREN_Expr_COMMA_OptExprList_RPAREN(self, *kids):
        self.val = qlast.SequenceNode(elements=[kids[1].val] + kids[3].val)


class Mapping(Nonterm):
    def reduce_LBRACE_MappingElementsList_RBRACE(self, *kids):
        self.val = qlast.MappingNode(items=kids[1].val)


class MappingElementsList(Nonterm):
    def reduce_MappingElement(self, *kids):
        self.val = [kids[0].val]

    def reduce_MappingElementsList_COMMA_MappingElement(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class MappingElement(Nonterm):
    def reduce_BaseStringConstant_COLON_Expr(self, *kids):
        self.val = (kids[0].val, kids[2].val)


class OptExprList(Nonterm):
    def reduce_ExprList(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


class ExprList(Nonterm):
    def reduce_Expr(self, *kids):
        self.val = [kids[0].val]

    def reduce_ExprList_COMMA_Expr(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class InExpr(Nonterm):
    def reduce_LPAREN_ExprList_RPAREN(self, *kids):
        self.val = qlast.SequenceNode(elements=kids[1].val)

    def reduce_Path(self, *kids):
        self.val = kids[0].val

    def reduce_ArgConstant(self, *kids):
        self.val = kids[0].val


class IsExpr(Nonterm):
    def reduce_LPAREN_FqNodeNameList_RPAREN(self, *kids):
        self.val = qlast.SequenceNode(elements=kids[1].val)

    def reduce_FqNodeName(self, *kids):
        self.val = kids[0].val

    def reduce_ArgConstant(self, *kids):
        self.val = kids[0].val


class Constant(Nonterm):
    # BaseConstant
    # | BaseNumberConstant
    # | BaseStringConstant
    # | BaseBooleanConstant

    def reduce_BaseConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseNumberConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseStringConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseBooleanConstant(self, *kids):
        self.val = kids[0].val


class BaseConstant(Nonterm):
    # NoneConstant
    # | ArgConstant

    def reduce_NoneConstant(self, *kids):
        self.val = kids[0].val

    def reduce_ArgConstant(self, *kids):
        self.val = kids[0].val


class NoneConstant(Nonterm):
    def reduce_NONE(self, *kids):
        self.val = qlast.ConstantNode(value=None)


class ArgConstant(Nonterm):
    def reduce_DOLLAR_ICONST(self, *kids):
        self.val = qlast.ConstantNode(value=None, index=int(kids[1].val))

    def reduce_DOLLAR_ArgName(self, *kids):
        self.val = qlast.ConstantNode(value=None, index=str(kids[1].val))


class BaseNumberConstant(Nonterm):
    def reduce_ICONST(self, *kids):
        self.val = qlast.ConstantNode(value=int(kids[0].val))

    def reduce_FCONST(self, *kids):
        self.val = qlast.ConstantNode(value=float(kids[0].val))


class NumberConstant(Nonterm):
    def reduce_BaseConstant(self, *kids):
        self.val = kids[0].val

    def reduce_BaseNumberConstant(self, *kids):
        self.val = kids[0].val


class BaseStringConstant(Nonterm):
    def reduce_SCONST(self, *kids):
        self.val = qlast.ConstantNode(value=str(kids[0].val))


class BaseBooleanConstant(Nonterm):
    def reduce_TRUE(self, *kids):
        self.val = qlast.ConstantNode(value=True)

    def reduce_FALSE(self, *kids):
        self.val = qlast.ConstantNode(value=False)


class Path(Nonterm):
    def reduce_PathSteps(self, *kids):
        self.val = qlast.PathNode(steps=kids[0].val)


class PathSteps(Nonterm):
    def reduce_PathStart_OptPathStepList(self, *kids):
        self.val = [kids[0].val]
        if kids[1].val:
            self.val += kids[1].val

    def reduce_FuncApplication_PathStepList(self, *kids):
        self.val = [kids[0].val] + kids[1].val


class PathStart(Nonterm):
    @parsing.precedence(P_PATHSTART)
    def reduce_NodeName(self, *kids):
        self.val = qlast.PathStepNode(expr=kids[0].val.name,
                                      namespace=kids[0].val.module)


class OptPathStepList(Nonterm):
    def reduce_PathStepList(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = None


class PathStepList(Nonterm):
    def reduce_PathStep(self, *kids):
        self.val = [kids[0].val]

    def reduce_PathStepList_PathStep(self, *kids):
        self.val = kids[0].val + [kids[1].val]


class PathStep(Nonterm):
    def reduce_PathStepSimple(self, *kids):
        self.val = kids[0].val


class PathStepSimple(Nonterm):
    def reduce_DOT_PathExprOrType(self, *kids):
        self.val = qlast.LinkExprNode(expr=kids[1].val)

    def reduce_AT_PathExpr(self, *kids):
        self.val = qlast.LinkPropExprNode(expr=kids[1].val)
        kids[1].val.type = 'property'


class PathExprOrType(Nonterm):
    def reduce_PathExpr(self, *kids):
        self.val = kids[0].val

    def reduce_TYPEINDIRECTION(self, *kids):
        self.val = qlast.TypeIndirection()


class PathExpr(Nonterm):
    def reduce_LBRACKET_LinkExpr_RBRACKET(self, *kids):
        self.val = kids[1].val

    def reduce_SimpleLinkExpr(self, *kids):
        self.val = kids[0].val


class SimpleLinkExpr(Nonterm):
    def reduce_LinkDirection_LabelExpr(self, *kids):
        self.val = qlast.LinkNode(name=kids[1].val, direction=kids[0].val)

    def reduce_LabelExpr(self, *kids):
        self.val = qlast.LinkNode(name=kids[0].val)


class LinkExpr(Nonterm):
    # LinkExpr AND LinkExpr
    # | LinkExpr OR LinkExpr
    # | NOT LinkExpr

    def reduce_SimpleFqLinkExpr(self, *kids):
        self.val = kids[0].val

    def reduce_LinkExpr_AND_LinkExpr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.AND,
                                   right=kids[1].val)

    def reduce_LinkExpr_OR_LinkExpr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.OR,
                                   right=kids[1].val)

    def reduce_NOT_LinkExpr(self, *kids):
        self.val = qlast.UnaryOpNode(op=ast.ops.NOT, operand=kids[1].val)


class SimpleFqLinkExpr(Nonterm):
    def reduce_LinkDirection_AnyFqNodeName_OptLinkTargetExpr(self, *kids):
        self.val = qlast.LinkNode(name=kids[1].val.name,
                                  namespace=kids[1].val.module,
                                  direction=kids[0].val,
                                  target=kids[2].val)

    def reduce_AnyFqNodeName_OptLinkTargetExpr(self, *kids):
        self.val = qlast.LinkNode(name=kids[0].val.name,
                                  namespace=kids[0].val.module,
                                  target=kids[1].val)


class OptLinkTargetExpr(Nonterm):
    def reduce_LPAREN_AnyFqNodeName_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_empty(self, *kids):
        self.val = None


class LinkDirection(Nonterm):
    def reduce_LANGBRACKET(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers
        self.val = s_pointers.PointerDirection.Inbound

    def reduce_RANGBRACKET(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers
        self.val = s_pointers.PointerDirection.Outbound


class OptFilterClause(Nonterm):
    def reduce_FILTER_LPAREN_WhereClause_RPAREN(self, *kids):
        self.val = kids[2].val

    def reduce_empty(self, *kids):
        self.val = None


class FuncApplication(Nonterm):
    def reduce_CAST_LPAREN_Expr_AS_ExtTypeExpr_RPAREN(self, *kids):
        self.val = qlast.TypeCastNode(expr=kids[2].val, type=kids[4].val)

    def reduce_FqFuncName_LPAREN_FuncArgList_OptSortClause_RPAREN_OptFilterClause(
            self, *kids):
        self.val = qlast.FunctionCallNode(func=kids[0].val, args=kids[2].val,
                                          agg_sort=kids[3].val,
                                          agg_filter=kids[5].val)

    def reduce_IDENT_LPAREN_FuncArgList_OptSortClause_RPAREN_OptFilterClause(
            self, *kids):
        func_name = kids[0].val
        args = kids[2].val

        if func_name == 'type':
            if len(args) != 1:
                msg = 'type() takes exactly one argument, {} given' \
                    .format(len(args))
                raise EdgeQLSyntaxError(msg)
            self.val = qlast.TypeRefNode(expr=args[0])
        else:
            self.val = qlast.FunctionCallNode(func=func_name, args=args,
                                              agg_sort=kids[3].val,
                                              agg_filter=kids[5].val)


class FuncExpr(Nonterm):
    def reduce_FuncApplication_OptOverClause(self, *kids):
        self.val = kids[0].val
        self.val.window = kids[1].val


class OptOverClause(Nonterm):
    def reduce_OVER_WindowSpec(self, *kids):
        self.val = kids[1].val

    def reduce_empty(self, *kids):
        self.val = None


class WindowSpec(Nonterm):
    def reduce_LPAREN_OptPartitionClause_OptSortClause_RPAREN(self, *kids):
        self.val = qlast.WindowSpecNode(
            partition=kids[1].val,
            orderby=kids[2].val
        )


class OptPartitionClause(Nonterm):
    def reduce_PARTITION_BY_ExprList(self, *kids):
        self.val = kids[2].val

    def reduce_empty(self, *kids):
        self.val = None


class FuncArgExpr(Nonterm):
    def reduce_Expr(self, *kids):
        self.val = kids[0].val

    def reduce_ParamName_COLONEQUALS_Expr(self, *kids):
        self.val = qlast.NamedArgNode(name=kids[0].val, arg=kids[2].val)


class FuncArgList(Nonterm):
    def reduce_FuncArgExpr(self, *kids):
        self.val = [kids[0].val]

    def reduce_FuncArgList_COMMA_FuncArgExpr(self, *kids):
        self.val = kids[0].val + [kids[2].val]

    def reduce_empty(self, *kids):
        self.val = []


class AnyFqLinkPropName(Nonterm):
    def reduce_AnyFqNodeName(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers
        self.val = qlast.LinkNode(
            name=kids[0].val.name, namespace=kids[0].val.module,
            direction=s_pointers.PointerDirection.Outbound, type='property')


class FqNodeName(Nonterm):
    def reduce_FqName(self, *kids):
        self.val = qlast.PrototypeRefNode(module='.'.join(kids[0].val[:-1]),
                                          name=kids[0].val[-1])


class AnyFqNodeName(Nonterm):
    # Fully-qualified node name permitting reserved keywords
    def reduce_AnyFqName(self, *kids):
        self.val = qlast.PrototypeRefNode(module='.'.join(kids[0].val[:-1]),
                                          name=kids[0].val[-1])


class FqNodeNameList(Nonterm):
    def reduce_FqNodeName(self, *kids):
        self.val = [kids[0].val]

    def reduce_FqNodeNameList_COMMA_FqNodeName(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class NodeName(Nonterm):
    def reduce_LabelExpr(self, *kids):
        self.val = qlast.PrototypeRefNode(name=kids[0].val)

    def reduce_LBRACE_AnyFqName_RBRACE(self, *kids):
        self.val = qlast.PrototypeRefNode(module='.'.join(kids[1].val[:-1]),
                                          name=kids[1].val[-1])


class NodeNameList(Nonterm):
    def reduce_NodeName(self, *kids):
        self.val = [kids[0].val]

    def reduce_NodeNameList_COMMA_NodeName(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class FqName(Nonterm):
    def reduce_LabelExpr(self, *kids):
        self.val = [kids[0].val]

    def reduce_DotName_DOUBLECOLON_LabelExpr(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class AnyFqName(Nonterm):
    def reduce_AnyLabelExpr(self, *kids):
        self.val = [kids[0].val]

    def reduce_AnyDotName_DOUBLECOLON_AnyLabelExpr(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class DotName(Nonterm):
    def reduce_LabelExpr(self, *kids):
        self.val = [kids[0].val]

    def reduce_FqName_DOT_LabelExpr(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class AnyDotName(Nonterm):
    def reduce_AnyLabelExpr(self, *kids):
        self.val = [kids[0].val]

    def reduce_AnyFqName_DOT_AnyLabelExpr(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class FqFuncName(Nonterm):
    def reduce_IDENT_DOUBLECOLON_IDENT(self, *kids):
        self.val = (kids[0].val, kids[2].val)


class AliasName(Nonterm):
    def reduce_IDENT(self, *kids):
        self.val = kids[0].val

    def reduce_UnreservedKeyword(self, *kids):
        self.val = kids[0].val


class LabelExpr(Nonterm):
    def reduce_PERCENT(self, *kids):
        self.val = kids[0].val

    def reduce_IDENT(self, *kids):
        self.val = kids[0].val

    def reduce_UnreservedKeyword(self, *kids):
        self.val = kids[0].val


class AnyLabelExpr(Nonterm):
    def reduce_LabelExpr(self, *kids):
        self.val = kids[0].val

    def reduce_ReservedKeyword(self, *kids):
        self.val = kids[0].val


class TypeName(Nonterm):
    def reduce_ArgName(self, *kids):
        self.val = qlast.TypeNameNode(maintype=kids[0].val)

    def reduce_ArgName_LANGBRACKET_TypeName_RANGBRACKET(self, *kids):
        self.val = qlast.TypeNameNode(maintype=kids[0].val,
                                      subtype=kids[2].val)


class ExtTypeExpr(Nonterm):
    def reduce_ArgName(self, *kids):
        self.val = qlast.TypeNameNode(maintype=kids[0].val)

    def reduce_PathStart_SelectPathSpec(self, *kids):
        self.val = qlast.PathNode(
            steps=[kids[0].val],
            pathspec=kids[1].val
        )

    def reduce_ArgName_LANGBRACKET_ExtTypeExpr_RANGBRACKET(self, *kids):
        self.val = qlast.TypeNameNode(maintype=kids[0].val,
                                      subtype=kids[2].val)


class ArgName(Nonterm):
    def reduce_SimpleArgName(self, *kids):
        self.val = kids[0].val

    def reduce_LBRACE_AnyFqName_RBRACE(self, *kids):
        self.val = '.'.join(kids[1].val)


class SimpleArgName(Nonterm):
    def reduce_IDENT(self, *kids):
        self.val = kids[0].val

    def reduce_UnreservedKeyword(self, *kids):
        self.val = kids[0].val

    def reduce_ReservedKeyword(self, *kids):
        self.val = kids[0].val


class ParamName(Nonterm):
    def reduce_IDENT(self, *kids):
        self.val = kids[0].val

    def reduce_UnreservedKeyword(self, *kids):
        self.val = kids[0].val


class KeywordMeta(context.ContextNontermMeta):
    def __new__(mcls, name, bases, dct, *, type):
        result = super().__new__(mcls, name, bases, dct)

        assert type in keywords.keyword_types

        for val, token in keywords.by_type[type].items():
            def method(inst, *kids):
                inst.val = kids[0].val
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
