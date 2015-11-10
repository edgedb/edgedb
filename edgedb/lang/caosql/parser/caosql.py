##
# Copyright (c) 2008-2012, 2014 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import sys

from metamagic.utils import ast
from metamagic.utils import parsing

from . import keywords
from .errors import CaosQLSyntaxError
from metamagic.caos.caosql import ast as qlast, CaosQLQueryError
from metamagic.caos.ir import ast as irast
from metamagic.caos import types as caos_types


##################
# Precedence Rules

class PrecedenceMeta(parsing.PrecedenceMeta):
    pass

class Precedence(parsing.Precedence, assoc='fail', metaclass=PrecedenceMeta):
    pass

class P_UNION_EXCEPT(Precedence, assoc='left', tokens=('UNION', 'EXCEPT')):
    pass

class P_INTERSECT(Precedence, assoc='left', tokens=('INTERSECT',)):
    pass

class P_OR(Precedence, assoc='left', tokens=('OR',)):
    pass

class P_AND(Precedence, assoc='left', tokens=('AND',)):
    pass

class P_NOT(Precedence, assoc='right', tokens=('NOT',)):
    pass

class P_EQUALS(Precedence, assoc='right', tokens=('EQUALS',)):
    pass

class P_ANGBRACKET(Precedence, assoc='nonassoc', tokens=('LANGBRACKET', 'RANGBRACKET')):
    pass

class P_LIKE_ILIKE(Precedence, assoc='nonassoc', tokens=('LIKE', 'ILIKE')):
    pass

class P_IN(Precedence, assoc='nonassoc', tokens=('IN',)):
    pass

class P_POSTFIXOP(Precedence, assoc='left'):
    pass

class P_IDENT(Precedence, assoc='nonassoc', tokens=('IDENT', 'PARTITION')):
    pass

class P_OP(Precedence, assoc='left', tokens=('OPERATOR', 'OP')):
    pass

class P_IS(Precedence, assoc='nonassoc', tokens=('IS', 'NONE')):
    pass

class P_ADD_OP(Precedence, assoc='left', tokens=('PLUS', 'MINUS')):
    pass

class P_MUL_OP(Precedence, assoc='left', tokens=('STAR', 'SLASH', 'PERCENT')):
    pass

class P_POW_OP(Precedence, assoc='left', tokens=('STARSTAR',)):
    pass


class P_UMINUS(Precedence, assoc='right'):
    pass

class P_PATHSTART(Precedence, assoc='nonassoc'):
    pass

class P_BRACKET(Precedence, assoc='left', tokens=('LBRACKET', 'RBRACKET')):
    pass

class P_PAREN(Precedence, assoc='left', tokens=('LPAREN', 'RPAREN')):
    pass


class P_DOT(Precedence, assoc='left', tokens=('DOT',)):
    pass

class P_AT(Precedence, assoc='left', tokens=('AT',)):
    pass



########
# Tokens

class TokenMeta(parsing.TokenMeta):
    pass

class Token(parsing.Token, metaclass=TokenMeta):
    pass


class T_DOT(Token, lextoken='.'):
    pass

class T_LBRACKET(Token, lextoken='['):
    pass

class T_RBRACKET(Token, lextoken=']'):
    pass

class T_LPAREN(Token, lextoken='('):
    pass

class T_RPAREN(Token, lextoken=')'):
    pass

class T_LBRACE(Token, lextoken='{'):
    pass

class T_RBRACE(Token, lextoken='}'):
    pass

class T_DOUBLECOLON(Token, lextoken='::'):
    pass

class T_COLON(Token, lextoken=':'):
    pass

class T_COMMA(Token, lextoken=','):
    pass

class T_PLUS(Token, lextoken='+'):
    pass

class T_MINUS(Token, lextoken='-'):
    pass

class T_STAR(Token, lextoken='*'):
    pass

class T_SLASH(Token, lextoken='/'):
    pass

class T_PERCENT(Token, lextoken='%'):
    pass

class T_AT(Token, lextoken='@'):
    pass

class T_DOLLAR(Token, lextoken='$'):
    pass

class T_CIRCUM(Token, lextoken='^'):
    pass

class T_STARSTAR(Token, lextoken='**'):
    pass

class T_COLONEQUALS(Token):
    pass

class T_LANGBRACKET_RANGBRACKET(Token, lextoken='<>'):
    pass

class T_LANGBRACKET(Token, lextoken='<'):
    pass

class T_RANGBRACKET(Token, lextoken='>'):
    pass

class T_EQUALS(Token, lextoken='='):
    pass

class T_ICONST(Token):
    pass

class T_FCONST(Token):
    pass

class T_SCONST(Token):
    pass

class T_IDENT(Token):
    pass

class T_OPERATOR(Token):
    pass

class T_OP(Token):
    pass

class T_TYPE_INDIRECTION(Token):
    pass


def _gen_keyword_tokens():
    # Define keyword tokens
    for val, (token, typ) in keywords.caosql_keywords.items():
        clsname = 'T_%s' % token
        cls = parsing.TokenMeta(clsname, (Token,), {'__module__': __name__}, token=token)
        setattr(sys.modules[__name__], clsname, cls)
_gen_keyword_tokens()



#############
# Productions

class Nonterm(parsing.Nonterm):
    pass


class Result(Nonterm):
    "%start"

    def reduce_Stmt(self, expr):
        "%reduce Stmt"
        self.val = expr.val

    def reduce_Expr(self, expr):
        "%reduce Expr"
        self.val = expr.val


class Stmt(Nonterm):
    def reduce_SelectNoParens(self, *kids):
        "%reduce SelectNoParens"
        self.val = kids[0].val

    def reduce_UpdateStmt(self, *kids):
        "%reduce UpdateStmt"
        self.val = kids[0].val

    def reduce_DeleteStmt(self, *kids):
        "%reduce DeleteStmt"
        self.val = kids[0].val


class UpdateStmt(Nonterm):
    def reduce_UpdateStmt(self, *kids):
        "%reduce OptNsDecl UPDATE Path SET SetClauseList OptWhereClause OptReturningClause"
        self.val = qlast.UpdateQueryNode(
            namespaces = kids[0].val,
            subject = kids[2].val,
            values = kids[4].val,
            where = kids[5].val,
            targets = kids[6].val
        )


class SetClauseList(Nonterm):
    def reduce_SetClause(self, *kids):
        "%reduce SetClause"
        self.val = [kids[0].val]

    def reduce_SetClauseList_COMMA_SetClause(self, *kids):
        "%reduce SetClauseList COMMA SetClause"
        self.val = kids[0].val + [kids[2].val]


class SetClause(Nonterm):
    def reduce_SetTarget_EQUALS_Expr(self, *kids):
        "%reduce SetTarget EQUALS Expr"
        self.val = qlast.UpdateExprNode(expr=kids[0].val, value=kids[2].val)


class SetTarget(Nonterm):
    def reduce_NodeName(self, *kids):
        "%reduce NodeName"
        self.val = kids[0].val


class DeleteStmt(Nonterm):
    def reduce_DeleteStmt(self, *kids):
        "%reduce OptNsDecl DELETE Path OptWhereClause OptReturningClause"
        self.val = qlast.DeleteQueryNode(
            namespaces = kids[0].val,
            subject = kids[2].val,
            where = kids[3].val,
            targets = kids[4].val
        )


class ReturningClause(Nonterm):
    def reduce_RETURNING_SelectTargetList(self, *kids):
        "%reduce RETURNING SelectTargetList"
        self.val = kids[1].val


class OptReturningClause(Nonterm):
    def reduce_ReturningClause(self, *kids):
        "%reduce ReturningClause"
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class SelectStmt(Nonterm):
    def reduce_SelectNoParens(self, *kids):
        "%reduce SelectNoParens [P_UMINUS]"
        self.val = kids[0].val

    def reduce_SelectWithParens(self, *kids):
        "%reduce SelectWithParens [P_UMINUS]"
        self.val = kids[0].val


class SelectWithParens(Nonterm):
    def reduce_LPAREN_SelectNoParens_RPAREN(self, *kids):
        "%reduce LPAREN SelectNoParens RPAREN"
        self.val = kids[1].val

    def reduce_LPAREN_SelectWithParens_RPAREN(self, *kids):
        "%reduce LPAREN SelectWithParens RPAREN"
        self.val = kids[1].val


class SelectNoParens(Nonterm):
    def reduce_NsDecl_SelectClause_OptSortClause_OptSelectLimit(self, *kids):
        "%reduce NsDecl SelectClause OptSortClause OptSelectLimit"
        qry = kids[1].val
        qry.orderby = kids[2].val
        qry.offset = kids[3].val[0]
        qry.limit = kids[3].val[1]
        qry.namespaces = kids[0].val

        self.val = qry

    def reduce_NsDecl_WithClause_SelectClause_OptSortClause_OptSelectLimit(self, *kids):
        "%reduce NsDecl WithClause SelectClause OptSortClause OptSelectLimit"
        qry = kids[2].val
        qry.orderby = kids[3].val
        qry.offset = kids[4].val[0]
        qry.limit = kids[4].val[1]
        qry.namespaces = kids[0].val
        qry.cges = kids[1].val

        self.val = qry

    def reduce_WithClause_SelectClause_OptSortClause_OptSelectLimit(self, *kids):
        "%reduce WithClause SelectClause OptSortClause OptSelectLimit"
        qry = kids[1].val
        qry.orderby = kids[2].val
        qry.offset = kids[3].val[0]
        qry.limit = kids[3].val[1]
        qry.cges = kids[0].val

        self.val = qry

    def reduce_SimpleSelect(self, *kids):
        "%reduce SimpleSelect OptSelectLimit"
        qry = kids[0].val
        qry.offset = kids[1].val[0]
        qry.limit = kids[1].val[1]
        self.val = qry

    def reduce_SelectClause_SortClause_OptSelectLimit(self, *kids):
        "%reduce SelectClause SortClause OptSelectLimit"
        qry = kids[0].val
        qry.orderby = kids[1].val
        qry.offset = kids[2].val[0]
        qry.limit = kids[2].val[1]

        self.val = qry


class WithClause(Nonterm):
    def reduce_WITH_CgeList(self, *kids):
        "%reduce WITH CgeList"
        self.val = kids[1].val


class CgeList(Nonterm):
    def reduce_Cge(self, *kids):
        "%reduce Cge"
        self.val = [kids[0].val]

    def reduce_CgeList_COMMA_Cge(self, *kids):
        "%reduce CgeList COMMA Cge"
        self.val = kids[0].val + [kids[2].val]


class Cge(Nonterm):
    def reduce_AnchorName_AS_LPAREN_SelectStmt_RPAREN(self, *kids):
        "%reduce AnchorName AS LPAREN SelectStmt RPAREN"
        self.val = qlast.CGENode(expr=kids[3].val, alias=kids[0].val)


class SelectClause(Nonterm):
    def reduce_SimpleSelect(self, *kids):
        "%reduce SimpleSelect"
        self.val = kids[0].val

    def reduce_SelectWithParens(self, *kids):
        "%reduce SelectWithParens"
        self.val = kids[0].val


class SimpleSelect(Nonterm):
    def reduce_SelectStmt(self, *kids):
        "%reduce SELECT OptDistinct SelectTargetList OptWhereClause OptGroupClause"
        self.val = qlast.SelectQueryNode(
                        distinct=kids[1].val,
                        targets=kids[2].val,
                        where=kids[3].val,
                        groupby=kids[4].val
                   )

    def reduce_UNION(self, *kids):
        "%reduce SelectClause UNION OptAll SelectClause"
        self.val = qlast.SelectQueryNode(
            op=qlast.UNION,
            op_larg=kids[0].val,
            op_rarg=kids[3].val
        )

    def reduce_INTERSECT(self, *kids):
        "%reduce SelectClause INTERSECT OptAll SelectClause"
        self.val = qlast.SelectQueryNode(
            op=qlast.INTERSECT,
            op_larg=kids[0].val,
            op_rarg=kids[3].val
        )

    def reduce_EXCEPT(self, *kids):
        "%reduce SelectClause EXCEPT OptAll SelectClause"
        self.val = qlast.SelectQueryNode(
            op=qlast.EXCEPT,
            op_larg=kids[0].val,
            op_rarg=kids[3].val
        )


class OptNsDecl(Nonterm):
    def reduce_NsDecl(self, *kids):
        "%reduce NsDecl"
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class NsDecl(Nonterm):
    def reduce_USING_NsDeclElList(self, *kids):
        "%reduce USING NsDeclElList"
        self.val = kids[1].val


class NsDeclElList(Nonterm):
    def reduce_NsDeclEl(self, *kids):
        "%reduce NsDeclEl"
        self.val = [kids[0].val]

    def reduce_NsDeclElList_COMMA_NsDeclEl(self, *kids):
        "%reduce NsDeclElList COMMA NsDeclEl"
        self.val = kids[0].val + [kids[2].val]


class NsDeclEl(Nonterm):
    def reduce_FqName(self, *kids):
        "%reduce FqName"
        self.val = qlast.NamespaceDeclarationNode(namespace='.'.join(kids[0].val))

    def reduce_FqName_AS_NsAliasName(self, *kids):
        "%reduce FqName AS NsAliasName"
        self.val = qlast.NamespaceDeclarationNode(namespace='.'.join(kids[0].val),
                                                  alias=kids[2].val)


class OptDistinct(Nonterm):
    def reduce_DISTINCT(self, *kids):
        "%reduce DISTINCT"
        self.val = True

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = False


class SelectTargetList(Nonterm):
    def reduce_SelectTargetEl(self, *kids):
        "%reduce SelectTargetEl"
        self.val = [kids[0].val]

    def reduce_SelectTargetList_COMMA_SelectTargetEl(self, *kids):
        "%reduce SelectTargetList COMMA SelectTargetEl"
        self.val = kids[0].val + [kids[2].val]


class SelectTargetEl(Nonterm):
    def reduce_Expr_AS_LabelExpr(self, *kids):
        "%reduce Expr AS LabelExpr"
        self.val = qlast.SelectExprNode(expr=kids[0].val, alias=kids[2].val)

    def reduce_Expr(self, *kids):
        "%reduce Expr"
        self.val = qlast.SelectExprNode(expr=kids[0].val)

    def reduce_Path_SelectPathSpec(self, *kids):
        "%reduce Path SelectPathSpec"
        kids[0].val.pathspec = kids[1].val
        self.val = qlast.SelectExprNode(expr=kids[0].val)


class SelectPathSpec(Nonterm):
    def reduce_LDBRACKET_SelectPointerSpecList_RDBRACKET(self, *kids):
        "%reduce LBRACKET SelectPointerSpecList RBRACKET"
        self.val = kids[1].val


class OptSelectPathSpec(Nonterm):
    def reduce_SelectPathSpec(self, *kids):
        "%reduce SelectPathSpec"
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class SelectPointerSpecList(Nonterm):
    def reduce_SelectPointerSpec(self, *kids):
        "%reduce SelectPointerSpec"
        self.val = [kids[0].val]

    def reduce_SelectPointerSpecList_COMMA_SelectPointerSpec(self, *kids):
        "%reduce SelectPointerSpecList COMMA SelectPointerSpec"
        self.val = kids[0].val + [kids[2].val]


class SelectPointerSpec(Nonterm):
    def reduce_PointerGlob(self, *kids):
        "%reduce PointerGlob"
        self.val = kids[0].val

    def reduce_AT_AnyFqLinkPropName(self, *kids):
        "%reduce AT AnyFqLinkPropName"
        self.val = qlast.SelectPathSpecNode(
            expr=qlast.LinkExprNode(expr=kids[1].val)
        )

    def reduce_PointerSpecSetExpr_OptSelectPathSpec(self, *kids):
        "%reduce PointerSpecSetExpr OptPointerRecursionSpec OptSelectPathSpec"
        self.val = kids[0].val
        self.val.recurse = kids[1].val
        self.val.pathspec = kids[2].val


class PointerSpecSetExpr(Nonterm):
    def reduce_ParenthesizedTransformedLinkedSetExpr(self, *kids):
        "%reduce ParenthesizedTransformedLinkedSetExpr"
        self.val = kids[0].val

    def reduce_SimpleFqLinkExpr(self, *kids):
        "%reduce SimpleFqLinkExpr"
        self.val = qlast.SelectPathSpecNode(
            expr=qlast.LinkExprNode(expr=kids[0].val)
        )

    def reduce_TYPE_INDIRECTION_SubpathNoParens(self, *kids):
        "%reduce TYPE_INDIRECTION SubpathNoParens"
        self.val = qlast.SelectTypeRefNode(
            attrs=kids[1].val
        )


class QualifiedLinkedSetExpr(Nonterm):
    def reduce_SimpleFqLinkExpr(self, *kids):
        "%reduce SimpleFqLinkExpr"
        self.val = qlast.SelectPathSpecNode(
            expr=qlast.LinkExprNode(expr=kids[0].val)
        )

    def reduce_SimpleFqLinkExpr_WhereClause(self, *kids):
        "%reduce SimpleFqLinkExpr WhereClause"
        self.val = qlast.SelectPathSpecNode(
            expr=qlast.LinkExprNode(expr=kids[0].val),
            where=kids[1].val
        )


class TransformedLinkedSetExpr(Nonterm):
    def reduce_QualifiedLinkedSetExpr_OptSortClause(self, *kids):
        "%reduce QualifiedLinkedSetExpr OptSortClause OptSelectLimit"
        self.val = kids[0].val
        self.val.orderby = kids[1].val
        self.val.offset = kids[2].val[0]
        self.val.limit = kids[2].val[1]


class ParenthesizedTransformedLinkedSetExpr(Nonterm):
    def reduce_LPAREN_TransformedLinkedSetExpr_RPAREN(self, *kids):
        "%reduce LPAREN TransformedLinkedSetExpr RPAREN"
        self.val = kids[1].val


class OptPointerRecursionSpec(Nonterm):
    def reduce_STAR(self, *kids):
        "%reduce STAR"
        self.val = qlast.ConstantNode(value=0)

    def reduce_STAR_NumberConstant(self, *kids):
        "%reduce STAR NumberConstant"
        self.val = kids[1].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class PointerGlob(Nonterm):
    def reduce_STAR(self, *kids):
        "%reduce STAR"
        flt = qlast.PointerGlobFilter(property='loading', value='eager')
        self.val = qlast.PointerGlobNode(filters=[flt], type='link')

    def reduce_STAR_PointerGlobFilterList(self, *kids):
        "%reduce STAR LPAREN PointerGlobFilterList RPAREN"
        self.val = qlast.PointerGlobNode(filters=kids[2].val, type='link')

    def reduce_AT_STAR(self, *kids):
        "%reduce AT STAR"
        flt = qlast.PointerGlobFilter(property='loading', value='eager')
        self.val = qlast.PointerGlobNode(filters=[flt], type='property')

    def reduce_AT_STAR_PointerGlobFilterList(self, *kids):
        "%reduce AT STAR LPAREN PointerGlobFilterList RPAREN"
        self.val = qlast.PointerGlobNode(filters=kids[2].val, type='property')


class PointerGlobFilterList(Nonterm):
    def reduce_PointerGlobFilter(self, *kids):
        "%reduce PointerGlobFilter"
        self.val = [kids[0].val]

    def reduce_PointerGlobFilterList_COMMA_PointerGlobFilter(self, *kids):
        "%reduce PointerGlobFilterList COMMA PointerGlobFilter"
        self.val = kids[0].val + [kids[2].val]


class PointerGlobFilter(Nonterm):
    def reduce_LabelExpr_EQUALS_LabelExpr(self, *kids):
        "%reduce LabelExpr EQUALS LabelExpr"
        self.val = qlast.PointerGlobFilter(property=kids[0].val, value=kids[2].val)

    def reduce_ANY_LabelExpr(self, *kids):
        "%reduce ANY LabelExpr"
        self.val = qlast.PointerGlobFilter(property=kids[1].val, any=True)


class WhereClause(Nonterm):
    def reduce_WHERE_Expr(self, *kids):
        "%reduce WHERE Expr"
        self.val = kids[1].val


class OptWhereClause(Nonterm):
    def reduce_WhereClause(self, *kids):
        "%reduce WhereClause"
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class OptGroupClause(Nonterm):
    def reduce_GROUP_BY_ExprList(self, *kids):
        "%reduce GROUP BY ExprList"
        self.val = kids[2].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class SortClause(Nonterm):
    def reduce_ORDER_BY_OrderbyList(self, *kids):
        "%reduce ORDER BY OrderbyList"
        self.val = kids[2].val


class OptSortClause(Nonterm):
    def reduce_SortClause(self, *kids):
        "%reduce SortClause"
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class OrderbyList(Nonterm):
    def reduce_OrderbyExpr(self, *kids):
        "%reduce OrderbyExpr"
        self.val = [kids[0].val]

    def reduce_OrderbyList_COMMA_OrderbyExpr(self, *kids):
        "%reduce OrderbyList COMMA OrderbyExpr"
        self.val = kids[0].val + [kids[2].val]


class OrderbyExpr(Nonterm):
    def reduce_Expr_OptDirection_OptNonesOrder(self, *kids):
        "%reduce Expr OptDirection OptNonesOrder"
        self.val = qlast.SortExprNode(path=kids[0].val,
                                      direction=kids[1].val,
                                      nones_order=kids[2].val)


class OptSelectLimit(Nonterm):
    def reduce_SelectLimit(self, *kids):
        "%reduce SelectLimit"
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = (None, None)


class SelectLimit(Nonterm):
    def reduce_OffsetClause_LimitClause(self, *kids):
        "%reduce OffsetClause LimitClause"
        self.val = (kids[0].val, kids[1].val)

    def reduce_OffsetClause(self, *kids):
        "%reduce OffsetClause"
        self.val = (kids[0].val, None)

    def reduce_LimitClause(self, *kids):
        "%reduce LimitClause"
        self.val = (None, kids[0].val)


class OffsetClause(Nonterm):
    def reduce_OFFSET_NumberConstant(self, *kids):
        "%reduce OFFSET NumberConstant"
        self.val = kids[1].val


class LimitClause(Nonterm):
    def reduce_LIMIT_NumberConstant(self, *kids):
        "%reduce LIMIT NumberConstant"
        self.val = kids[1].val


class OptDirection(Nonterm):
    def reduce_ASC(self, *kids):
        "%reduce ASC"
        self.val = qlast.SortAsc

    def reduce_DESC(self, *kids):
        "%reduce DESC"
        self.val = qlast.SortDesc

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = qlast.SortDefault


class OptNonesOrder(Nonterm):
    def reduce_NONES_FIRST(self, *kids):
        "%reduce NONES FIRST"
        self.val = qlast.NonesFirst

    def reduce_NONES_LAST(self, *kids):
        "%reduce NONES LAST"
        self.val = qlast.NonesLast

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class OptAll(Nonterm):
    def reduce_ALL(self, *kids):
        "%reduce ALL"
        self.val = True

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class IndirectionEl(Nonterm):
    def reduce_LBRACKET_Expr_RBRACKET(self, *kids):
        "%reduce LBRACKET Expr RBRACKET"
        self.val = qlast.IndexNode(index=kids[1].val)

    def reduce_LBRACKET_Expr_COLON_Expr_RBRACKET(self, *kids):
        "%reduce LBRACKET Expr COLON Expr RBRACKET"
        self.val = qlast.SliceNode(start=kids[1].val, stop=kids[3].val)

    def reduce_LBRACKET_Expr_COLON_RBRACKET(self, *kids):
        "%reduce LBRACKET Expr COLON RBRACKET"
        self.val = qlast.SliceNode(start=kids[1].val, stop=None)

    def reduce_LBRACKET_COLON_Expr_RBRACKET(self, *kids):
        "%reduce LBRACKET COLON Expr RBRACKET"
        self.val = qlast.SliceNode(start=None, stop=kids[2].val)


class OptIndirection(Nonterm):
    def reduce_OptIndirection_IndirectionEl(self, *kids):
        "%reduce OptIndirection IndirectionEl"
        if kids[0].val is not None:
            self.val = kids[0].val + [kids[1].val]
        else:
            self.val = [kids[1].val]

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class Indirection(Nonterm):
    def reduce_OptIndirection_IndirectionEl(self, *kids):
        "%reduce OptIndirection IndirectionEl"
        if kids[0].val is not None:
            self.val = kids[0].val + [kids[1].val]
        else:
            self.val = [kids[1].val]


class Expr(Nonterm):
    # Path | Constant | '(' Expr ')' | FuncExpr | Sequence
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
        "%reduce Path"
        self.val = kids[0].val

    def reduce_Constant(self, *kids):
        "%reduce Constant"
        self.val = kids[0].val

    def reduce_LPAREN_Expr_RPAREN_OptIndirection(self, *kids):
        "%reduce LPAREN Expr RPAREN OptIndirection"
        if kids[3].val:
            self.val = qlast.IndirectionNode(arg=kids[1].val, indirection=kids[3].val)
        else:
            self.val = kids[1].val

    def reduce_FuncExpr(self, *kids):
        "%reduce FuncExpr"
        self.val = kids[0].val

    def reduce_SelectWithParens(self, *kids):
        "%reduce SelectWithParens [P_UMINUS]"
        self.val = kids[0].val

    def reduce_EXISTS_SelectWithParens(self, *kids):
        "%reduce EXISTS SelectWithParens"
        self.val = qlast.ExistsPredicateNode(expr=kids[1].val)

    def reduce_EXISTS_LPAREN_Expr_RPAREN(self, *kids):
        "%reduce EXISTS LPAREN Expr RPAREN"
        self.val = qlast.ExistsPredicateNode(expr=kids[2].val)

    def reduce_LPAREN_Sequence_RPAREN(self, *kids):
        "%reduce LPAREN Sequence RPAREN"
        self.val = kids[1].val

    def reduce_unary_plus(self, *kids):
        "%reduce PLUS Expr [P_UMINUS]"
        self.val = qlast.UnaryOpNode(op=ast.ops.UPLUS, operand=kids[1].val)

    def reduce_unary_minus(self, *kids):
        "%reduce MINUS Expr [P_UMINUS]"
        self.val = qlast.UnaryOpNode(op=ast.ops.UMINUS, operand=kids[1].val)

    def reduce_add(self, *kids):
        "%reduce Expr PLUS Expr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.ADD, right=kids[2].val)

    def reduce_sub(self, *kids):
        "%reduce Expr MINUS Expr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.SUB, right=kids[2].val)

    def reduce_mul(self, *kids):
        "%reduce Expr STAR Expr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.MUL, right=kids[2].val)

    def reduce_div(self, *kids):
        "%reduce Expr SLASH Expr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.DIV, right=kids[2].val)

    def reduce_mod(self, *kids):
        "%reduce Expr PERCENT Expr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.MOD, right=kids[2].val)

    def reduce_pow(self, *kids):
        "%reduce Expr STARSTAR Expr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.POW, right=kids[2].val)

    def reduce_lt(self, *kids):
        "%reduce Expr LANGBRACKET Expr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.LT, right=kids[2].val)

    def reduce_gt(self, *kids):
        "%reduce Expr RANGBRACKET Expr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.GT, right=kids[2].val)

    def reduce_equals(self, *kids):
        "%reduce Expr EQUALS Expr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.EQ, right=kids[2].val)

    def reduce_Expr_OP_Expr(self, *kids):
        "%reduce Expr OP Expr [P_OP]"
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

    def reduce_OP_Expr(self, *kids):
        "%reduce OP Expr [P_OP]"
        self.val = qlast.UnaryOpNode(op=kids[0].val, operand=kids[1].val)

    def reduce_Expr_OP(self, *kids):
        "%reduce Expr OP [P_POSTFIXOP]"
        self.val = qlast.PostfixOpNode(op=kids[1].val, operand=kids[0].val)

    def reduce_Expr_AND_Expr(self, *kids):
        "%reduce Expr AND Expr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.AND, right=kids[2].val)

    def reduce_Expr_OR_Expr(self, *kids):
        "%reduce Expr OR Expr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.OR, right=kids[2].val)

    def reduce_NOT_Expr(self, *kids):
        "%reduce NOT Expr"
        self.val = qlast.UnaryOpNode(op=ast.ops.NOT, operand=kids[1].val)

    def reduce_Expr_LIKE_Expr(self, *kids):
        "%reduce Expr LIKE Expr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=irast.LIKE, right=kids[2].val)

    def reduce_Expr_NOT_LIKE_Expr(self, *kids):
        "%reduce Expr NOT LIKE Expr"
        val = qlast.BinOpNode(left=kids[0].val, op=irast.LIKE, right=kids[2].val)
        self.val = qlast.UnaryOpNode(op=ast.ops.NOT, operand=val)

    def reduce_Expr_ILIKE_Expr(self, *kids):
        "%reduce Expr ILIKE Expr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=irast.ILIKE, right=kids[2].val)

    def reduce_Expr_NOT_ILIKE_Expr(self, *kids):
        "%reduce Expr NOT ILIKE Expr"
        val = qlast.BinOpNode(left=kids[0].val, op=irast.ILIKE, right=kids[2].val)
        self.val = qlast.UnaryOpNode(op=ast.ops.NOT, operand=val)

    def reduce_Expr_IS_NONE(self, *kids):
        "%reduce Expr IS NONE"
        self.val = qlast.NoneTestNode(expr=kids[0].val)

    def reduce_Expr_IS_NOT_NONE(self, *kids):
        "%reduce Expr IS NOT NONE"
        nt = qlast.NoneTestNode(expr=kids[0].val)
        self.val = qlast.UnaryOpNode(op=ast.ops.NOT, operand=nt)

    def reduce_Expr_IS_IsExpr(self, *kids):
        "%reduce Expr IS IsExpr [P_IS]"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.IS, right=kids[2].val)

    def reduce_Expr_IS_NOT_IsExpr(self, *kids):
        "%reduce Expr IS NOT IsExpr [P_IS]"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.IS_NOT, right=kids[3].val)

    def reduce_Expr_IN_InExpr(self, *kids):
        "%reduce Expr IN InExpr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.IN, right=kids[2].val)

    def reduce_Expr_NOT_IN_in_expr(self, *kids):
        "%reduce Expr NOT IN InExpr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.NOT_IN, right=kids[3].val)


class Sequence(Nonterm):
    def reduce_Expr_COMMA_ExprList(self, *kids):
        "%reduce Expr COMMA ExprList"
        self.val = qlast.SequenceNode(elements=[kids[0].val] + kids[2].val)


class ExprList(Nonterm):
    def reduce_Expr(self, *kids):
        "%reduce Expr"
        self.val = [kids[0].val]

    def reduce_ExprList_COMMA_Expr(self, *kids):
        "%reduce ExprList COMMA Expr"
        self.val = kids[0].val + [kids[2].val]


class InExpr(Nonterm):
    def reduce_LPAREN_ExprList_RPAREN(self, *kids):
        "%reduce LPAREN ExprList RPAREN"
        self.val = qlast.SequenceNode(elements=kids[1].val)

    def reduce_Path(self, *kids):
        "%reduce Path"
        self.val = kids[0].val

    def reduce_ArgConstant(self, *kids):
        "%reduce ArgConstant"
        self.val = kids[0].val


class IsExpr(Nonterm):
    def reduce_LPAREN_FqNodeNameList_RPAREN(self, *kids):
        "%reduce LPAREN FqNodeNameList RPAREN"
        self.val = qlast.SequenceNode(elements=kids[1].val)

    def reduce_FqNodeName(self, *kids):
        "%reduce FqNodeName"
        self.val = kids[0].val

    def reduce_ArgConstant(self, *kids):
        "%reduce ArgConstant"
        self.val = kids[0].val


class Constant(Nonterm):
    # BaseConstant
    # | BaseNumberConstant
    # | BaseStringConstant
    # | BaseBooleanConstant

    def reduce_BaseConstant(self, *kids):
        "%reduce BaseConstant"
        self.val = kids[0].val

    def reduce_BaseNumberConstant(self, *kids):
        "%reduce BaseNumberConstant"
        self.val = kids[0].val

    def reduce_BaseStringConstant(self, *kids):
        "%reduce BaseStringConstant"
        self.val = kids[0].val

    def reduce_BaseBooleanConstant(self, *kids):
        "%reduce BaseBooleanConstant"
        self.val = kids[0].val


class BaseConstant(Nonterm):
    # NoneConstant
    # | ArgConstant

    def reduce_NoneConstant(self, *kids):
        "%reduce NoneConstant"
        self.val = kids[0].val

    def reduce_ArgConstant(self, *kids):
        "%reduce ArgConstant"
        self.val = kids[0].val


class NoneConstant(Nonterm):
    def reduce_NONE(self, *kids):
        "%reduce NONE"
        self.val = qlast.ConstantNode(value=None)


class ArgConstant(Nonterm):
    def reduce_DOLLAR_ICONST(self, *kids):
        "%reduce DOLLAR ICONST"
        self.val = qlast.ConstantNode(value=None, index=int(kids[1].val))

    def reduce_DOLLAR_ArgName(self, *kids):
        "%reduce DOLLAR ArgName"
        self.val = qlast.ConstantNode(value=None, index=str(kids[1].val))


class BaseNumberConstant(Nonterm):
    def reduce_ICONST(self, *kids):
        "%reduce ICONST"
        self.val = qlast.ConstantNode(value=int(kids[0].val))

    def reduce_FCONST(self, *kids):
        "%reduce FCONST"
        self.val = qlast.ConstantNode(value=float(kids[0].val))


class NumberConstant(Nonterm):
    def reduce_BaseConstant(self, *kids):
        "%reduce BaseConstant"
        self.val = kids[0].val

    def reduce_BaseNumberConstant(self, *kids):
        "%reduce BaseNumberConstant"
        self.val = kids[0].val


class BaseStringConstant(Nonterm):
    def reduce_SCONST(self, *kids):
        "%reduce SCONST"
        self.val = qlast.ConstantNode(value=str(kids[0].val))


class BaseBooleanConstant(Nonterm):
    def reduce_TRUE(self, *kids):
        "%reduce TRUE"
        self.val = qlast.ConstantNode(value=True)

    def reduce_FALSe(self, *kids):
        "%reduce FALSE"
        self.val = qlast.ConstantNode(value=False)


class Path(Nonterm):
    def reduce_PathSimple(self, *kids):
        "%reduce PathSimple"
        self.val = qlast.PathNode(steps=kids[0].val)


class PathSimple(Nonterm):
    def reduce_PathStart_OptSubpath(self, *kids):
        "%reduce PathStart OptSubpath"
        self.val = [kids[0].val]
        if kids[1].val:
            self.val += kids[1].val

    def reduce_FuncApplication_SubpathNoParens(self, *kids):
        "%reduce FuncApplication SubpathNoParens"
        self.val = [kids[0].val] + kids[1].val


class PathStart(Nonterm):
    def reduce_NodeName_Anchor(self, *kids):
        "%reduce NodeName Anchor"
        step = qlast.PathStepNode(expr=kids[0].val.name,
                                  namespace=kids[0].val.module)
        self.val = qlast.PathNode(steps=[step], var=kids[1].val)

    def reduce_NodeName(self, *kids):
        "%reduce NodeName [P_PATHSTART]"
        self.val = qlast.PathStepNode(expr=kids[0].val.name,
                                      namespace=kids[0].val.module)


class OptSubpath(Nonterm):
    def reduce_SubpathNoParens(self, *kids):
        "%reduce SubpathNoParens"
        self.val = kids[0].val

    def reduce_SubpathWithParens(self, *kids):
        "%reduce SubpathWithParens"
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class SubpathWithParens(Nonterm):
    def reduce_LPAREN_SubpathNoParens_RPAREN(self, *kids):
        "%reduce LPAREN SubpathNoParens RPAREN"
        self.val = kids[1].val

    def reduce_LPAREN_SubpathWithParens_RPAREN(self, *kids):
        "%reduce LPAREN SubpathWithParens RPAREN"
        self.val = kids[1].val


class SubpathNoParens(Nonterm):
    def reduce_PathStepList(self, *kids):
        "%reduce PathStepList"
        self.val = kids[0].val


class PathStepList(Nonterm):
    def reduce_PathStep(self, *kids):
        "%reduce PathStep"
        self.val = [kids[0].val]

    def reduce_PathStepList_PathStep(self, *kids):
        "%reduce PathStepList PathStep"
        self.val = kids[0].val + [kids[1].val]


class PathStep(Nonterm):
    def reduce_PathStepSimple_Anchor(self, *kids):
        "%reduce PathStepSimple Anchor"

        path = kids[0].val

        if not isinstance(path, qlast.PathNode):
            path = qlast.PathNode(steps=[path])
        path.var = kids[1].val

        self.val = path

    def reduce_PathStepSimple(self, *kids):
        "%reduce PathStepSimple"
        self.val = kids[0].val


class Anchor(Nonterm):
    def reduce_LBRACE_AnchorName_RBRACE(self, *kids):
        "%reduce LBRACE AnchorName RBRACE"
        self.val = qlast.VarNode(name=kids[1].val)


class PathStepSimple(Nonterm):
    def reduce_DOT_PathExprOrType(self, *kids):
        "%reduce DOT PathExprOrType"
        self.val = qlast.LinkExprNode(expr=kids[1].val)

    def reduce_AT_PathExpr(self, *kids):
        "%reduce AT PathExpr"
        self.val = qlast.LinkPropExprNode(expr=kids[1].val)
        kids[1].val.type = 'property'


class PathExprOrType(Nonterm):
    def reduce_PathExpr(self, *kids):
        "%reduce PathExpr"
        self.val = kids[0].val

    def reduce_TYPE_INDIRECTION(self, *kids):
        "%reduce TYPE_INDIRECTION"
        self.val = qlast.TypeIndirection()


class PathExpr(Nonterm):
    def reduce_LBRACKET_LinkExpr_RBRACKET(self, *kids):
        "%reduce LBRACKET LinkExpr RBRACKET"
        self.val = kids[1].val

    def reduce_SimpleLinkExpr(self, *kids):
        "%reduce SimpleLinkExpr"
        self.val = kids[0].val


class SimpleLinkExpr(Nonterm):
    def reduce_LinkDirection_LabelExpr(self, *kids):
        "%reduce LinkDirection LabelExpr"
        self.val = qlast.LinkNode(name=kids[1].val, direction=kids[0].val)

    def reduce_LabelExpr(self, *kids):
        "%reduce LabelExpr"
        self.val = qlast.LinkNode(name=kids[0].val)


class LinkExpr(Nonterm):
    # LinkExpr AND LinkExpr
    # | LinkExpr OR LinkExpr
    # | NOT LinkExpr

    def reduce_SimpleFqLinkExpr(self, *kids):
        "%reduce SimpleFqLinkExpr"
        self.val = kids[0].val

    def reduce_LinkExpr_AND_LinkExpr(self, *kids):
        "%reduce LinkExpr AND LinkExpr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.AND, right=kids[1].val)

    def reduce_LinkExpr_OR_LinkExpr(self, *kids):
        "%reduce LinkExpr OR LinkExpr"
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.OR, right=kids[1].val)

    def reduce_NOT_LinkExpr(self, *kids):
        "%reduce NOT LinkExpr"
        self.val = qlast.UnaryOpNode(op=ast.ops.NOT, operand=kids[1].val)


class SimpleFqLinkExpr(Nonterm):
    def reduce_LinkDirection_AnyFqNodeName_OptLinkTargetExpr(self, *kids):
        "%reduce LinkDirection AnyFqNodeName OptLinkTargetExpr"
        self.val = qlast.LinkNode(name=kids[1].val.name,
                                  namespace=kids[1].val.module,
                                  direction=kids[0].val,
                                  target=kids[2].val)

    def reduce_AnyFqNodeName_OptLinkTargetExpr(self, *kids):
        "%reduce AnyFqNodeName OptLinkTargetExpr"
        self.val = qlast.LinkNode(name=kids[0].val.name,
                                  namespace=kids[0].val.module,
                                  target=kids[1].val)


class OptLinkTargetExpr(Nonterm):
    def reduce_LPAREN_AnyFqNodeName_RPAREN(self, *kids):
        "%reduce LPAREN AnyFqNodeName RPAREN"
        self.val = kids[1].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class LinkDirection(Nonterm):
    def reduce_LANGBRACKET(self, *kids):
        "%reduce LANGBRACKET"
        self.val = caos_types.InboundDirection

    def reduce_RANGBRACKET(self, *kids):
        "%reduce RANGBRACKET"
        self.val = caos_types.OutboundDirection


class FuncApplication(Nonterm):
    def reduce_CAST_LPAREN_Expr_AS_TypeName_RPAREN(self, *kids):
        "%reduce CAST LPAREN Expr AS TypeName RPAREN"
        self.val = qlast.TypeCastNode(expr=kids[2].val, type=kids[4].val)

    def reduce_FqFuncName_LPAREN_FuncArgList_RPAREN(self, *kids):
        "%reduce FqFuncName LPAREN FuncArgList OptSortClause RPAREN"
        self.val = qlast.FunctionCallNode(func=kids[0].val, args=kids[2].val,
                                          agg_sort=kids[3].val)

    def reduce_IDENT_LPAREN_FuncArgList_RPAREN(self, *kids):
        "%reduce IDENT LPAREN FuncArgList OptSortClause RPAREN"

        func_name = kids[0].val
        args = kids[2].val

        if func_name == 'type':
            if len(args) != 1:
                msg = 'type() takes exactly one argument, {} given'.format(len(args))
                raise CaosQLSyntaxError(msg)
            self.val = qlast.TypeRefNode(expr=args[0])
        else:
            self.val = qlast.FunctionCallNode(func=func_name, args=args, agg_sort=kids[3].val)


class FuncExpr(Nonterm):
    def reduce_FuncApplication_OptOverClause(self, *kids):
        "%reduce FuncApplication OptOverClause"
        self.val = kids[0].val
        self.val.window = kids[1].val


class OptOverClause(Nonterm):
    def reduce_OVER_WindowSpec(self, *kids):
        "%reduce OVER WindowSpec"
        self.val = kids[1].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class WindowSpec(Nonterm):
    def reduce_LPAREN_OptPartitionClause_OptSortClause_RPAREN(self, *kids):
        "%reduce LPAREN OptPartitionClause OptSortClause RPAREN"
        self.val = qlast.WindowSpecNode(
                        partition=kids[1].val,
                        orderby=kids[2].val
                   )


class OptPartitionClause(Nonterm):
    def reduce_PARTITION_BY_ExprList(self, *kids):
        "%reduce PARTITION BY ExprList"
        self.val = kids[2].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class FuncArgExpr(Nonterm):
    def reduce_Expr(self, *kids):
        "%reduce Expr"
        self.val = kids[0].val

    def reduce_ParamName_COLONEQUALS_Expr(self, *kids):
        "%reduce ParamName COLONEQUALS Expr"
        self.val = qlast.NamedArgNode(name=kids[0].val, arg=kids[2].val)


class FuncArgList(Nonterm):
    def reduce_FuncArgExpr(self, *kids):
        "%reduce FuncArgExpr"
        self.val = [kids[0].val]

    def reduce_FuncArgList_COMMA_Expr(self, *kids):
        "%reduce FuncArgList COMMA FuncArgExpr"
        self.val = kids[0].val + [kids[2].val]

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = []


class LinkPropName(Nonterm):
    def reduce_NodeName(self, *kids):
        "%reduce NodeName"
        self.val = qlast.LinkNode(name=kids[0].val.name, namespace=kids[0].val.module,
                                  type='property')


class AnyFqLinkPropName(Nonterm):
    def reduce_AnyFqNodeName(self, *kids):
        "%reduce AnyFqNodeName"
        self.val = qlast.LinkNode(name=kids[0].val.name,
                                  namespace=kids[0].val.module,
                                  direction=caos_types.OutboundDirection,
                                  type='property')


class AnchorName(Nonterm):
    def reduce_LabelExpr(self, *kids):
        "%reduce LabelExpr"
        self.val = kids[0].val


class FqNodeName(Nonterm):
    def reduce_FqName(self, *kids):
        "%reduce FqName"
        self.val = qlast.PrototypeRefNode(module='.'.join(kids[0].val[:-1]), name=kids[0].val[-1])


class AnyFqNodeName(Nonterm):
    # Fully-qualified node name permitting reserved keywords
    def reduce_FqName(self, *kids):
        "%reduce AnyFqName"
        self.val = qlast.PrototypeRefNode(module='.'.join(kids[0].val[:-1]), name=kids[0].val[-1])


class FqNodeNameList(Nonterm):
    def reduce_FqNodeName(self, *kids):
        "%reduce FqNodeName"
        self.val = [kids[0].val]

    def reduce_FqNodeNameList_COMMA_FqNodeName(self, *kids):
        "%reduce FqNodeNameList COMMA FqNodeName"
        self.val = kids[0].val + [kids[2].val]


class NodeName(Nonterm):
    def reduce_LabelExpr(self, *kids):
        "%reduce LabelExpr"
        self.val = qlast.PrototypeRefNode(name=kids[0].val)

    def reduce_LBRACKET_AnyFqName_RBRACKET(self, *kids):
        "%reduce LBRACKET AnyFqName RBRACKET"
        self.val = qlast.PrototypeRefNode(module='.'.join(kids[1].val[:-1]), name=kids[1].val[-1])


class NodeNameList(Nonterm):
    def reduce_NodeName(self, *kids):
        "%reduce NodeName"
        self.val = [kids[0].val]

    def reduce_NodeNameList_COMMA_NodeName(self, *kids):
        "%reduce NodeNameList COMMA NodeName"
        self.val = kids[0].val + [kids[2].val]


class FqName(Nonterm):
    def reduce_LabelExpr(self, *kids):
        "%reduce LabelExpr"
        self.val = [kids[0].val]

    def reduce_FqName_DOT_LabelExpr(self, *kids):
        "%reduce FqName DOT LabelExpr"
        self.val = kids[0].val + [kids[2].val]


class AnyFqName(Nonterm):
    def reduce_AnyLabelExpr(self, *kids):
        "%reduce AnyLabelExpr"
        self.val = [kids[0].val]

    def reduce_AnyFqName_DOT_AnyLabelExpr(self, *kids):
        "%reduce AnyFqName DOT AnyLabelExpr"
        self.val = kids[0].val + [kids[2].val]


class FqFuncName(Nonterm):
    def reduce_IDENT_DOUBLECOLON_IDENT(self, *kids):
        "%reduce IDENT DOUBLECOLON IDENT"
        self.val = (kids[0].val, kids[2].val)


class NsAliasName(Nonterm):
    def reduce_IDENT(self, *kids):
        "%reduce IDENT"
        self.val = kids[0].val

    def reduce_UnreservedKeyword(self, *kids):
        "%reduce UnreservedKeyword"
        self.val = kids[0].val


class LabelExpr(Nonterm):
    def reduce_PERCENT(self, *kids):
        "%reduce PERCENT"
        self.val = kids[0].val

    def reduce_IDENT(self, *kids):
        "%reduce IDENT"
        self.val = kids[0].val

    def reduce_UnreservedKeyword(self, *kids):
        "%reduce UnreservedKeyword"
        self.val = kids[0].val


class AnyLabelExpr(Nonterm):
    def reduce_LabelExpr(self, *kids):
        "%reduce LabelExpr"
        self.val = kids[0].val

    def reduce_ReservedKeyword(self, *kids):
        "%reduce ReservedKeyword"
        self.val = kids[0].val


class TypeName(Nonterm):
    def reduce_ArgName(self, *kids):
        "%reduce ArgName"
        self.val = kids[0].val

    def reduce_ArgName_LBRACKET_RBRACKET(self, *kids):
        "%reduce ArgName LBRACKET RBRACKET"
        self.val = (list, kids[0].val)


class ArgName(Nonterm):
    def reduce_SimpleArgName(self, *kids):
        "%reduce SimpleArgName"
        self.val = kids[0].val

    def reduce_LBRACKET_AnyFqName_RBRACKET(self, *kids):
        "%reduce LBRACKET AnyFqName RBRACKET"
        self.val = '.'.join(kids[1].val)


class SimpleArgName(Nonterm):
    def reduce_IDENT(self, *kids):
        "%reduce IDENT"
        self.val = kids[0].val

    def reduce_UnreservedKeyword(self, *kids):
        "%reduce UnreservedKeyword"
        self.val = kids[0].val

    def reduce_ReservedKeyword(self, *kids):
        "%reduce ReservedKeyword"
        self.val = kids[0].val


class ParamName(Nonterm):
    def reduce_IDENT(self, *kids):
        "%reduce IDENT"
        self.val = kids[0].val

    def reduce_UnreservedKeyword(self, *kids):
        "%reduce UnreservedKeyword"
        self.val = kids[0].val


class KeywordMeta(parsing.NontermMeta):
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


class UnreservedKeyword(Nonterm, metaclass=KeywordMeta, type=keywords.UNRESERVED_KEYWORD):
    pass


class ReservedKeyword(Nonterm, metaclass=KeywordMeta, type=keywords.RESERVED_KEYWORD):
    pass
