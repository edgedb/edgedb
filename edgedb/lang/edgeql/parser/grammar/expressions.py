##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast
from edgedb.lang.common import parsing, context

from edgedb.lang.edgeql import ast as qlast

from ...errors import EdgeQLSyntaxError

from . import keywords

from .precedence import *  # NOQA
from .tokens import *  # NOQA


class Nonterm(context.Nonterm):
    pass


class ListNonterm(context.ListNonterm, element=None):
    pass


class SetStmt(Nonterm):
    # SelectNoParens is used instead of SelectExpr to avoid
    # a conflict with a Stmt|Expr production, as Expr contains a
    # SelectWithParens production.  Additionally, this is consistent
    # with other statement productions in disallowing parentheses.
    #
    def reduce_SelectNoParens(self, *kids):
        self.val = kids[0].val

    def reduce_InsertExpr(self, *kids):
        self.val = kids[0].val

    def reduce_UpdateExpr(self, *kids):
        self.val = kids[0].val

    def reduce_DeleteExpr(self, *kids):
        self.val = kids[0].val

    def reduce_ValuesExpr(self, *kids):
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
        qry.aliases = kids[0].val
        qry.orderby = kids[2].val
        qry.offset = kids[3].val[0]
        qry.limit = kids[3].val[1]
        self.val = qry

    def reduce_SimpleSelect_OptSortClause_OptSelectLimit(self, *kids):
        qry = kids[0].val
        qry.orderby = kids[1].val
        qry.offset = kids[2].val[0]
        qry.limit = kids[2].val[1]
        self.val = qry

    # SelectWithParens MUST have a non optional clause here to avoid loops
    #
    def reduce_SelectWithParens_SelectLimit(self, *kids):
        qry = kids[0].val
        qry.offset = kids[1].val[0]
        qry.limit = kids[1].val[1]
        self.val = qry

    def reduce_SelectWithParens_SortClause_OptSelectLimit(self, *kids):
        qry = kids[0].val
        qry.orderby = kids[1].val
        qry.offset = kids[2].val[0]
        qry.limit = kids[2].val[1]
        self.val = qry


class SelectClause(Nonterm):
    def reduce_SimpleSelect(self, *kids):
        self.val = kids[0].val

    def reduce_SelectWithParens(self, *kids):
        self.val = kids[0].val


class OptSingle(Nonterm):
    def reduce_SINGLETON(self, *kids):
        self.val = True

    def reduce_empty(self):
        self.val = False


class SimpleSelect(Nonterm):
    def reduce_Select(self, *kids):
        r"%reduce SELECT OptSingle SelectTargetEl \
                  OptWhereClause OptGroupClause OptHavingClause"
        self.val = qlast.SelectQueryNode(
            single=kids[1].val,
            # XXX: for historical reasons a list is expected here by
            # the compiler
            #
            targets=[kids[2].val],
            where=kids[3].val,
            groupby=kids[4].val,
            having=kids[5].val,
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


class InsertExpr(Nonterm):
    def reduce_OptAliasBlock_INSERT_Path_OptReturningClause(self, *kids):
        single, targets = kids[3].val
        self.val = qlast.InsertQueryNode(
            aliases=kids[0].val,
            subject=kids[2].val,
            single=single,
            targets=targets,
        )

    def reduce_OptAliasBlock_INSERT_TypedShape_OptReturningClause(self, *kids):
        pathspec = kids[2].val.pathspec
        kids[2].val.pathspec = None
        single, targets = kids[3].val
        self.val = qlast.InsertQueryNode(
            aliases=kids[0].val,
            subject=kids[2].val,
            pathspec=pathspec,
            single=single,
            targets=targets,
        )

    def reduce_InsertFrom(self, *kids):
        r'%reduce OptAliasBlock INSERT TypedShape FROM FromClause \
          OptReturningClause'
        pathspec = kids[2].val.pathspec
        kids[2].val.pathspec = None
        single, targets = kids[5].val
        self.val = qlast.InsertQueryNode(
            aliases=kids[0].val,
            subject=kids[2].val,
            pathspec=pathspec,
            single=single,
            targets=targets,
            source=kids[4].val,
        )


class FromClause(Nonterm):
    def reduce_SelectWithParens(self, *kids):
        self.val = kids[0].val

    def reduce_ParenthesisedStmt(self, *kids):
        self.val = kids[0].val

    def reduce_ValuesExpr(self, *kids):
        self.val = kids[0].val

    def reduce_SelectNoParens(self, *kids):
        self.val = kids[0].val


class ParenthesisedStmt(Nonterm):
    def reduce_LPAREN_InsertExpr_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_UpdateExpr_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_DeleteExpr_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_ParenthesisedStmt_RPAREN(self, *kids):
        self.val = kids[1].val


class UpdateExpr(Nonterm):
    def reduce_UpdateExpr(self, *kids):
        r"%reduce OptAliasBlock UPDATE TypedShape \
                  OptWhereClause OptReturningClause"
        pathspec = kids[2].val.pathspec
        kids[2].val.pathspec = None
        single, targets = kids[4].val
        self.val = qlast.UpdateQueryNode(
            aliases=kids[0].val,
            subject=kids[2].val,
            pathspec=pathspec,
            where=kids[3].val,
            single=single,
            targets=targets,
        )


class DeleteExpr(Nonterm):
    def reduce_DeleteExpr(self, *kids):
        "%reduce OptAliasBlock DELETE Path OptWhereClause OptReturningClause"
        single, targets = kids[4].val
        self.val = qlast.DeleteQueryNode(
            aliases=kids[0].val,
            subject=kids[2].val,
            where=kids[3].val,
            single=single,
            targets=targets,
        )


class ValuesExpr(Nonterm):
    def reduce_ValuesExpr(self, *kids):
        "%reduce OptAliasBlock VALUES ValueTargetList \
                 OptSortClause OptSelectLimit"
        self.val = qlast.ValuesQueryNode(
            aliases=kids[0].val,
            targets=kids[2].val,
            orderby=kids[3].val,
            offset=kids[4].val[0],
            limit=kids[4].val[1],
        )


class ValueTarget(Nonterm):
    def reduce_Expr(self, *kids):
        self.val = qlast.SelectExprNode(expr=kids[0].val)


class ValueTargetList(ListNonterm, element=ValueTarget, separator=T_COMMA):
    pass


class OptAliasBlock(Nonterm):
    def reduce_AliasBlock(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


class AliasBlock(Nonterm):
    def reduce_WITH_AliasDeclList(self, *kids):
        self.val = kids[1].val


class ModuleName(Nonterm):
    def reduce_IDENT(self, *kids):
        self.val = [kids[0].val]


class AliasDecl(Nonterm):
    def reduce_MODULE_ModuleName(self, *kids):
        self.val = qlast.NamespaceAliasDeclNode(
            namespace='.'.join(kids[1].val))

    def reduce_ShortName_TURNSTILE_MODULE_ModuleName(self, *kids):
        self.val = qlast.NamespaceAliasDeclNode(
            alias=kids[0].val,
            namespace='.'.join(kids[3].val))

    def reduce_ShortName_TURNSTILE_DETACHED_Path(self, *kids):
        self.val = qlast.DetachedPathDeclNode(
            alias=kids[0].val,
            expr=kids[3].val)

    def reduce_ShortName_TURNSTILE_Expr(self, *kids):
        # NOTE: if Expr is a subquery, we need to treat it specially
        if isinstance(kids[2].val, qlast.SelectQueryNode):
            self.val = qlast.CGENode(
                alias=kids[0].val,
                expr=kids[2].val)
        else:
            self.val = qlast.ExpressionAliasDeclNode(
                alias=kids[0].val,
                expr=kids[2].val)


class AliasDeclList(ListNonterm, element=AliasDecl, separator=T_COMMA):
    pass


class SelectTargetEl(Nonterm):
    def reduce_Expr(self, *kids):
        self.val = qlast.SelectExprNode(expr=kids[0].val)

    def reduce_Expr_Shape(self, *kids):
        tshape = kids[0].val
        if (not isinstance(tshape, qlast.PathNode) or
                tshape.pathspec):
            raise EdgeQLSyntaxError('unexpected shape',
                                    context=kids[1].context)

        tshape.pathspec = kids[1].val
        self.val = qlast.SelectExprNode(expr=tshape)

    def reduce_Shape(self, *kids):
        # anonymous shape
        #
        self.val = qlast.SelectExprNode(
            expr=qlast.PathNode(pathspec=kids[0].val))


class Shape(Nonterm):
    def reduce_LBRACE_ShapeElementList_RBRACE(self, *kids):
        self.val = kids[1].val

    def reduce_LBRACE_ShapeElementList_COMMA_RBRACE(self, *kids):
        self.val = kids[1].val


class TypedShape(Nonterm):
    def reduce_NodeName_Shape(self, *kids):
        self.val = qlast.PathNode(
            steps=[qlast.PathStepNode(expr=kids[0].val.name,
                                      namespace=kids[0].val.module)],
            pathspec=kids[1].val)


class OptAnySubShape(Nonterm):
    def reduce_COLON_Shape(self, *kids):
        self.val = kids[1].val

    def reduce_COLON_TypedShape(self, *kids):
        # typed shape needs to be transformed here into a different
        # expression
        shape = kids[1].val
        name = shape.steps[0]
        typenode = qlast.ClassRefNode(name=name.expr,
                                          module=name.namespace,
                                          context=name.context)
        self.val = [typenode] + shape.pathspec

    def reduce_empty(self, *kids):
        self.val = None


class OptShape(Nonterm):
    def reduce_Shape(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self):
        self.val = None


class ShapeElement(Nonterm):
    def reduce_ShapeElementWithSubShape(self, *kids):
        r"""%reduce ShapePointer OptPointerRecursionSpec \
             OptAnySubShape OptWhereClause OptSortClause OptSelectLimit \
        """
        self.val = kids[0].val
        if kids[1].val is True:
            self.val.recurse = True
            self.val.recurse_limit = None
        elif kids[1].val is not None:
            self.val.recurse = True
            self.val.recurse_limit = kids[1].val

        shape = kids[2].val
        if shape and isinstance(shape[0], qlast.ClassRefNode):
            self.val.expr.steps[-1].expr.target = shape[0]
            self.val.pathspec = shape[1:]
        else:
            self.val.pathspec = shape
        self.val.where = kids[3].val
        self.val.orderby = kids[4].val
        self.val.offset = kids[5].val[0]
        self.val.limit = kids[5].val[1]

    def reduce_ShapePointer_TURNSTILE_Expr_OptShape(self, *kids):
        self.val = kids[0].val
        self.val.compexpr = kids[2].val
        self.val.pathspec = kids[3].val


class ShapeElementList(ListNonterm, element=ShapeElement, separator=T_COMMA):
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
        kids[0].val.direction = s_pointers.PointerDirection.Outbound
        self.val = qlast.PathNode(
            steps=[qlast.LinkExprNode(expr=kids[0].val)]
        )

    def reduce_LinkDirection_ShapePathPtr(self, *kids):
        kids[1].val.direction = kids[0].val
        self.val = qlast.PathNode(
            steps=[qlast.LinkExprNode(expr=kids[1].val)]
        )

    def reduce_AT_ShapePathPtr(self, *kids):
        kids[1].val.type = 'property'
        self.val = qlast.PathNode(
            steps=[qlast.LinkPropExprNode(expr=kids[1].val)]
        )

    def reduce_ShapePathStart_AT_ShapePathPtr(self, *kids):
        kids[2].val.type = 'property'
        self.val = qlast.PathNode(
            steps=[kids[0].val, qlast.LinkPropExprNode(expr=kids[2].val)]
        )

    def reduce_ShapePathStart_DOT_ShapePathPtr(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers
        kids[2].val.direction = s_pointers.PointerDirection.Outbound
        self.val = qlast.PathNode(
            steps=[kids[0].val, qlast.LinkExprNode(expr=kids[2].val)]
        )

    def reduce_ShapePathStart_DOTFW_ShapePathPtr(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers
        kids[2].val.direction = s_pointers.PointerDirection.Outbound
        self.val = qlast.PathNode(
            steps=[kids[0].val, qlast.LinkExprNode(expr=kids[2].val)]
        )

    def reduce_ShapePathStart_DOTBW_ShapePathPtr(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers
        kids[2].val.direction = s_pointers.PointerDirection.Inbound
        self.val = qlast.PathNode(
            steps=[kids[0].val, qlast.LinkExprNode(expr=kids[2].val)]
        )


class ShapePathStart(Nonterm):
    def reduce_NodeName(self, *kids):
        self.val = qlast.PathStepNode(
            expr=kids[0].val.name, namespace=kids[0].val.module)

    def reduce_NodeNameParens(self, *kids):
        self.val = qlast.PathStepNode(
            expr=kids[0].val.name, namespace=kids[0].val.module)


class ShapePathPtr(Nonterm):
    def reduce_NodeName(self, *kids):
        self.val = qlast.LinkNode(name=kids[0].val.name,
                                  namespace=kids[0].val.module)

    def reduce_NodeNameParens(self, *kids):
        self.val = qlast.LinkNode(name=kids[0].val.name,
                                  namespace=kids[0].val.module)


class ShapePointer(Nonterm):
    def reduce_ShapePath(self, *kids):
        self.val = qlast.SelectPathSpecNode(
            expr=kids[0].val
        )


class OptPointerRecursionSpec(Nonterm):
    def reduce_STAR(self, *kids):
        self.val = True

    def reduce_STAR_NumberConstant(self, *kids):
        self.val = kids[1].val

    def reduce_empty(self, *kids):
        self.val = None


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


class HavingClause(Nonterm):
    def reduce_HAVING_Expr(self, *kids):
        self.val = kids[1].val


class OptHavingClause(Nonterm):
    def reduce_HavingClause(self, *kids):
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
        self.val = None


class OrderbyExpr(Nonterm):
    def reduce_Expr_OptDirection_OptNonesOrder(self, *kids):
        self.val = qlast.SortExprNode(path=kids[0].val,
                                      direction=kids[1].val,
                                      nones_order=kids[2].val)


class OrderbyList(ListNonterm, element=OrderbyExpr, separator=T_THEN):
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
    def reduce_NULLS_FIRST(self, *kids):
        self.val = qlast.NonesFirst

    def reduce_NULLS_LAST(self, *kids):
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


class ParenExpr(Nonterm):
    def reduce_LPAREN_Expr_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_OpPath_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_InsertExpr_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_UpdateExpr_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_DeleteExpr_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_ValuesExpr_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_Expr_AS_TypeName_RPAREN(self, *kids):
        # LHS has to be a Path, also the result of this operation is
        # also a Path.
        #
        self.val = qlast.PathNode(
            steps=[qlast.TypeInterpretationNode(expr=kids[1].val,
                                                type=kids[3].val)])


class Expr(Nonterm):
    # Path | Constant | '(' Expr ')' | FuncExpr | Sequence | Mapping
    # | Array
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
    # | Expr '[' TO NodeName ']'
    # | '<' ExtTypeExpr '>' '(' Expr ')'
    # | '(' Expr AS Expr ')'
    # | Expr IF Expr ELSE Expr

    def reduce_Path(self, *kids):
        self.val = kids[0].val

    def reduce_Constant(self, *kids):
        self.val = kids[0].val

    def reduce_ParenExpr(self, *kids):
        self.val = kids[0].val

    def reduce_Expr_IndirectionEl(self, *kids):
        expr = kids[0].val
        if isinstance(expr, qlast.IndirectionNode):
            self.val = expr
            expr.indirection.append(kids[1].val)
        else:
            self.val = qlast.IndirectionNode(arg=expr,
                                             indirection=[kids[1].val])

    def reduce_Expr_LBRACKET_TO_OpNodeName_RBRACKET(self, *kids):
        # The path filter rule is here to resolve ambiguity with
        # indexes and slices, so Expr needs to be enforced as a path
        # with one or more steps.
        #
        # NOTE: We specifically disallow "Foo.(bar[TO Baz])"" because
        # it is incorrect logical grouping. The example where the
        # incorrect grouping is more obvious is: "Foo.<(bar[TO Baz])"
        #
        path = kids[0].val
        if (not isinstance(path, qlast.PathNode)
                or not isinstance(path.steps[-1], qlast.LinkExprNode)
                or not isinstance(path.steps[-1].expr, qlast.LinkNode)):
            raise EdgeQLSyntaxError('Unexpected token: {}'.format(kids[2]),
                                    context=kids[2].context)

        path.steps[-1].expr.target = kids[3].val
        self.val = path

    def reduce_FuncExpr(self, *kids):
        self.val = kids[0].val

    @parsing.precedence(P_UMINUS)
    def reduce_SelectWithParens(self, *kids):
        self.val = kids[0].val

    @parsing.precedence(P_UMINUS)
    def reduce_EXISTS_Expr(self, *kids):
        self.val = qlast.ExistsPredicateNode(expr=kids[1].val)

    def reduce_Sequence(self, *kids):
        self.val = kids[0].val

    def reduce_Array(self, *kids):
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

    def reduce_Expr_CIRCUMFLEX_Expr(self, *kids):
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
            op = qlast.SEARCH
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
        self.val = qlast.BinOpNode(left=kids[0].val, op=qlast.LIKE,
                                   right=kids[2].val)

    def reduce_Expr_NOT_LIKE_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=qlast.NOT_LIKE,
                                   right=kids[3].val)

    def reduce_Expr_ILIKE_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=qlast.ILIKE,
                                   right=kids[2].val)

    def reduce_Expr_NOT_ILIKE_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=qlast.NOT_ILIKE,
                                   right=kids[3].val)

    def reduce_Expr_IS_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.IS,
                                   right=kids[2].val)

    @parsing.precedence(P_IS)
    def reduce_Expr_IS_NOT_Expr(self, *kids):
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.IS_NOT,
                                   right=kids[3].val)

    def reduce_Expr_IN_Expr(self, *kids):
        inexpr = kids[2].val
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.IN,
                                   right=inexpr)

    @parsing.precedence(P_IN)
    def reduce_Expr_NOT_IN_Expr(self, *kids):
        inexpr = kids[3].val
        self.val = qlast.BinOpNode(left=kids[0].val, op=ast.ops.NOT_IN,
                                   right=inexpr)

    @parsing.precedence(P_TYPECAST)
    def reduce_LANGBRACKET_ExtTypeExpr_RANGBRACKET_Expr(
            self, *kids):
        self.val = qlast.TypeCastNode(expr=kids[3].val, type=kids[1].val)

    def reduce_Expr_IF_Expr_ELSE_Expr(self, *kids):
        self.val = qlast.IfElseNode(
            if_expr=kids[0].val, condition=kids[2].val, else_expr=kids[4].val)


class Sequence(Nonterm):
    def reduce_LPAREN_Expr_COMMA_OptExprList_RPAREN(self, *kids):
        self.val = qlast.SequenceNode(elements=[kids[1].val] + kids[3].val)


class Array(Nonterm):
    def reduce_LBRACKET_OptExprList_RBRACKET(self, *kids):
        self.val = qlast.ArrayNode(elements=kids[1].val)


class Mapping(Nonterm):
    def reduce_LBRACE_MappingElementsList_RBRACE(self, *kids):
        self.val = qlast.MappingNode(items=kids[1].val)

    def reduce_LBRACE_MappingElementsList_COMMA_RBRACE(self, *kids):
        self.val = qlast.MappingNode(items=kids[1].val)


class MappingElement(Nonterm):
    def reduce_SCONST_COLON_Expr(self, *kids):
        self.val = (qlast.ConstantNode(value=kids[0].val), kids[2].val)


class MappingElementsList(ListNonterm, element=MappingElement,
                          separator=T_COMMA):
    pass


class OptExprList(Nonterm):
    def reduce_ExprList_COMMA(self, *kids):
        self.val = kids[0].val

    def reduce_ExprList(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


class ExprList(ListNonterm, element=Expr, separator=T_COMMA):
    pass


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
    def reduce_NULL(self, *kids):
        self.val = qlast.ConstantNode(value=None)


class ArgConstant(Nonterm):
    def reduce_DOLLAR_ICONST(self, *kids):
        self.val = qlast.ConstantNode(value=None, index=int(kids[1].val))

    def reduce_DOLLAR_ShortName(self, *kids):
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


# this is used inside parentheses or in shapes
#
class OpPath(Nonterm):
    @parsing.precedence(P_PATHSTART)
    def reduce_OpOnlyNodeName(self, *kids):
        self.val = qlast.PathNode(
            steps=[qlast.PathStepNode(expr=kids[0].val.name,
                                      namespace=kids[0].val.module)])


class Path(Nonterm):
    @parsing.precedence(P_PATHSTART)
    def reduce_NodeName(self, *kids):
        self.val = qlast.PathNode(
            steps=[qlast.PathStepNode(expr=kids[0].val.name,
                                      namespace=kids[0].val.module)])

    @parsing.precedence(P_DOT)
    def reduce_Expr_PathStep(self, *kids):
        path = kids[0].val
        if not isinstance(path, qlast.PathNode):
            raise EdgeQLSyntaxError('illegal path node',
                                    context=kids[1].val.context)

        path.steps.append(kids[1].val)
        self.val = path


class PathStep(Nonterm):
    def reduce_DOT_PathPtr(self, *kids):
        self.val = qlast.LinkExprNode(expr=kids[1].val)
        from edgedb.lang.schema import pointers as s_pointers
        kids[1].val.direction = s_pointers.PointerDirection.Outbound

    def reduce_DOTFW_PathPtr(self, *kids):
        self.val = qlast.LinkExprNode(expr=kids[1].val)
        from edgedb.lang.schema import pointers as s_pointers
        kids[1].val.direction = s_pointers.PointerDirection.Outbound

    def reduce_DOTBW_PathPtr(self, *kids):
        self.val = qlast.LinkExprNode(expr=kids[1].val)
        from edgedb.lang.schema import pointers as s_pointers
        kids[1].val.direction = s_pointers.PointerDirection.Inbound

    def reduce_AT_PathPtr(self, *kids):
        self.val = qlast.LinkPropExprNode(expr=kids[1].val)
        kids[1].val.type = 'property'


class PathPtr(Nonterm):
    def reduce_ShortOpNodeName(self, *kids):
        self.val = qlast.LinkNode(name=kids[0].val.name,
                                  namespace=kids[0].val.module)

    def reduce_PathPtrParen(self, *kids):
        self.val = kids[0].val


class PathPtrParen(Nonterm):
    def reduce_LPAREN_PathPtrParen_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_OpNodeName_RPAREN(self, *kids):
        self.val = qlast.LinkNode(name=kids[1].val.name,
                                  namespace=kids[1].val.module)


class LinkDirection(Nonterm):
    def reduce_LANGBRACKET(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers
        self.val = s_pointers.PointerDirection.Inbound

    def reduce_RANGBRACKET(self, *kids):
        from edgedb.lang.schema import pointers as s_pointers
        self.val = s_pointers.PointerDirection.Outbound


class FuncApplication(Nonterm):
    def reduce_FuncApplication(self, *kids):
        r"""%reduce NodeName LPAREN OptFuncArgList OptWhereClause \
                    OptSortClause RPAREN \
        """
        module = kids[0].val.module
        func_name = kids[0].val.name
        args = kids[2].val
        name = func_name if not module else (module, func_name)
        self.val = qlast.FunctionCallNode(func=name, args=args,
                                          agg_filter=kids[3].val,
                                          agg_sort=kids[4].val)


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

    def reduce_ParamName_TURNSTILE_Expr(self, *kids):
        self.val = qlast.NamedArgNode(name=kids[0].val, arg=kids[2].val)


class FuncArgList(ListNonterm, element=FuncArgExpr, separator=T_COMMA):
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


class ShortName(Nonterm):
    def reduce_IDENT(self, *kids):
        self.val = kids[0].val


# this can appear anywhere
#
class BaseName(Nonterm):
    def reduce_IDENT(self, *kids):
        self.val = [kids[0].val]

    def reduce_IDENT_DOUBLECOLON_AnyIdentifier(self, *kids):
        # the identifier following a '::' cannot start with '@'
        #
        if kids[2].val[0] == '@':
            raise EdgeQLSyntaxError("name cannot start with '@'",
                                    context=kids[2].context)

        self.val = [kids[0].val, kids[2].val]


# this can appear anywhere after parentheses or operator
#
class OpName(Nonterm):
    def reduce_UnreservedKeyword(self, *kids):
        self.val = [kids[0].val]

    def reduce_UnreservedKeyword_DOUBLECOLON_AnyIdentifier(self, *kids):
        # the identifier following a '::' cannot start with '@'
        #
        if kids[2].val[0] == '@':
            raise EdgeQLSyntaxError("name cannot start with '@'",
                                    context=kids[2].context)

        self.val = [kids[0].val, kids[2].val]


class TypeName(Nonterm):
    def reduce_NodeName(self, *kids):
        self.val = qlast.TypeNameNode(maintype=kids[0].val)

    def reduce_NodeName_LANGBRACKET_NodeNameList_RANGBRACKET(self, *kids):
        self.val = qlast.TypeNameNode(maintype=kids[0].val,
                                      subtypes=kids[2].val)


class ExtTypeExpr(Nonterm):
    def reduce_TypeName(self, *kids):
        self.val = kids[0].val

    def reduce_TypedShape(self, *kids):
        self.val = qlast.TypeNameNode(maintype=kids[0].val)


class ParamName(Nonterm):
    def reduce_IDENT(self, *kids):
        self.val = kids[0].val

    def reduce_UnreservedKeyword(self, *kids):
        self.val = kids[0].val


class NodeName(Nonterm):
    # NOTE: Generic short of fully-qualified name.
    #
    # This name is safe to be used anywhere as it starts with IDENT only.

    def reduce_BaseName(self, *kids):
        # NodeName must not start with a '@' in any way
        #
        if kids[0].val[-1][0] == '@':
            raise EdgeQLSyntaxError("name cannot start with '@'",
                                    context=kids[0].context)
        self.val = qlast.ClassRefNode(
            module='.'.join(kids[0].val[:-1]) or None,
            name=kids[0].val[-1])


class NodeNameList(ListNonterm, element=NodeName, separator=T_COMMA):
    pass


class ShortOpNodeName(Nonterm):
    # NOTE: A non-qualified name that can be ANY identifier.
    #
    # This name is used as part of paths after the DOT. It can be any
    # identifier including RESERVED_KEYWORD and UNRESERVED_KEYWORD and
    # does not need to be quoted or parenthesized.

    def reduce_Identifier(self, *kids):
        # ShortOpNodeName cannot start with a '@' in any way
        #
        if kids[0].val[0] == '@':
            raise EdgeQLSyntaxError("name cannot start with '@'",
                                    context=kids[0].context)
        self.val = qlast.ClassRefNode(
            module=None,
            name=kids[0].val)


class OpOnlyNodeName(Nonterm):
    # NOTE: A name that starts with UNRESERVED_KEYWORD.
    #
    # It can only be used inside parentheses or after a DOT. In
    # principle, we may extend this to be used after any operator in the
    # future.

    def reduce_OpName(self, *kids):
        # OpNodeName cannot start with a '@' in any way already
        #
        self.val = qlast.ClassRefNode(
            module='.'.join(kids[0].val[:-1]) or None,
            name=kids[0].val[-1])


class OpNodeName(Nonterm):
    # NOTE: Generic short of fully-qualified name that must be parenthesized.
    #
    # This name is safe to be used anywhere within parentheses. If it
    # causes ambiguity, it may be necessary to use OpOnlyNodeName instead.

    def reduce_NodeName(self, *kids):
        self.val = kids[0].val

    def reduce_OpOnlyNodeName(self, *kids):
        self.val = kids[0].val


class NodeNameParens(Nonterm):
    # NOTE: Arbitrarily parenthesized name.
    #
    # This is used in shapes.

    def reduce_LPAREN_NodeNameParens_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_LPAREN_OpNodeName_RPAREN(self, *kids):
        self.val = kids[1].val


class KeywordMeta(context.ContextNontermMeta):
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


class ReturningClause(Nonterm):
    def reduce_RETURNING_OptSingle_SelectTargetEl(self, *kids):
        # XXX: for historical reasons a list is expected here by the compiler
        #
        self.val = [kids[1].val, [kids[2].val]]


class OptReturningClause(Nonterm):
    def reduce_ReturningClause(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = [False, None]
