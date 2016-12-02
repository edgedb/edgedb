##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import types as pg_types
from edgedb.server.pgsql import exceptions as pg_errors

from edgedb.lang.common import ast, markup

from . import dbobj
from . import func


class IRCompilerError(pg_errors.BackendError):
    pass


class IRCompilerInternalError(IRCompilerError):
    pass


class IRCompilerErrorContext(markup.MarkupExceptionContext):
    title = 'EdgeDB PgSQL IR Compiler Error Context'

    def __init__(self, tree):
        super().__init__()
        self.tree = tree

    @classmethod
    def as_markup(cls, self, *, ctx):
        tree = markup.serialize(self.tree, ctx=ctx)
        return markup.elements.lang.ExceptionContext(
            title=self.title, body=[tree])


class IRCompilerBase(ast.visitor.NodeVisitor,
                     dbobj.IRCompilerDBObjects,
                     func.IRCompilerFunctionSupport):
    def __init__(self, **kwargs):
        self.context = None
        super().__init__(**kwargs)

    @property
    def memo(self):
        if self.context is not None:
            return self.context.current.memo
        else:
            return self._memo

    def generic_visit(self, node, *, combine_results=None):
        raise NotImplementedError(
            'no IR compiler handler for {}'.format(node.__class__))

    def visit_Constant(self, expr):
        ctx = self.context.current

        if expr.type and expr.type.name != 'std::null':
            const_type = self._schema_type_to_pg_type(expr.type)
        else:
            const_type = None

        if expr.expr:
            result = self.visit(expr.expr)
        else:
            val = expr.value
            index = None

            if expr.index is not None and not isinstance(expr.index, int):
                if expr.index in ctx.argmap:
                    index = list(ctx.argmap).index(expr.index)
                else:
                    ctx.argmap.add(expr.index)
                    index = len(ctx.argmap) - 1

                result = pgast.ParamRef(number=index)
            else:
                result = pgast.Constant(val=val)

        if const_type not in {None, 'bigint'}:
            result = pgast.TypeCast(
                arg=result,
                type_name=pgast.TypeName(
                    name=const_type
                )
            )

        return result

    def visit_TypeCast(self, expr):
        ctx = self.context.current

        if (isinstance(expr.expr, irast.BinOp) and
                isinstance(expr.expr.op,
                           (ast.ops.ComparisonOperator,
                            ast.ops.TypeCheckOperator))):
            expr_type = bool
        elif isinstance(expr.expr, irast.Constant):
            expr_type = expr.expr.type
        else:
            expr_type = None

        schema = ctx.schema

        pg_expr = self.visit(expr.expr)

        target_type = expr.type

        if target_type.subtypes:
            if target_type.maintype == 'list':
                elem_type = pg_types.pg_type_from_atom(
                    schema, schema.get(target_type.subtypes[0]), topbase=True)
                result = pgast.TypeCast(
                    arg=pg_expr,
                    type_name=pgast.TypeName(
                        name=elem_type,
                        array_bounds=[-1]))
            else:
                raise NotImplementedError(
                    '{} composite type is not supported '
                    'yet'.format(target_type.maintype))

        else:
            int_class = schema.get('std::int')
            target_type = schema.get(target_type.maintype)

            if (expr_type and expr_type is bool and
                    target_type.issubclass(int_class)):
                when_expr = pgast.CaseWhen(
                    expr=pg_expr, result=pgast.Constant(val=1))
                default = pgast.Constant(val=0)
                result = pgast.CaseExpr(
                    args=[when_expr], defresult=default)
            else:
                result = pgast.TypeCast(
                    arg=pg_expr,
                    type_name=pgast.TypeName(
                        name=pg_types.pg_type_from_atom(
                            schema, target_type, topbase=True)
                    )
                )

        return result

    def visit_IndexIndirection(self, expr):
        # Handle Expr[Index], where Expr may be std::str or array<T>.
        # For strings we translate this into substr calls, whereas
        # for arrays the native slice syntax is used.
        ctx = self.context.current

        is_string = False
        arg_type = irutils.infer_type(expr.expr, ctx.schema)

        subj = self.visit(expr.expr)
        index = self.visit(expr.index)

        if isinstance(arg_type, s_atoms.Atom):
            b = arg_type.get_topmost_base()
            is_string = b.name == 'std::str'

        one = pgast.Constant(val=1)
        zero = pgast.Constant(val=0)

        when_cond = self._new_binop(
            lexpr=index, rexpr=zero, op=ast.ops.LT)

        index_plus_one = self._new_binop(
            lexpr=index, op=ast.ops.ADD, rexpr=one)

        if is_string:
            upper_bound = pgast.FuncCall(
                name='char_length', args=[subj])
        else:
            upper_bound = pgast.FuncCall(
                name='array_upper', args=[subj, one])

        neg_off = self._new_binop(
            lexpr=upper_bound, rexpr=index_plus_one, op=ast.ops.ADD)

        when_expr = pgast.CaseWhen(
            expr=when_cond, result=neg_off)

        index = pgast.CaseExpr(
            args=[when_expr], defresult=index_plus_one)

        if is_string:
            result = pgast.FuncCall(
                name='substr',
                args=[subj, index, one]
            )
        else:
            indirection = pgast.Indices(ridx=index)
            result = pgast.Indirection(
                arg=subj, indirection=[indirection])

        return result

    def visit_SliceIndirection(self, expr):
        # Handle Expr[Start:End], where Expr may be std::str or array<T>.
        # For strings we translate this into substr calls, whereas
        # for arrays the native slice syntax is used.

        ctx = self.context.current

        subj = self.visit(expr.expr)
        start = self.visit(expr.start)
        stop = self.visit(expr.stop)
        one = pgast.Constant(val=1)
        zero = pgast.Constant(val=0)

        is_string = False
        arg_type = irutils.infer_type(expr.expr, ctx.schema)

        if isinstance(arg_type, s_atoms.Atom):
            b = arg_type.get_topmost_base()
            is_string = b.name == 'std::str'

        if is_string:
            upper_bound = pgast.FuncCall(
                name='char_length', args=[subj])
        else:
            upper_bound = pgast.FuncCall(
                name='array_upper', args=[subj, one])

        if isinstance(start, pgast.Constant) and start.val is None:
            lower = one
        else:
            lower = start

            when_cond = self._new_binop(
                lexpr=lower, rexpr=zero, op=ast.ops.LT)
            lower_plus_one = self._new_binop(
                lexpr=lower, rexpr=one, op=ast.ops.ADD)

            neg_off = self._new_binop(
                lexpr=upper_bound, rexpr=lower_plus_one, op=ast.ops.ADD)

            when_expr = pgast.CaseWhen(
                expr=when_cond, result=neg_off)
            lower = pgast.CaseExpr(
                args=[when_expr], defresult=lower_plus_one)

        if isinstance(stop, pgast.Constant) and stop.val is None:
            upper = upper_bound
        else:
            upper = stop

            when_cond = self._new_binop(
                lexpr=upper, rexpr=zero, op=ast.ops.LT)

            neg_off = self._new_binop(
                lexpr=upper_bound, rexpr=upper, op=ast.ops.ADD)

            when_expr = pgast.CaseWhen(
                expr=when_cond, result=neg_off)
            upper = pgast.CaseExpr(
                args=[when_expr], defresult=upper)

        if is_string:
            args = [subj, lower]

            if upper is not upper_bound:
                for_length = self._new_binop(
                    lexpr=upper, op=ast.ops.SUB, rexpr=lower)
                for_length = self._new_binop(
                    lexpr=for_length, op=ast.ops.ADD, rexpr=one)
                args.append(for_length)

            result = pgast.FuncCall(name='substr', args=args)

        else:
            indirection = pgast.Indices(
                lidx=lower, ridx=upper)
            result = pgast.Indirection(
                arg=subj, indirection=[indirection])

        return result

    def visit_BinOp(self, expr):
        ctx = self.context.current

        with self.context.new():
            op = expr.op
            if ctx.location == 'set_expr' and op in {ast.ops.AND, ast.ops.OR}:
                self.context.current.location = 'exists'

            if isinstance(expr.op, ast.ops.TypeCheckOperator):
                cl = self._get_ptr_set(expr.left, 'std::__class__')
                left = self.visit(self._get_ptr_set(cl, 'std::id'))
            else:
                left = self.visit(expr.left)

            if expr.op in (ast.ops.IN, ast.ops.NOT_IN) \
                    and isinstance(expr.right, irast.Sequence):
                with self.context.new():
                    self.context.current.sequence_is_array = True
                    right = self.visit(expr.right)
            else:
                right = self.visit(expr.right)

        if isinstance(expr.op, ast.ops.TypeCheckOperator):
            result = pgast.FuncCall(
                name='edgedb.issubclass',
                args=[left, right])

            if expr.op == ast.ops.IS_NOT:
                result = self._new_unop(ast.ops.NOT, result)

        else:
            if (expr.op in (ast.ops.IN, ast.ops.NOT_IN) and
                    isinstance(expr.right, irast.Constant)):
                # "expr IN $CONST" translates into
                # "expr = any($CONST)" and
                # "expr NOT IN $CONST" translates into
                # "expr != all($CONST)"
                if expr.op == ast.ops.IN:
                    op = ast.ops.EQ
                    qual_func = 'any'
                else:
                    op = ast.ops.NE
                    qual_func = 'all'

                if isinstance(right.expr, pgast.Sequence):
                    right.expr = pgast.ArrayExpr(
                        elements=right.expr.elements)
                elif right.type == 'text[]':
                    left_type = irutils.infer_type(
                        expr.left, ctx.schema)
                    if isinstance(left_type, s_obj.Class):
                        if isinstance(left_type, s_concepts.Concept):
                            left_type = left_type.pointers[
                                'std::id'].target
                        left_type = pg_types.pg_type_from_atom(
                            ctx.schema, left_type,
                            topbase=True)
                        right.type = left_type + '[]'

                right = pgast.FuncCall(
                    name=qual_func, args=[right])
            else:
                op = expr.op

            left_type = irutils.infer_type(
                expr.left, ctx.schema)
            right_type = irutils.infer_type(
                expr.right, ctx.schema)

            if left_type and right_type:
                if isinstance(left_type, s_obj.Class):
                    if isinstance(left_type, s_concepts.Concept):
                        left_type = left_type.pointers[
                            'std::id'].target
                    left_type = pg_types.pg_type_from_atom(
                        ctx.schema, left_type,
                        topbase=True)
                elif (
                        not isinstance(
                            left_type, s_obj.Class) and
                        (not isinstance(left_type, tuple) or
                         not isinstance(
                            left_type[1], s_obj.Class))):
                    left_type = self._schema_type_to_pg_type(
                        left_type)

                if isinstance(right_type, s_obj.Class):
                    if isinstance(right_type, s_concepts.Concept):
                        right_type = right_type.pointers[
                            'std::id'].target
                    right_type = pg_types.pg_type_from_atom(
                        ctx.schema, right_type,
                        topbase=True)
                elif (
                        not isinstance(
                            right_type, s_obj.Class) and
                        (not isinstance(right_type, tuple) or
                         not isinstance(
                            right_type[1], s_obj.Class))):
                    right_type = self._schema_type_to_pg_type(
                        right_type)

                if (
                        left_type in ('text', 'varchar') and
                        right_type in ('text', 'varchar') and
                        op == ast.ops.ADD):
                    op = '||'
                elif left_type != right_type:
                    if isinstance(
                            right, pgast.
                            Constant) and right_type == 'text':
                        right.type = left_type
                    elif isinstance(
                            left, pgast.
                            Constant) and left_type == 'text':
                        left.type = right_type

                if (isinstance(right, pgast.Constant) and
                        op in {ast.ops.IS, ast.ops.IS_NOT}):
                    right.type = None

            result = self._new_binop(left, right, op=op)

        return result

    def visit_UnaryOp(self, expr):
        operand = self.visit(expr.expr)
        return pgast.Expr(name=expr.op, rexpr=operand, kind=pgast.ExprKind.OP)

    def visit_IfElseExpr(self, expr):
        return pgast.CaseExpr(
            args=[
                pgast.CaseWhen(
                    expr=self.visit(expr.condition),
                    result=self.visit(expr.if_expr))
            ],
            defresult=self.visit(expr.else_expr))

    def visit_Sequence(self, expr):
        ctx = self.context.current

        elements = [
            self.visit(e) for e in expr.elements
        ]
        if expr.is_array:
            result = pgast.ArrayExpr(elements=elements)
        elif getattr(ctx, 'sequence_is_array', False):
            result = pgast.ImplicitRowExpr(args=elements)
        else:
            result = pgast.RowExpr(args=elements)

        return result

    def visit_TypeRef(self, expr):
        ctx = self.context.current

        data_backend = ctx.backend
        schema = ctx.schema

        if expr.subtypes:
            raise NotImplementedError()
        else:
            cls = schema.get(expr.maintype)
            concept_id = data_backend.get_concept_id(cls)
            result = pgast.TypeCast(
                arg=pgast.Constant(val=concept_id),
                type_name=pgast.TypeName(
                    name='uuid'
                )
            )

        return result

    def _new_binop(self, lexpr, rexpr, op):
        return pgast.Expr(
            kind=pgast.ExprKind.OP,
            name=op,
            lexpr=lexpr,
            rexpr=rexpr
        )

    def _extend_binop(self, binop, *exprs, op=ast.ops.AND, reversed=False):
        exprs = list(exprs)
        binop = binop or exprs.pop(0)

        for expr in exprs:
            if expr is not binop:
                if reversed:
                    binop = self._new_binop(rexpr=binop, op=op, lexpr=expr)
                else:
                    binop = self._new_binop(lexpr=binop, op=op, rexpr=expr)

        return binop

    def _new_unop(self, op, expr):
        return pgast.Expr(
            kind=pgast.ExprKind.OP,
            name=op,
            rexpr=expr
        )

    def _get_ptr_set(self, source_set, ptr_name):
        ctx = self.context.current

        schema = ctx.schema
        scls = source_set.scls
        ptrcls = scls.resolve_pointer(schema, ptr_name)

        path_id = irutils.LinearPath(source_set.path_id)
        path_id.add(ptrcls, s_pointers.PointerDirection.Outbound,
                    ptrcls.target)

        target_set = irast.Set()
        target_set.scls = ptrcls.target
        target_set.path_id = path_id

        ptr = irast.Pointer(
            source=source_set,
            target=target_set,
            ptrcls=ptrcls,
            direction=s_pointers.PointerDirection.Outbound
        )

        target_set.rptr = ptr

        return target_set
