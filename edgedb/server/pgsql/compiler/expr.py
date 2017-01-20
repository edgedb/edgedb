##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms
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
        return self.context.current.memo

    def generic_visit(self, node, *, combine_results=None):
        raise NotImplementedError(
            'no IR compiler handler for {}'.format(node.__class__))

    def _maybe_cast(self, node, typ):
        if typ.name == 'std::null':
            return node

        ctx = self.context.current
        const_type = pg_types.pg_type_from_object(
            ctx.schema, typ, topbase=True)

        node = pgast.TypeCast(
            arg=node,
            type_name=pgast.TypeName(
                name=const_type
            )
        )

        return node

    def visit_Parameter(self, expr):
        ctx = self.context.current

        if expr.name.isnumeric():
            index = int(expr.name)
        else:
            if expr.name in ctx.argmap:
                index = list(ctx.argmap).index(expr.name)
            else:
                ctx.argmap.add(expr.name)
                index = len(ctx.argmap)

        result = pgast.ParamRef(number=index)
        return self._maybe_cast(result, expr.type)

    def visit_Constant(self, expr):
        result = pgast.Constant(val=expr.value)
        result = self._maybe_cast(result, expr.type)
        return result

    def visit_TypeCast(self, expr):
        ctx = self.context.current
        schema = ctx.schema

        pg_expr = self.visit(expr.expr)
        target_type = expr.type

        if target_type.subtypes:
            if target_type.maintype == 'array':
                # EdgeQL: SELECT <array<int>>['1', '2']
                # to SQL: SELECT ARRAY['1', '2']::int[]

                elem_type = pg_types.pg_type_from_atom(
                    schema, schema.get(target_type.subtypes[0]), topbase=True)
                result = pgast.TypeCast(
                    arg=pg_expr,
                    type_name=pgast.TypeName(
                        name=elem_type,
                        array_bounds=[-1]))
            else:
                raise NotImplementedError(
                    f'{target_type.maintype} composite type '
                    f'is not supported yet')

        else:
            typ = schema.get(target_type.maintype)
            result = self._maybe_cast(pg_expr, typ)

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

        if isinstance(arg_type, s_obj.Map):
            return self._maybe_cast(
                self._new_binop(
                    lexpr=subj,
                    op='->>',
                    rexpr=index),
                arg_type.element_type)

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
                name=('char_length',), args=[subj])
        else:
            upper_bound = pgast.FuncCall(
                name=('array_upper',), args=[subj, one])

        neg_off = self._new_binop(
            lexpr=upper_bound, rexpr=index_plus_one, op=ast.ops.ADD)

        when_expr = pgast.CaseWhen(
            expr=when_cond, result=neg_off)

        index = pgast.CaseExpr(
            args=[when_expr], defresult=index_plus_one)

        if is_string:
            index = pgast.TypeCast(
                arg=index,
                type_name=pgast.TypeName(
                    name=('int',)
                )
            )
            result = pgast.FuncCall(
                name=('substr',),
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
                name=('char_length',), args=[subj])
        else:
            upper_bound = pgast.FuncCall(
                name=('array_upper',), args=[subj, one])

        if self._is_null_const(start):
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

        if self._is_null_const(stop):
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
            lower = pgast.TypeCast(
                arg=lower,
                type_name=pgast.TypeName(
                    name=('int',)
                )
            )

            args = [subj, lower]

            if upper is not upper_bound:
                for_length = self._new_binop(
                    lexpr=upper, op=ast.ops.SUB, rexpr=lower)
                for_length = self._new_binop(
                    lexpr=for_length, op=ast.ops.ADD, rexpr=one)

                for_length = pgast.TypeCast(
                    arg=for_length,
                    type_name=pgast.TypeName(
                        name=('int',)
                    )
                )
                args.append(for_length)

            result = pgast.FuncCall(name=('substr',), args=args)

        else:
            indirection = pgast.Indices(
                lidx=lower, ridx=upper)
            result = pgast.Indirection(
                arg=subj, indirection=[indirection])

        return result

    def visit_BinOp(self, expr):
        ctx = self.context.current

        with self.context.new() as newctx:
            newctx.expr_exposed = False
            op = expr.op
            is_bool_op = op in {ast.ops.AND, ast.ops.OR}

            if ctx.in_set_expr and is_bool_op:
                newctx.in_set_expr = False

            if isinstance(expr.op, ast.ops.TypeCheckOperator):
                cl = self._get_ptr_set(expr.left, 'std::__class__')
                left = self.visit(self._get_ptr_set(cl, 'std::id'))
            elif is_bool_op:
                with self.context.newsetscope():
                    left = self.visit(expr.left)
            else:
                left = self.visit(expr.left)

            if expr.op in (ast.ops.IN, ast.ops.NOT_IN):
                with self.context.new():
                    if isinstance(expr.right, irast.Sequence):
                        self.context.current.sequence_is_array = True
                    self.context.current.output_format = 'identity'
                    right = self.visit(expr.right)
            elif is_bool_op:
                with self.context.newsetscope():
                    right = self.visit(expr.right)
            else:
                right = self.visit(expr.right)

        if isinstance(expr.op, ast.ops.TypeCheckOperator):
            result = pgast.FuncCall(
                name=('edgedb', 'issubclass'),
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

                right = pgast.FuncCall(
                    name=(qual_func,), args=[right])
            else:
                op = expr.op

            left_type = irutils.infer_type(expr.left, ctx.schema)
            right_type = irutils.infer_type(expr.right, ctx.schema)

            if left_type and right_type:
                left_pg_type = pg_types.pg_type_from_object(
                    ctx.schema, left_type, True)

                right_pg_type = pg_types.pg_type_from_object(
                    ctx.schema, right_type, True)

                if (left_pg_type in {('text',), ('varchar',)} and
                        right_pg_type in {('text',), ('varchar',)} and
                        op == ast.ops.ADD):
                    op = '||'

            result = self._new_binop(left, right, op=op)

        return result

    def visit_UnaryOp(self, expr):
        with self.context.new() as ctx:
            ctx.expr_exposed = False
            operand = self.visit(expr.expr)
        return pgast.Expr(name=expr.op, rexpr=operand, kind=pgast.ExprKind.OP)

    def visit_IfElseExpr(self, expr):
        with self.context.new() as ctx:
            ctx.lax_paths = True
            return pgast.CaseExpr(
                args=[
                    pgast.CaseWhen(
                        expr=self.visit(expr.condition),
                        result=self.visit(expr.if_expr))
                ],
                defresult=self.visit(expr.else_expr))

    def visit_Sequence(self, expr):
        ctx = self.context.current

        elements = [self.visit(e) for e in expr.elements]

        if expr.is_array:
            result = pgast.ArrayExpr(elements=elements)
        elif (ctx.clause == 'result' and ctx.output_format == 'json' and
                ctx.expr_exposed):
            result = pgast.FuncCall(
                name=('jsonb_build_array',),
                args=elements
            )
        else:
            result = pgast.ImplicitRowExpr(args=elements)

        return result

    def visit_Mapping(self, expr):
        elements = []

        for k, v in expr.items.items():
            elements.append(pgast.Constant(val=k))
            elements.append(self.visit(v))

        return pgast.FuncCall(
            name=('jsonb_build_object',),
            args=elements
        )

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
                    name=('uuid',)
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
                if reversed:  # XXX: dead
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

    def _is_null_const(self, expr):
        if isinstance(expr, pgast.TypeCast):
            expr = expr.arg
        return isinstance(expr, pgast.Constant) and expr.val is None
