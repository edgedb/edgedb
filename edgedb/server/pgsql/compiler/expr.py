##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import typing

from edgedb.lang.common import ast

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types

from . import astutils
from . import context
from . import dispatch
from . import expr as expr_compiler  # NOQA
from . import output
from . import pathctx
from . import relctx
from . import relgen
from . import typecomp


@dispatch.compile.register(irast.Parameter)
def compile_Parameter(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    if expr.name.isnumeric():
        index = int(expr.name)
    else:
        if expr.name in ctx.argmap:
            index = ctx.argmap[expr.name]
        else:
            index = len(ctx.argmap) + 1
            ctx.argmap[expr.name] = index

    result = pgast.ParamRef(number=index)
    return typecomp.cast(
        result, source_type=expr.type, target_type=expr.type,
        force=True, env=ctx.env)


@dispatch.compile.register(irast.EmptySet)
def compile_EmptySet(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    return pgast.Constant(val=None)


@dispatch.compile.register(irast.Constant)
def compile_Constant(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    result = pgast.Constant(val=expr.value)
    result = typecomp.cast(
        result, source_type=expr.type, target_type=expr.type,
        force=True, env=ctx.env)
    return result


@dispatch.compile.register(irast.TypeCast)
def compile_TypeCast(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    pg_expr = dispatch.compile(expr.expr, ctx=ctx)

    target_type = irutils.infer_type(expr, ctx.schema)

    if (isinstance(expr.expr, irast.EmptySet) or
            (isinstance(expr.expr, irast.Array) and
                not expr.expr.elements) or
            (isinstance(expr.expr, irast.Mapping) and
                not expr.expr.keys)):

        return typecomp.cast(
            pg_expr, source_type=target_type,
            target_type=target_type, force=True, env=ctx.env)

    else:
        source_type = irutils.infer_type(expr.expr, ctx.schema)
        return typecomp.cast(
            pg_expr, source_type=source_type, target_type=target_type,
            env=ctx.env)


@dispatch.compile.register(irast.IndexIndirection)
def compile_IndexIndirection(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    # Handle Expr[Index], where Expr may be std::str or array<T>.
    # For strings we translate this into substr calls, whereas
    # for arrays the native slice syntax is used.
    is_string = False
    arg_type = irutils.infer_type(expr.expr, ctx.schema)

    subj = dispatch.compile(expr.expr, ctx=ctx)
    index = dispatch.compile(expr.index, ctx=ctx)

    if isinstance(arg_type, s_obj.Map):
        # When we compile maps we always cast keys to text,
        # hence we need to cast the index to text here.
        index_type = irutils.infer_type(expr.index, ctx.schema)
        index = typecomp.cast(
            index,
            source_type=index_type,
            target_type=ctx.schema.get('std::str'),
            env=ctx.env)

        if isinstance(arg_type.element_type, s_obj.Array):
            return typecomp.cast(
                astutils.new_binop(
                    lexpr=subj,
                    op='->',
                    rexpr=index),
                source_type=ctx.schema.get('std::json'),
                target_type=arg_type.element_type,
                env=ctx.env)

        elif isinstance(arg_type.element_type, s_obj.Map):
            return astutils.new_binop(
                lexpr=subj,
                op='->',
                rexpr=index)

        else:
            return typecomp.cast(
                astutils.new_binop(
                    lexpr=subj,
                    op='->>',
                    rexpr=index),
                source_type=ctx.schema.get('std::str'),
                target_type=arg_type.element_type,
                env=ctx.env)

    if isinstance(arg_type, s_atoms.Atom):
        b = arg_type.get_topmost_base()
        is_string = b.name == 'std::str'

    one = pgast.Constant(val=1)
    zero = pgast.Constant(val=0)

    when_cond = astutils.new_binop(
        lexpr=index, rexpr=zero, op=ast.ops.LT)

    index_plus_one = astutils.new_binop(
        lexpr=index, op=ast.ops.ADD, rexpr=one)

    if is_string:
        upper_bound = pgast.FuncCall(
            name=('char_length',), args=[subj])
    else:
        upper_bound = pgast.FuncCall(
            name=('array_upper',), args=[subj, one])

    neg_off = astutils.new_binop(
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


@dispatch.compile.register(irast.SliceIndirection)
def compile_SliceIndirection(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    # Handle Expr[Start:End], where Expr may be std::str or array<T>.
    # For strings we translate this into substr calls, whereas
    # for arrays the native slice syntax is used.
    subj = dispatch.compile(expr.expr, ctx=ctx)
    start = dispatch.compile(expr.start, ctx=ctx)
    stop = dispatch.compile(expr.stop, ctx=ctx)
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

    if astutils.is_null_const(start):
        lower = one
    else:
        lower = start

        when_cond = astutils.new_binop(
            lexpr=lower, rexpr=zero, op=ast.ops.LT)
        lower_plus_one = astutils.new_binop(
            lexpr=lower, rexpr=one, op=ast.ops.ADD)

        neg_off = astutils.new_binop(
            lexpr=upper_bound, rexpr=lower_plus_one, op=ast.ops.ADD)

        when_expr = pgast.CaseWhen(
            expr=when_cond, result=neg_off)
        lower = pgast.CaseExpr(
            args=[when_expr], defresult=lower_plus_one)

    if astutils.is_null_const(stop):
        upper = upper_bound
    else:
        upper = stop

        when_cond = astutils.new_binop(
            lexpr=upper, rexpr=zero, op=ast.ops.LT)

        neg_off = astutils.new_binop(
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
            for_length = astutils.new_binop(
                lexpr=upper, op=ast.ops.SUB, rexpr=lower)
            for_length = astutils.new_binop(
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


@dispatch.compile.register(irast.BinOp)
def compile_BinOp(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    with ctx.new() as newctx:
        newctx.expr_exposed = False
        op = expr.op
        is_bool_op = op in {ast.ops.AND, ast.ops.OR}
        left = dispatch.compile(expr.left, ctx=newctx)
        right = dispatch.compile(expr.right, ctx=newctx)

    if isinstance(expr.op, ast.ops.TypeCheckOperator):
        result = pgast.FuncCall(
            name=('edgedb', 'issubclass'),
            args=[left, right])

        if expr.op == ast.ops.IS_NOT:
            result = astutils.new_unop(ast.ops.NOT, result)

    else:
        if not isinstance(expr.left, irast.EmptySet):
            left_type = irutils.infer_type(expr.left, ctx.schema)
        else:
            left_type = None

        if not isinstance(expr.right, irast.EmptySet):
            right_type = irutils.infer_type(expr.right, ctx.schema)
        else:
            right_type = None

        if (not isinstance(expr.left, irast.EmptySet) and
                not isinstance(expr.right, irast.EmptySet)):
            left_pg_type = pg_types.pg_type_from_object(
                ctx.schema, left_type, True)

            right_pg_type = pg_types.pg_type_from_object(
                ctx.schema, right_type, True)

            if (left_pg_type in {('text',), ('varchar',)} and
                    right_pg_type in {('text',), ('varchar',)} and
                    op == ast.ops.ADD):
                op = '||'

        if isinstance(left_type, s_obj.Tuple):
            left = _tuple_to_row_expr(expr.left, ctx=newctx)
            left_count = len(left.args)
        else:
            left_count = 0

        if isinstance(right_type, s_obj.Tuple):
            right = _tuple_to_row_expr(expr.right, ctx=newctx)
            right_count = len(right.args)
        else:
            right_count = 0

        if left_count != right_count:
            # Postgres does not allow comparing rows with
            # unequal number of entries, but we want to allow
            # this.  Fortunately, we know that such comparison is
            # always False.
            result = pgast.Constant(val=False)
        else:
            if is_bool_op:
                # Transform logical operators to force
                # the correct behaviour with respect to NULLs.
                # See the OrFilterFunction comment for details.
                if ctx.clause == 'where':
                    if expr.op == ast.ops.OR:
                        result = pgast.FuncCall(
                            name=('edgedb', '_or'),
                            args=[left, right]
                        )
                    else:
                        # For the purposes of the WHERE clause,
                        # AND operator works correctly, as
                        # it will either return NULL or FALSE,
                        # which both will disqualify the row.
                        result = astutils.new_binop(left, right, op=op)
                else:
                    # For expressions outside WHERE, we
                    # always want the result to be NULL
                    # if either operand is NULL.
                    bitop = '&' if expr.op == ast.ops.AND else '|'
                    bitcond = astutils.new_binop(
                        lexpr=pgast.TypeCast(
                            arg=left,
                            type_name=pgast.TypeName(
                                name=('int',)
                            )
                        ),
                        rexpr=pgast.TypeCast(
                            arg=right,
                            type_name=pgast.TypeName(
                                name=('int',)
                            )
                        ),
                        op=bitop
                    )
                    bitcond = pgast.TypeCast(
                        arg=bitcond,
                        type_name=pgast.TypeName(
                            name=('bool',)
                        )
                    )
                    result = bitcond
            else:
                result = astutils.new_binop(left, right, op=op)

    return result


@dispatch.compile.register(irast.UnaryOp)
def compile_UnaryOp(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    with ctx.new() as subctx:
        subctx.expr_exposed = False
        operand = dispatch.compile(expr.expr, ctx=subctx)
    return pgast.Expr(name=expr.op, rexpr=operand, kind=pgast.ExprKind.OP)


@dispatch.compile.register(irast.IfElseExpr)
def compile_IfElseExpr(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    with ctx.new() as subctx:
        return pgast.CaseExpr(
            args=[
                pgast.CaseWhen(
                    expr=dispatch.compile(expr.condition, ctx=subctx),
                    result=dispatch.compile(expr.if_expr, ctx=subctx))
            ],
            defresult=dispatch.compile(expr.else_expr, ctx=subctx))


@dispatch.compile.register(irast.Array)
def compile_Array(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    elements = [dispatch.compile(e, ctx=ctx) for e in expr.elements]
    return pgast.ArrayExpr(elements=elements)


@dispatch.compile.register(irast.TupleIndirection)
def compile_TupleIndirection(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    for se in expr.expr.expr.elements:
        if se.name == expr.name:
            return dispatch.compile(se.val, ctx=ctx)

    raise ValueError(f'no tuple element with name {expr.name}')


@dispatch.compile.register(irast.Tuple)
def compile_Tuple(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    ttype = irutils.infer_type(expr, ctx.env.schema)
    ttypes = ttype.element_types
    telems = list(ttypes)

    path_id = irast.PathId([ttype])

    result = astutils.TupleVar(elements=[
        astutils.TupleElement(
            path_id=irutils.tuple_indirection_path_id(
                path_id, telems[i], ttypes[telems[i]]),
            val=dispatch.compile(e.val, ctx=ctx)
        ) for i, e in enumerate(expr.elements)
    ])

    return output.output_as_value(result, ctx=ctx)


@dispatch.compile.register(irast.Mapping)
def compile_Mapping(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    elements = []

    schema = ctx.schema
    str_t = schema.get('std::str')

    for k, v in zip(expr.keys, expr.values):
        # Cast keys to 'text' explicitly.
        elements.append(
            typecomp.cast(
                dispatch.compile(k, ctx=ctx),
                source_type=irutils.infer_type(k, schema),
                target_type=str_t,
                env=ctx.env)
        )

        # Don't cast values as we want to preserve ints, floats, bools,
        # and arrays as JSON arrays (not text-encoded PostgreSQL types.)
        elements.append(dispatch.compile(v, ctx=ctx))

    return pgast.FuncCall(
        name=('jsonb_build_object',),
        args=elements
    )


@dispatch.compile.register(irast.TypeRef)
def compile_TypeRef(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
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


@dispatch.compile.register(irast.FunctionCall)
def compile_FunctionCall(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    funcobj = expr.func

    if funcobj.aggregate:
        raise RuntimeError(
            'aggregate functions are not supported in simple expressions')

    if funcobj.set_returning:
        raise RuntimeError(
            'set returning functions are not supported in simple expressions')

    args = [dispatch.compile(a, ctx=ctx) for a in expr.args]

    if funcobj.from_function:
        name = (funcobj.from_function,)
    else:
        name = (
            common.edgedb_module_name_to_schema_name(
                funcobj.shortname.module),
            common.edgedb_name_to_pg_name(
                funcobj.shortname.name)
        )

    result = pgast.FuncCall(name=name, args=args)

    return result


@dispatch.compile.register(irast.Set)
def compile_Set(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:

    if ctx.singleton_mode:
        return _compile_set_in_singleton_mode(expr, ctx=ctx)

    has_shape = (
        bool(expr.shape) and
        (ctx.expr_exposed or
         ctx.shape_format == context.ShapeFormat.FLAT)
    )

    if ctx.clause == 'where' and ctx.rel is ctx.stmt:
        # When referred to in WHERE
        # we want to wrap the set CTE into
        #    EXISTS(SELECT * FROM SetCTE WHERE SetCTE.expr)
        source_cte = relgen.set_to_cte(expr, ctx=ctx)
        result = relgen.wrap_set_rel_as_bool_disjunction(
            expr, source_cte, ctx=ctx)

    elif ctx.expr_as_isolated_set or ctx.expr_as_value:
        # When referred to in OFFSET/LIMIT we want to wrap the
        # set CTE into
        #    SELECT v FROM SetCTE
        with ctx.subquery() as subctx:
            source_cte = relgen.set_to_cte(expr, ctx=subctx)

            relctx.include_range(
                subctx.rel, source_cte, join_type='inner', ctx=subctx)

            if has_shape:
                _compile_shape(expr, ctx=subctx)

            source_cte = subctx.rel

        result = relgen.wrap_set_rel(
            expr, source_cte, as_value=ctx.expr_as_value, ctx=ctx)

    else:
        # Otherwise we join the range directly into the current rel
        # and make its refs available in the path namespace.
        if ctx.clause == 'orderby':
            join_type = 'left'
        else:
            join_type = 'inner'

        source_cte = relgen.set_to_cte(expr, ctx=ctx)

        source_rvar = relctx.include_range(
            ctx.rel, source_cte, join_type=join_type,
            lateral=True, ctx=ctx)

        result = pathctx.get_rvar_path_value_var(
            source_rvar, expr.path_id, env=ctx.env)

        if has_shape:
            result = _compile_shape(expr, ctx=ctx)

        result = output.output_as_value(result, ctx=ctx)

    return result


@dispatch.compile.register(irast.Coalesce)
def compile_Coalesce(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    with ctx.new() as subctx:
        subctx.lax_paths = 1
        pg_args = [dispatch.compile(a, ctx=subctx) for a in expr.args]
    return pgast.FuncCall(name=('coalesce',), args=pg_args)


def _tuple_to_row_expr(tuple_expr, *, ctx):
    tuple_type = irutils.infer_type(tuple_expr, ctx.schema)
    subtypes = tuple_type.element_types
    row = []
    for n in subtypes:
        ref = irutils.new_expression_set(
            irast.TupleIndirection(
                expr=tuple_expr,
                name=n,
                path_id=irutils.tuple_indirection_path_id(
                    tuple_expr.path_id, n, subtypes[n])),
            ctx.schema
        )
        row.append(dispatch.compile(ref, ctx=ctx))

    return pgast.RowExpr(args=row)


def _compile_shape(
        ir_set: irast.Set, *, ctx: context.CompilerContextLevel) \
        -> typing.Union[pgast.Base, astutils.TupleVar]:
    elements = []

    for e in ir_set.shape:
        rptr = e.rptr
        cset = irutils.get_canonical_set(e)
        ptrcls = rptr.ptrcls
        ptrdir = rptr.direction or s_pointers.PointerDirection.Outbound
        is_singleton = ptrcls.singular(ptrdir)

        with ctx.new() as newctx:
            newctx.expr_as_value = (
                not is_singleton or
                irutils.is_subquery_set(cset) or
                irutils.is_inner_view_reference(cset) or
                isinstance(cset.scls, s_concepts.Concept)
            )
            element = dispatch.compile(e, ctx=newctx)

        elements.append(astutils.tuple_element_for_shape_el(e, element))

    result = astutils.TupleVar(elements=elements, named=True)
    pathctx.put_path_value_var(ctx.rel, ir_set.path_id, result, env=ctx.env)

    if ctx.shape_format == context.ShapeFormat.SERIALIZED:
        result = output.output_as_value(result, ctx=ctx)
        pathctx.put_path_serialized_var(
            ctx.rel, ir_set.path_id, result, env=ctx.env)

    for element in elements:
        # The ref might have already been added by the nested shape
        # processing, so check for it.
        pathctx.put_path_value_var_if_not_exists(
            ctx.rel, element.path_id, element.val, env=ctx.env)

    return result


def _compile_set_in_singleton_mode(
        node: irast.Set, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    if node.expr is not None:
        return dispatch.compile(node.expr, ctx=ctx)
    else:
        if node.rptr:
            ptrcls = node.rptr.ptrcls
            source = node.rptr.source

            if not isinstance(ptrcls, s_lprops.LinkProperty):
                if source.rptr:
                    raise RuntimeError(
                        'unexpectedly long path in simple expr')

            colref = pgast.ColumnRef(
                name=[
                    common.edgedb_name_to_pg_name(ptrcls.shortname)
                ]
            )
        elif isinstance(node.scls, s_atoms.Atom):
            colref = pgast.ColumnRef(
                name=[
                    common.edgedb_name_to_pg_name(node.scls.name)
                ]
            )
        else:
            colref = pgast.ColumnRef(
                name=[
                    common.edgedb_name_to_pg_name(node.scls.name)
                ]
            )

        return colref
