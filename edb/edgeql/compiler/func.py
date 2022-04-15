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


"""EdgeQL compiler routines for function calls and operators."""


from __future__ import annotations
from typing import *

from edb import errors

from edb.ir import ast as irast

from edb.schema import constraints as s_constr
from edb.schema import functions as s_func
from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import operators as s_oper
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes as ft
from edb.edgeql import parser as qlparser

from . import casts
from . import context
from . import dispatch
from . import inference
from . import pathctx
from . import polyres
from . import schemactx
from . import setgen
from . import stmt
from . import typegen

if TYPE_CHECKING:
    import uuid


@dispatch.compile.register(qlast.FunctionCall)
def compile_FunctionCall(
        expr: qlast.FunctionCall, *, ctx: context.ContextLevel) -> irast.Set:

    env = ctx.env

    funcname: sn.Name
    if isinstance(expr.func, str):
        if (
            ctx.env.options.func_params is not None
            and ctx.env.options.func_params.get_by_name(
                env.schema, expr.func
            )
        ):
            raise errors.QueryError(
                f'parameter `{expr.func}` is not callable',
                context=expr.context)

        funcname = sn.UnqualName(expr.func)
    else:
        funcname = sn.QualName(*expr.func)

    funcs = env.schema.get_functions(funcname, module_aliases=ctx.modaliases)

    if funcs is None:
        raise errors.QueryError(
            f'could not resolve function name {funcname}',
            context=expr.context)

    in_polymorphic_func = (
        ctx.env.options.func_params is not None and
        ctx.env.options.func_params.has_polymorphic(env.schema)
    )

    in_abstract_constraint = (
        in_polymorphic_func and
        ctx.env.options.schema_object_context is s_constr.Constraint
    )

    typemods = polyres.find_callable_typemods(
        funcs, num_args=len(expr.args), kwargs_names=expr.kwargs.keys(),
        ctx=ctx)
    args, kwargs = compile_func_call_args(
        expr, funcname, typemods, ctx=ctx)
    matched = polyres.find_callable(funcs, args=args, kwargs=kwargs, ctx=ctx)
    if not matched:
        alts = [f.get_signature_as_str(env.schema) for f in funcs]
        sig: List[str] = []
        # This is used to generate unique arg names.
        argnum = 0
        for argtype, _ in args:
            # Skip any name colliding with kwargs.
            while f'arg{argnum}' in kwargs:
                argnum += 1
            sig.append(
                f'arg{argnum}: {argtype.get_displayname(env.schema)}'
            )
            argnum += 1
        for kwname, (kwtype, _) in kwargs.items():
            sig.append(
                f'NAMED ONLY {kwname}: {kwtype.get_displayname(env.schema)}'
            )

        signature = f'{funcname}({", ".join(sig)})'

        if not funcs:
            hint = None
        elif len(alts) == 1:
            hint = f'Did you want "{alts[0]}"?'
        else:  # Multiple alternatives
            hint = (
                f'Did you want one of the following functions instead:\n' +
                ('\n'.join(alts))
            )

        raise errors.QueryError(
            f'function "{signature}" does not exist',
            hint=hint,
            context=expr.context)
    elif len(matched) > 1:
        if in_abstract_constraint:
            matched_call = matched[0]
        else:
            alts = [m.func.get_signature_as_str(env.schema) for m in matched]
            raise errors.QueryError(
                f'function {funcname} is not unique',
                hint=f'Please disambiguate between the following '
                     f'alternatives:\n' +
                     ('\n'.join(alts)),
                context=expr.context)
    else:
        matched_call = matched[0]

    func = matched_call.func

    # Record this node in the list of potential DML expressions.
    if isinstance(func, s_func.Function) and func.get_has_dml(env.schema):
        ctx.env.dml_exprs.append(expr)

        # This is some kind of mutation, so we need to check if it is
        # allowed.
        if ctx.env.options.in_ddl_context_name is not None:
            raise errors.SchemaDefinitionError(
                f'mutations are invalid in '
                f'{ctx.env.options.in_ddl_context_name}',
                context=expr.context,
            )
        elif ((dv := ctx.defining_view) is not None and
                dv.get_expr_type(ctx.env.schema) is s_types.ExprType.Select and
                not ctx.env.options.allow_top_level_shape_dml):
            # This is some shape in a regular query. Although
            # DML is not allowed in the computable, but it may
            # be possible to refactor it.
            raise errors.QueryError(
                f"mutations are invalid in a shape's computed expression",
                hint=(
                    f'To resolve this try to factor out the mutation '
                    f'expression into the top-level WITH block.'
                ),
                context=expr.context,
            )

    assert isinstance(func, s_func.Function)
    func_name = func.get_shortname(env.schema)

    matched_func_params = func.get_params(env.schema)
    variadic_param = matched_func_params.find_variadic(env.schema)
    variadic_param_type = None
    if variadic_param is not None:
        variadic_param_type = typegen.type_to_typeref(
            variadic_param.get_type(env.schema),
            env=env,
        )

    matched_func_ret_type = func.get_return_type(env.schema)
    is_polymorphic = (
        any(p.get_type(env.schema).is_polymorphic(env.schema)
            for p in matched_func_params.objects(env.schema)) and
        matched_func_ret_type.is_polymorphic(env.schema)
    )

    matched_func_initial_value = func.get_initial_value(env.schema)

    final_args, params_typemods = finalize_args(
        matched_call,
        guessed_typemods=typemods,
        is_polymorphic=is_polymorphic,
        ctx=ctx,
    )

    if not in_abstract_constraint:
        # We cannot add strong references to functions from
        # abstract constraints, since we cannot know which
        # form of the function is actually used.
        env.add_schema_ref(func, expr)

    func_initial_value: Optional[irast.Set]

    if matched_func_initial_value is not None:
        frag = qlparser.parse_fragment(matched_func_initial_value.text)
        assert isinstance(frag, qlast.Expr)
        iv_ql = qlast.TypeCast(
            expr=frag,
            type=typegen.type_to_ql_typeref(matched_call.return_type, ctx=ctx),
        )
        func_initial_value = dispatch.compile(iv_ql, ctx=ctx)
    else:
        func_initial_value = None

    rtype = matched_call.return_type
    path_id = pathctx.get_expression_path_id(rtype, ctx=ctx)

    if rtype.is_tuple(env.schema):
        rtype = cast(s_types.Tuple, rtype)
        tuple_path_ids = []
        nested_path_ids = []
        for n, st in rtype.iter_subtypes(ctx.env.schema):
            elem_path_id = pathctx.get_tuple_indirection_path_id(
                path_id, n, st, ctx=ctx)

            if isinstance(st, s_types.Tuple):
                nested_path_ids.append([
                    pathctx.get_tuple_indirection_path_id(
                        elem_path_id, nn, sst, ctx=ctx)
                    for nn, sst in st.iter_subtypes(ctx.env.schema)
                ])

            tuple_path_ids.append(elem_path_id)
        for nested in nested_path_ids:
            tuple_path_ids.extend(nested)
    else:
        tuple_path_ids = []

    global_args = get_globals(expr, matched_call, candidates=funcs, ctx=ctx)

    fcall = irast.FunctionCall(
        args=final_args,
        func_shortname=func_name,
        backend_name=func.get_backend_name(env.schema),
        func_polymorphic=is_polymorphic,
        func_sql_function=func.get_from_function(env.schema),
        func_sql_expr=func.get_from_expr(env.schema),
        force_return_cast=func.get_force_return_cast(env.schema),
        volatility=func.get_volatility(env.schema),
        sql_func_has_out_params=func.get_sql_func_has_out_params(env.schema),
        error_on_null_result=func.get_error_on_null_result(env.schema),
        preserves_optionality=func.get_preserves_optionality(env.schema),
        preserves_upper_cardinality=func.get_preserves_upper_cardinality(
            env.schema),
        params_typemods=params_typemods,
        context=expr.context,
        typeref=typegen.type_to_typeref(
            rtype, env=env,
        ),
        typemod=matched_call.func.get_return_typemod(env.schema),
        has_empty_variadic=matched_call.has_empty_variadic,
        variadic_param_type=variadic_param_type,
        func_initial_value=func_initial_value,
        tuple_path_ids=tuple_path_ids,
        impl_is_strict=func.get_impl_is_strict(env.schema),
        global_args=global_args,
    )

    ir_set = setgen.ensure_set(fcall, typehint=rtype, path_id=path_id, ctx=ctx)
    return stmt.maybe_add_view(ir_set, ctx=ctx)


#: A dictionary of conditional callables and the indices
#: of the arguments that are evaluated conditionally.
CONDITIONAL_OPS = {
    sn.QualName('std', 'IF'): {0, 2},
    sn.QualName('std', '??'): {1},
}


def compile_operator(
        qlexpr: qlast.Base, op_name: str, qlargs: List[qlast.Expr], *,
        ctx: context.ContextLevel) -> irast.Set:

    env = ctx.env
    schema = env.schema
    opers = schema.get_operators(op_name, module_aliases=ctx.modaliases)

    if opers is None:
        raise errors.QueryError(
            f'no operator matches the given name and argument types',
            context=qlexpr.context)

    fq_op_name = next(iter(opers)).get_shortname(ctx.env.schema)
    conditional_args = CONDITIONAL_OPS.get(fq_op_name)

    typemods = polyres.find_callable_typemods(
        opers, num_args=len(qlargs), kwargs_names=set(), ctx=ctx)

    args = []

    for ai, qlarg in enumerate(qlargs):
        arg_ir = compile_arg(
            qlarg,
            typemods[ai],
            in_conditional=bool(conditional_args and ai in conditional_args),
            ctx=ctx,
        )

        arg_type = inference.infer_type(arg_ir, ctx.env)
        if arg_type is None:
            raise errors.QueryError(
                f'could not resolve the type of operand '
                f'#{ai} of {op_name}',
                context=qlarg.context)

        args.append((arg_type, arg_ir))

    # Check if the operator is a derived operator, and if so,
    # find the origins.
    origin_op = opers[0].get_derivative_of(env.schema)
    derivative_op: Optional[s_oper.Operator]
    if origin_op is not None:
        # If this is a derived operator, there should be
        # exactly one form of it.  This is enforced at the DDL
        # level, but check again to be sure.
        if len(opers) > 1:
            raise errors.InternalServerError(
                f'more than one derived operator of the same name: {op_name}',
                context=qlarg.context)

        derivative_op = opers[0]
        opers = schema.get_operators(origin_op)
        if not opers:
            raise errors.InternalServerError(
                f'cannot find the origin operator for {op_name}',
                context=qlarg.context)
        actual_typemods = [
            param.get_typemod(schema)
            for param in derivative_op.get_params(schema).objects(schema)
        ]
    else:
        derivative_op = None
        actual_typemods = []

    matched = None
    # Some 2-operand operators are special when their operands are
    # arrays or tuples.
    if len(args) == 2:
        coll_opers = None
        # If both of the args are arrays or tuples, potentially
        # compile the operator for them differently than for other
        # combinations.
        if args[0][0].is_tuple(env.schema) and args[1][0].is_tuple(env.schema):
            # Out of the candidate operators, find the ones that
            # correspond to tuples.
            coll_opers = [
                op for op in opers
                if all(
                    param.get_type(schema).is_tuple(schema)
                    for param in op.get_params(schema).objects(schema)
                )
            ]

        elif args[0][0].is_array() and args[1][0].is_array():
            # Out of the candidate operators, find the ones that
            # correspond to arrays.
            coll_opers = [
                op for op in opers
                if all(
                    param.get_type(schema).is_array()
                    for param in op.get_params(schema).objects(schema)
                )
            ]

        # Proceed only if we have a special case of collection operators.
        if coll_opers:
            # Then check if they are recursive (i.e. validation must be
            # done recursively for the subtypes). We rely on the fact that
            # it is forbidden to define an operator that has both
            # recursive and non-recursive versions.
            if not coll_opers[0].get_recursive(schema):
                # The operator is non-recursive, so regular processing
                # is needed.
                matched = polyres.find_callable(
                    coll_opers, args=args, kwargs={}, ctx=ctx)

            else:
                # The recursive operators are usually defined as
                # being polymorphic on all parameters, and so this has
                # a side-effect of forcing both operands to be of
                # the same type (via casting) before the operator is
                # applied.  This might seem suboptmial, since there might
                # be a more specific operator for the types of the
                # elements, but the current version of Postgres
                # actually requires tuples and arrays to be of the
                # same type in comparison, so this behavior is actually
                # what we want.
                matched = polyres.find_callable(
                    coll_opers,
                    args=args,
                    kwargs={},
                    ctx=ctx,
                )

                # Now that we have an operator, we need to validate that it
                # can be applied to the tuple or array elements.
                submatched = validate_recursive_operator(
                    opers, args[0], args[1], ctx=ctx)

                if len(submatched) != 1:
                    # This is an error. We want the error message to
                    # reflect whether no matches were found or too
                    # many, so we preserve the submatches found for
                    # this purpose.
                    matched = submatched

    # No special handling match was necessary, find a normal match.
    if matched is None:
        matched = polyres.find_callable(opers, args=args, kwargs={}, ctx=ctx)

    in_polymorphic_func = (
        ctx.env.options.func_params is not None and
        ctx.env.options.func_params.has_polymorphic(env.schema)
    )

    in_abstract_constraint = (
        in_polymorphic_func and
        ctx.env.options.schema_object_context is s_constr.Constraint
    )

    if not in_polymorphic_func:
        matched = [call for call in matched
                   if not call.func.get_abstract(env.schema)]

    if len(matched) == 1:
        matched_call = matched[0]
    else:
        if len(args) == 2:
            ltype = schemactx.get_material_type(args[0][0], ctx=ctx)
            rtype = schemactx.get_material_type(args[1][0], ctx=ctx)

            types = (
                f'{ltype.get_displayname(env.schema)!r} and '
                f'{rtype.get_displayname(env.schema)!r}')
        else:
            types = ', '.join(
                repr(
                    schemactx.get_material_type(
                        a[0], ctx=ctx).get_displayname(env.schema)
                ) for a in args
            )

        if not matched:
            hint = ('Consider using an explicit type cast or a conversion '
                    'function.')

            if op_name == 'std::IF':
                hint = (f"The IF and ELSE result clauses must be of "
                        f"compatible types, while the condition clause must "
                        f"be 'std::bool'. {hint}")
            elif op_name == '+':
                str_t = cast(s_scalars.ScalarType,
                             env.schema.get('std::str'))
                bytes_t = cast(s_scalars.ScalarType,
                               env.schema.get('std::bytes'))
                if (
                    (ltype.issubclass(env.schema, str_t) and
                        rtype.issubclass(env.schema, str_t)) or
                    (ltype.issubclass(env.schema, bytes_t) and
                        rtype.issubclass(env.schema, bytes_t)) or
                    (ltype.is_array() and rtype.is_array())
                ):
                    hint = 'Consider using the "++" operator for concatenation'

            if isinstance(qlexpr, qlast.SetConstructorOp):
                msg = (
                    f'set constructor has arguments of incompatible types '
                    f'{types}'
                )
            else:
                msg = (
                    f'operator {str(op_name)!r} cannot be applied to '
                    f'operands of type {types}'
                )
            raise errors.InvalidTypeError(
                msg,
                hint=hint,
                context=qlexpr.context)
        elif len(matched) > 1:
            if in_abstract_constraint:
                matched_call = matched[0]
            else:
                detail = ', '.join(
                    f'`{m.func.get_verbosename(ctx.env.schema)}`'
                    for m in matched
                )
                raise errors.QueryError(
                    f'operator {str(op_name)!r} is ambiguous for '
                    f'operands of type {types}',
                    hint=f'Possible variants: {detail}.',
                    context=qlexpr.context)

    oper = matched_call.func
    assert isinstance(oper, s_oper.Operator)
    env.add_schema_ref(oper, expr=qlexpr)
    oper_name = oper.get_shortname(env.schema)
    str_oper_name = str(oper_name)

    matched_params = oper.get_params(env.schema)
    rtype = matched_call.return_type
    matched_rtype = oper.get_return_type(env.schema)

    is_polymorphic = (
        any(p.get_type(env.schema).is_polymorphic(env.schema)
            for p in matched_params.objects(env.schema)) and
        matched_rtype.is_polymorphic(env.schema)
    )

    final_args, params_typemods = finalize_args(
        matched_call,
        actual_typemods=actual_typemods,
        guessed_typemods=typemods,
        is_polymorphic=is_polymorphic,
        ctx=ctx,
    )

    if str_oper_name in {'std::UNION', 'std::IF'} and rtype.is_object_type():
        # Special case for the UNION and IF operators, instead of common
        # parent type, we return a union type.
        if str_oper_name == 'std::UNION':
            larg, rarg = (a.expr for a in final_args)
        else:
            larg, _, rarg = (a.expr for a in final_args)

        left_type = setgen.get_set_type(larg, ctx=ctx)
        right_type = setgen.get_set_type(rarg, ctx=ctx)
        rtype = schemactx.get_union_type(
            [left_type, right_type], preserve_derived=True, ctx=ctx)

    from_op = oper.get_from_operator(env.schema)
    sql_operator = None
    if (from_op is not None and oper.get_code(env.schema) is None and
            oper.get_from_function(env.schema) is None and
            not in_polymorphic_func):
        sql_operator = tuple(from_op)

    origin_name: Optional[sn.QualName]
    origin_module_id: Optional[uuid.UUID]
    if derivative_op is not None:
        origin_name = oper_name
        origin_module_id = env.schema.get_global(
            s_mod.Module, origin_name.module).id
        oper_name = derivative_op.get_shortname(env.schema)
    else:
        origin_name = None
        origin_module_id = None

    from_func = oper.get_from_function(env.schema)
    if from_func is None:
        sql_func = None
    else:
        sql_func = tuple(from_func)

    node = irast.OperatorCall(
        args=final_args,
        func_shortname=oper_name,
        func_polymorphic=is_polymorphic,
        origin_name=origin_name,
        origin_module_id=origin_module_id,
        sql_function=sql_func,
        func_sql_expr=oper.get_from_expr(env.schema),
        sql_operator=sql_operator,
        force_return_cast=oper.get_force_return_cast(env.schema),
        volatility=oper.get_volatility(env.schema),
        operator_kind=oper.get_operator_kind(env.schema),
        params_typemods=params_typemods,
        context=qlexpr.context,
        typeref=typegen.type_to_typeref(rtype, env=env),
        typemod=oper.get_return_typemod(env.schema),
        tuple_path_ids=[],
        impl_is_strict=oper.get_impl_is_strict(env.schema),
    )

    _check_free_shape_op(node, ctx=ctx)

    return stmt.maybe_add_view(
        setgen.ensure_set(node, typehint=rtype, ctx=ctx),
        ctx=ctx)


# These ops are all footguns when used with free shapes,
# so we ban them
INVALID_FREE_SHAPE_OPS: Final = {
    sn.QualName('std', x) for x in [
        'DISTINCT', '=', '!=', '?=', '?!=', 'IN', 'NOT IN'
    ]
}


def _check_free_shape_op(
        ir: irast.Call, *, ctx: context.ContextLevel) -> None:
    if ir.func_shortname not in INVALID_FREE_SHAPE_OPS:
        return

    virt_obj = ctx.env.schema.get(
        'std::FreeObject', type=s_objtypes.ObjectType)
    for arg in ir.args:
        typ = inference.infer_type(arg.expr, ctx.env)
        if typ.issubclass(ctx.env.schema, virt_obj):
            raise errors.QueryError(
                f'cannot use {ir.func_shortname.name} on free shape',
                context=ir.context)


def validate_recursive_operator(
        opers: Iterable[s_func.CallableObject],
        larg: Tuple[s_types.Type, irast.Set],
        rarg: Tuple[s_types.Type, irast.Set], *,
        ctx: context.ContextLevel) -> List[polyres.BoundCall]:

    matched: List[polyres.BoundCall] = []

    # if larg and rarg are tuples or arrays, recurse into their subtypes
    if (
        (
            larg[0].is_tuple(ctx.env.schema)
            and rarg[0].is_tuple(ctx.env.schema)
        ) or (
            larg[0].is_array()
            and rarg[0].is_array()
        )
    ):
        assert isinstance(larg[0], s_types.Collection)
        assert isinstance(rarg[0], s_types.Collection)
        for rsub, lsub in zip(larg[0].get_subtypes(ctx.env.schema),
                              rarg[0].get_subtypes(ctx.env.schema)):
            matched = validate_recursive_operator(
                opers, (lsub, larg[1]), (rsub, rarg[1]), ctx=ctx)
            if len(matched) != 1:
                # this is an error already
                break

    else:
        # we just have a pair of non-containers to compare
        matched = polyres.find_callable(
            opers, args=[larg, rarg], kwargs={}, ctx=ctx)

    return matched


def compile_arg(
        arg_ql: qlast.Expr, typemod: ft.TypeModifier, *,
        in_conditional: bool=False,
        ctx: context.ContextLevel) -> irast.Set:
    fenced = typemod is ft.TypeModifier.SetOfType
    optional = typemod is ft.TypeModifier.OptionalType

    # Create a a branch for OPTIONAL ones. The OPTIONAL branch is to
    # have a place to mark as optional in the scope tree.
    # For fenced arguments we instead wrap it in a SELECT below.
    new = ctx.newscope(fenced=False) if optional else ctx.new()
    with new as argctx:
        if in_conditional:
            argctx.in_conditional = arg_ql.context

        if optional:
            argctx.path_scope.mark_as_optional()

        if fenced:
            arg_ql = qlast.SelectQuery(
                result=arg_ql, context=arg_ql.context,
                implicit=True, rptr_passthrough=True)

        argctx.inhibit_implicit_limit = True

        arg_ir = dispatch.compile(arg_ql, ctx=argctx)

        if optional:
            pathctx.register_set_in_scope(arg_ir, optional=True, ctx=ctx)

            if arg_ir.path_scope_id is None:
                pathctx.assign_set_scope(arg_ir, argctx.path_scope, ctx=argctx)

        return arg_ir


def compile_func_call_args(
    expr: qlast.FunctionCall,
    funcname: sn.Name,
    typemods: Dict[Union[int, str], ft.TypeModifier],
    *,
    ctx: context.ContextLevel
) -> Tuple[
    List[Tuple[s_types.Type, irast.Set]],
    Dict[str, Tuple[s_types.Type, irast.Set]],
]:
    args = []
    kwargs = {}

    for ai, arg in enumerate(expr.args):
        arg_ir = compile_arg(arg, typemods[ai], ctx=ctx)
        arg_type = inference.infer_type(arg_ir, ctx.env)
        if arg_type is None:
            raise errors.QueryError(
                f'could not resolve the type of positional argument '
                f'#{ai} of function {funcname}',
                context=arg.context)

        args.append((arg_type, arg_ir))

    for aname, arg in expr.kwargs.items():
        arg_ir = compile_arg(arg, typemods[aname], ctx=ctx)

        arg_type = inference.infer_type(arg_ir, ctx.env)
        if arg_type is None:
            raise errors.QueryError(
                f'could not resolve the type of named argument '
                f'${aname} of function {funcname}',
                context=arg.context)

        kwargs[aname] = (arg_type, arg_ir)

    return args, kwargs


def get_globals(
    expr: qlast.FunctionCall,
    bound_call: polyres.BoundCall,
    candidates: Sequence[s_func.Function],
    *, ctx: context.ContextLevel,
) -> List[irast.Set]:
    assert isinstance(bound_call.func, s_func.Function)

    func_language = bound_call.func.get_language(ctx.env.schema)
    if func_language is not qlast.Language.EdgeQL:
        return []

    schema = ctx.env.schema

    globs = set()
    if bound_call.func.get_params(schema).has_objects(schema):
        # We look at all the candidates since it might be used in a
        # subtype's overload.
        # TODO: be careful and only do this in the needed cases
        for func in candidates:
            globs.update(set(func.get_used_globals(schema).objects(schema)))
    else:
        globs.update(bound_call.func.get_used_globals(schema).objects(schema))

    if ctx.env.options.func_params is None:
        glob_set = setgen.get_globals_as_json(
            tuple(globs), ctx=ctx, srcctx=expr.context)
    else:
        # Make sure that we properly track the globals we use
        for glob in globs:
            setgen.get_global_param(glob, ctx=ctx)
        glob_set = setgen.get_func_global_json_arg(ctx=ctx)

    return [glob_set]


def finalize_args(
    bound_call: polyres.BoundCall, *,
    actual_typemods: Sequence[ft.TypeModifier] = (),
    guessed_typemods: Dict[Union[int, str], ft.TypeModifier],
    is_polymorphic: bool = False,
    ctx: context.ContextLevel,
) -> Tuple[List[irast.CallArg], List[ft.TypeModifier]]:

    args: List[irast.CallArg] = []
    typemods = []

    for i, barg in enumerate(bound_call.args):
        param = barg.param
        arg = barg.val
        arg_type_path_id: Optional[irast.PathId] = None
        if param is None:
            # defaults bitmask
            args.append(irast.CallArg(expr=arg))
            typemods.append(ft.TypeModifier.SingletonType)
            continue

        if actual_typemods:
            param_mod = actual_typemods[i]
        else:
            param_mod = param.get_typemod(ctx.env.schema)

        typemods.append(param_mod)

        if param_mod is not ft.TypeModifier.SetOfType:
            param_shortname = param.get_parameter_name(ctx.env.schema)

            if param_shortname in bound_call.null_args:
                pathctx.register_set_in_scope(arg, optional=True, ctx=ctx)

            # If we guessed the argument was optional but it wasn't,
            # try to go back and make it *not* optional.
            elif (
                param_mod is ft.TypeModifier.SingletonType
                and barg.arg_id is not None
                and param_mod is not guessed_typemods[barg.arg_id]
            ):
                for child in ctx.path_scope.children:
                    if (
                        child.path_id == arg.path_id
                        or (
                            arg.path_scope_id is not None
                            and child.unique_id == arg.path_scope_id
                        )
                    ):
                        child.optional = False

            # Object type arguments to functions may be overloaded, so
            # we populate a path id field so that we can also pass the
            # type as an argument on the pgsql side. If the param type
            # is "anytype", though, then it can't be overloaded on
            # that argument.
            arg_type = setgen.get_set_type(arg, ctx=ctx)
            if (
                isinstance(arg_type, s_objtypes.ObjectType)
                and barg.param
                and not barg.param.get_type(ctx.env.schema).is_any(
                    ctx.env.schema)
            ):
                arg_type_path_id = pathctx.extend_path_id(
                    arg.path_id,
                    ptrcls=arg_type.getptr(
                        ctx.env.schema, sn.UnqualName('__type__')),
                    ctx=ctx,
                )
        else:
            is_array_agg = (
                isinstance(bound_call.func, s_func.Function)
                and (
                    bound_call.func.get_shortname(ctx.env.schema)
                    == sn.QualName('std', 'array_agg')
                )
            )

            if (
                # Ideally, we should implicitly slice all array values,
                # but in practice, the vast majority of large arrays
                # will come from array_agg, and so we only care about
                # that.
                is_array_agg
                and ctx.expr_exposed
                and ctx.implicit_limit
                and isinstance(arg.expr, irast.SelectStmt)
                and arg.expr.limit is None
                and not ctx.inhibit_implicit_limit
            ):
                arg.expr.limit = dispatch.compile(
                    qlast.IntegerConstant(value=str(ctx.implicit_limit)),
                    ctx=ctx,
                )

        paramtype = barg.param_type
        param_kind = param.get_kind(ctx.env.schema)
        if param_kind is ft.ParameterKind.VariadicParam:
            # For variadic params, paramtype would be array<T>,
            # and we need T to cast the arguments.
            assert isinstance(paramtype, s_types.Array)
            paramtype = list(paramtype.get_subtypes(ctx.env.schema))[0]

        # Check if we need to cast the argument value before passing
        # it to the callable.
        compatible = s_types.is_type_compatible(
            paramtype, barg.valtype, schema=ctx.env.schema,
        )

        if not compatible:
            # The callable form was chosen via an implicit cast,
            # cast the arguments so that the backend has no
            # wiggle room to apply its own (potentially different)
            # casting.
            arg = casts.compile_cast(
                arg, paramtype, srcctx=None, ctx=ctx)

        args.append(
            irast.CallArg(expr=arg, expr_type_path_id=arg_type_path_id,
                          is_default=barg.is_default))

    return args, typemods
