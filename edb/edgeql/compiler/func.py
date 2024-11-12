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
from typing import (
    Callable,
    Final,
    Optional,
    Protocol,
    Tuple,
    Union,
    Iterable,
    Sequence,
    Dict,
    List,
    cast,
    TYPE_CHECKING,
)

from edb import errors
from edb.common import ast
from edb.common import parsing
from edb.common.typeutils import not_none

from edb.ir import ast as irast
from edb.ir import staeval
from edb.ir import utils as irutils
from edb.ir import typeutils as irtyputils

from edb.schema import constraints as s_constr
from edb.schema import delta as sd
from edb.schema import functions as s_func
from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import operators as s_oper
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types
from edb.schema import indexes as s_indexes
from edb.schema import schema as s_schema
from edb.schema import utils as s_utils

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes as ft
from edb.edgeql import parser as qlparser

from . import casts
from . import context
from . import dispatch
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
    expr: qlast.FunctionCall, *, ctx: context.ContextLevel
) -> irast.Set:

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
                span=expr.span)

        funcname = sn.UnqualName(expr.func)
    else:
        funcname = sn.QualName(*expr.func)

    try:
        funcs = env.schema.get_functions(
            funcname,
            module_aliases=ctx.modaliases,
        )
    except errors.InvalidReferenceError as e:
        s_utils.enrich_schema_lookup_error(
            e,
            funcname,
            modaliases=ctx.modaliases,
            schema=env.schema,
            suggestion_limit=1,
            item_type=s_types.Type,
            span=expr.span,
            hint_text='did you mean to cast to'
        )
        raise

    prefer_subquery_args = any(
        func.get_prefer_subquery_args(env.schema) for func in funcs
    )

    if funcs is None:
        raise errors.QueryError(
            f'could not resolve function name {funcname}',
            span=expr.span)

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
        expr, funcname, typemods, prefer_subquery_args=prefer_subquery_args,
        ctx=ctx)
    with errors.ensure_span(expr.span):
        matched = polyres.find_callable(
            funcs, args=args, kwargs=kwargs, ctx=ctx)
    if not matched:
        alts = [f.get_signature_as_str(env.schema) for f in funcs]
        sig: List[str] = []
        # This is used to generate unique arg names.
        argnum = 0
        for argtype, _ in args:
            # Skip any name colliding with kwargs.
            while f'arg{argnum}' in kwargs:
                argnum += 1
            ty = schemactx.get_material_type(argtype, ctx=ctx)
            sig.append(
                f'arg{argnum}: {ty.get_displayname(env.schema)}'
            )
            argnum += 1
        for kwname, (kwtype, _) in kwargs.items():
            ty = schemactx.get_material_type(kwtype, ctx=ctx)
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
            span=expr.span)
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
                span=expr.span)
    else:
        matched_call = matched[0]

    func = matched_call.func
    assert isinstance(func, s_func.Function)

    inline_func = None
    if (
        func.get_language(ctx.env.schema) == qlast.Language.EdgeQL
        and (
            func.get_volatility(ctx.env.schema) == ft.Volatility.Modifying
            or func.get_is_inlined(ctx.env.schema)
        )
    ):
        inline_func = s_func.compile_function_inline(
            schema=ctx.env.schema,
            context=sd.CommandContext(
                schema=ctx.env.schema,
            ),
            body=not_none(func.get_nativecode(ctx.env.schema)),
            func_name=func.get_name(ctx.env.schema),
            params=func.get_params(ctx.env.schema),
            language=not_none(func.get_language(ctx.env.schema)),
            return_type=func.get_return_type(ctx.env.schema),
            return_typemod=func.get_return_typemod(ctx.env.schema),
            track_schema_ref_exprs=False,
            inlining_context=ctx,
        )

    # Record this node in the list of potential DML expressions.
    if func.get_volatility(env.schema) == ft.Volatility.Modifying:
        ctx.env.dml_exprs.append(expr)

        # This is some kind of mutation, so we need to check if it is
        # allowed.
        if ctx.env.options.in_ddl_context_name is not None:
            raise errors.SchemaDefinitionError(
                f'mutations are invalid in '
                f'{ctx.env.options.in_ddl_context_name}',
                span=expr.span,
            )
        elif (
            (dv := ctx.defining_view) is not None
            and dv.get_expr_type(ctx.env.schema) is s_types.ExprType.Select
            and not irutils.is_trivial_free_object(
                not_none(ctx.partial_path_prefix))
        ):
            # This is some shape in a regular query. Although
            # DML is not allowed in the computable, but it may
            # be possible to refactor it.
            raise errors.QueryError(
                f"mutations are invalid in a shape's computed expression",
                hint=(
                    f'To resolve this try to factor out the mutation '
                    f'expression into the top-level WITH block.'
                ),
                span=expr.span,
            )

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

    final_args, param_name_to_arg_key = finalize_args(
        matched_call,
        guessed_typemods=typemods,
        is_polymorphic=is_polymorphic,
        ctx=ctx,
    )

    # Forbid DML in non-scalar function args
    if func.get_nativecode(env.schema):
        # We are sure that there is no such functions implemented with SQL

        for arg in final_args.values():
            if arg.expr.typeref.is_scalar:
                continue
            if not irutils.contains_dml(arg.expr):
                continue
            raise errors.UnsupportedFeatureError(
                'newly created or updated objects cannot be passed to '
                'functions',
                span=arg.expr.span
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

    global_args = None
    if not inline_func:
        global_args = get_globals(
            expr, matched_call, candidates=funcs, ctx=ctx
        )

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
        typeref=typegen.type_to_typeref(
            rtype, env=env,
        ),
        typemod=matched_call.func.get_return_typemod(env.schema),
        has_empty_variadic=(matched_call.variadic_arg_count == 0),
        variadic_param_type=variadic_param_type,
        func_initial_value=func_initial_value,
        tuple_path_ids=tuple_path_ids,
        impl_is_strict=(
            func.get_impl_is_strict(env.schema)
            # Inlined functions should always check for null arguments.
            and not inline_func
        ),
        prefer_subquery_args=func.get_prefer_subquery_args(env.schema),
        is_singleton_set_of=func.get_is_singleton_set_of(env.schema),
        global_args=global_args,
        span=expr.span,
    )

    # Apply special function handling
    if special_func := _SPECIAL_FUNCTIONS.get(str(func_name)):
        res = special_func(fcall, ctx=ctx)
    elif inline_func:
        res = fcall

        # TODO: Global parameters still use the implicit globals parameter.
        # They should be directly substituted in whenever possible.

        inline_args: dict[str, irast.CallArg | irast.Set] = {}

        # Collect non-default call args to inline
        for param_shortname, arg_key in param_name_to_arg_key.items():
            if (
                isinstance(arg_key, int)
                and matched_call.variadic_arg_id is not None
                and arg_key >= matched_call.variadic_arg_id
            ):
                continue

            arg = final_args[arg_key]
            if arg.is_default:
                continue

            inline_args[param_shortname] = arg

        # Package variadic arguments into an array
        if variadic_param is not None:
            assert variadic_param_type is not None
            assert matched_call.variadic_arg_id is not None
            assert matched_call.variadic_arg_count is not None

            param_shortname = variadic_param.get_parameter_name(env.schema)
            inline_args[param_shortname] = ir_set = setgen.ensure_set(
                irast.Array(
                    elements=[
                        final_args[arg_key].expr
                        for arg_key in range(
                            matched_call.variadic_arg_id,
                            matched_call.variadic_arg_id
                            + matched_call.variadic_arg_count
                        )
                    ],
                    typeref=variadic_param_type,
                ),
                ctx=ctx,
            )

        # Compile default args if necessary
        for param in matched_func_params.objects(env.schema):
            param_shortname = param.get_parameter_name(env.schema)

            if param_shortname in inline_args:
                continue

            else:
                # Missing named only args have their default values already
                # compiled in try_bind_call_args.
                if bound_args := [
                    bound_arg
                    for bound_arg in matched_call.args
                    if bound_arg.param == param and bound_arg.is_default
                ]:
                    assert len(bound_args) == 1
                    inline_args[param_shortname] = bound_args[0].val
                    continue

                # Check if default is available
                p_default = param.get_default(env.schema)
                if p_default is None:
                    continue

                # Compile default
                assert isinstance(param, s_func.Parameter)
                p_ir_default = dispatch.compile(p_default.parse(), ctx=ctx)
                inline_args[param_shortname] = p_ir_default

        argument_inliner = ArgumentInliner(inline_args, ctx=ctx)
        res.body = argument_inliner.visit(inline_func)

    else:
        res = fcall

    if isinstance(res, irast.FunctionCall) and res.body:
        # If we are generating a special-cased inlined function call,
        # make sure to register all the arguments in the scope tree
        # to ensure that the compiled arguments get picked up when
        # compiling the body.
        for arg in res.args.values():
            pathctx.register_set_in_scope(
                arg.expr,
                optional=(
                    arg.param_typemod == ft.TypeModifier.OptionalType
                ),
                ctx=ctx,
            )

    ir_set = setgen.ensure_set(res, typehint=rtype, path_id=path_id, ctx=ctx)
    return stmt.maybe_add_view(ir_set, ctx=ctx)


class ArgumentInliner(ast.NodeTransformer):

    mapped_args: dict[irast.PathId, irast.PathId]
    inlined_arg_keys: list[int | str]

    def __init__(
        self,
        inline_args: dict[str, irast.CallArg | irast.Set],
        ctx: context.ContextLevel,
    ) -> None:
        super().__init__()
        self.inline_args = inline_args
        self.ctx = ctx
        self.mapped_args = {}

    def visit_Set(self, node: irast.Set) -> irast.Base:
        if (
            isinstance(node.expr, irast.Parameter)
            and node.expr.name in self.inline_args
        ):
            arg = self.inline_args[node.expr.name]
            if isinstance(arg, irast.CallArg):
                # Inline param as an expr ref. The pg compiler will find the
                # appropriate rvar.
                self.mapped_args[node.path_id] = arg.expr.path_id
                inlined_param_expr = setgen.ensure_set(
                    irast.InlinedParameterExpr(
                        typeref=arg.expr.typeref,
                        required=node.expr.required,
                        is_global=node.expr.is_global,
                    ),
                    path_id=arg.expr.path_id,
                    ctx=self.ctx,
                )
                inlined_param_expr.shape = node.shape
                return inlined_param_expr
            else:
                # Directly inline the set.
                # Used for default values, which are constants.
                return arg

        elif isinstance(node.expr, irast.Pointer):
            # The set and source path ids must match in order for the pointer
            # to find the correct rvar. If a pointer's source path was modified
            # because of an inlined parameter, modify the pointer's path as
            # well.
            prev_source_path_id = node.expr.source.path_id
            result = cast(irast.Set, self.generic_visit(node))

            if prev_source_path_id in self.mapped_args:
                result = setgen.new_set_from_set(
                    result,
                    path_id=irtyputils.replace_pathid_prefix(
                        result.path_id,
                        prev_source_path_id,
                        self.mapped_args[prev_source_path_id],
                    ),
                    ctx=self.ctx,
                )
                self.mapped_args[node.path_id] = result.path_id

            return result

        return cast(irast.Base, self.generic_visit(node))

    # Don't transform pointer refs.
    # They are updated in other places, such as cardinality inference.
    def visit_PointerRef(
        self, node: irast.PointerRef
    ) -> irast.Base:
        return node

    def visit_TupleIndirectionPointerRef(
        self, node: irast.TupleIndirectionPointerRef
    ) -> irast.Base:
        return node

    def visit_SpecialPointerRef(
        self, node: irast.SpecialPointerRef
    ) -> irast.Base:
        return node

    def visit_TypeIntersectionPointerRef(
        self, node: irast.TypeIntersectionPointerRef
    ) -> irast.Base:
        return node


class _SpecialCaseFunc(Protocol):
    def __call__(
        self, call: irast.FunctionCall, *, ctx: context.ContextLevel
    ) -> irast.Expr:
        pass


_SPECIAL_FUNCTIONS: dict[str, _SpecialCaseFunc] = {}


def _special_case(name: str) -> Callable[[_SpecialCaseFunc], _SpecialCaseFunc]:
    def func(f: _SpecialCaseFunc) -> _SpecialCaseFunc:
        _SPECIAL_FUNCTIONS[name] = f
        return f

    return func


def compile_operator(
    qlexpr: qlast.Expr,
    op_name: str,
    qlargs: List[qlast.Expr],
    *,
    ctx: context.ContextLevel,
) -> irast.Set:

    env = ctx.env
    schema = env.schema
    opers = schema.get_operators(op_name, module_aliases=ctx.modaliases)

    if opers is None:
        raise errors.QueryError(
            f'no operator matches the given name and argument types',
            span=qlexpr.span)

    typemods = polyres.find_callable_typemods(
        opers, num_args=len(qlargs), kwargs_names=set(), ctx=ctx)

    prefer_subquery_args = any(
        oper.get_prefer_subquery_args(env.schema) for oper in opers
    )

    args = []

    for ai, qlarg in enumerate(qlargs):
        arg_ir = polyres.compile_arg(
            qlarg,
            typemods[ai],
            prefer_subquery_args=prefer_subquery_args,
            ctx=ctx,
        )

        arg_type = setgen.get_set_type(arg_ir, ctx=ctx)
        if arg_type is None:
            raise errors.QueryError(
                f'could not resolve the type of operand '
                f'#{ai} of {op_name}',
                span=qlarg.span)

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
                span=qlarg.span)

        derivative_op = opers[0]
        opers = schema.get_operators(origin_op)
        if not opers:
            raise errors.InternalServerError(
                f'cannot find the origin operator for {op_name}',
                span=qlarg.span)
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
        args_ty = [schemactx.get_material_type(a[0], ctx=ctx) for a in args]
        args_dn = [repr(a.get_displayname(env.schema)) for a in args_ty]

        if len(args_dn) == 2:
            types = f'{args_dn[0]} and {args_dn[1]}'
        else:
            types = ', '.join(a for a in args_dn)

        if not matched:
            hint = ('Consider using an explicit type cast or a conversion '
                    'function.')

            if op_name == 'std::IF':
                hint = (f"The IF and ELSE result clauses must be of "
                        f"compatible types, while the condition clause must "
                        f"be 'std::bool'. {hint}")
            elif op_name == '+':
                str_t = env.schema.get('std::str', type=s_scalars.ScalarType)
                bytes_t = env.schema.get('std::bytes',
                                         type=s_scalars.ScalarType)
                if (
                    all(t.issubclass(env.schema, str_t) for t in args_ty) or
                    all(t.issubclass(env.schema, bytes_t) for t in args_ty) or
                    all(t.is_array() for t in args_ty)
                ):
                    hint = 'Consider using the "++" operator for concatenation'

            if isinstance(qlexpr, qlast.BinOp) and qlexpr.set_constructor:
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
                span=qlexpr.span)
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
                    span=qlexpr.span)

    oper = matched_call.func
    assert isinstance(oper, s_oper.Operator)
    env.add_schema_ref(oper, expr=qlexpr)
    oper_name = oper.get_shortname(env.schema)
    str_oper_name = str(oper_name)

    is_singleton_set_of = oper.get_is_singleton_set_of(env.schema)

    matched_params = oper.get_params(env.schema)
    rtype = matched_call.return_type
    matched_rtype = oper.get_return_type(env.schema)

    is_polymorphic = (
        any(p.get_type(env.schema).is_polymorphic(env.schema)
            for p in matched_params.objects(env.schema)) and
        matched_rtype.is_polymorphic(env.schema)
    )

    final_args, _ = finalize_args(
        matched_call,
        actual_typemods=actual_typemods,
        guessed_typemods=typemods,
        is_polymorphic=is_polymorphic,
        ctx=ctx,
    )

    if str_oper_name in {
        'std::UNION', 'std::IF', 'std::??'
    } and rtype.is_object_type():
        # Special case for the UNION, IF and ?? operators: instead of common
        # parent type, we return a union type.
        if str_oper_name == 'std::IF':
            larg, _, rarg = (a.expr for a in final_args.values())
        else:
            larg, rarg = (a.expr for a in final_args.values())

        left_type = setgen.get_set_type(larg, ctx=ctx)
        right_type = setgen.get_set_type(rarg, ctx=ctx)
        rtype = schemactx.get_union_type(
            [left_type, right_type],
            preserve_derived=True,
            ctx=ctx,
            span=qlexpr.span
        )

    from_op = oper.get_from_operator(env.schema)
    sql_operator = None
    if (
        from_op is not None
        and oper.get_code(env.schema) is None
        and oper.get_from_function(env.schema) is None
    ):
        sql_operator = tuple(from_op)

    origin_name: Optional[sn.QualName]
    origin_module_id: Optional[uuid.UUID]
    if derivative_op is not None:
        origin_name = oper_name
        origin_module_id = env.schema.get_global(
            s_mod.Module, origin_name.module).id
        oper_name = derivative_op.get_shortname(env.schema)
        is_singleton_set_of = derivative_op.get_is_singleton_set_of(env.schema)
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
        typeref=typegen.type_to_typeref(rtype, env=env),
        typemod=oper.get_return_typemod(env.schema),
        tuple_path_ids=[],
        impl_is_strict=oper.get_impl_is_strict(env.schema),
        prefer_subquery_args=oper.get_prefer_subquery_args(env.schema),
        is_singleton_set_of=is_singleton_set_of,
        span=qlexpr.span,
    )

    _check_free_shape_op(node, ctx=ctx)

    return stmt.maybe_add_view(
        setgen.ensure_set(node, typehint=rtype, ctx=ctx),
        ctx=ctx)


# These ops are all footguns when used with free shapes,
# so we ban them
INVALID_FREE_SHAPE_OPS: Final = {
    sn.QualName('std', x) for x in [
        'DISTINCT', '=', '!=', '?=', '?!=', 'IN', 'NOT IN',
        'assert_distinct',
    ]
}


def _check_free_shape_op(ir: irast.Call, *, ctx: context.ContextLevel) -> None:
    if ir.func_shortname not in INVALID_FREE_SHAPE_OPS:
        return

    virt_obj = ctx.env.schema.get(
        'std::FreeObject', type=s_objtypes.ObjectType)
    for arg in ir.args.values():
        typ = setgen.get_set_type(arg.expr, ctx=ctx)
        if typ.issubclass(ctx.env.schema, virt_obj):
            raise errors.QueryError(
                f'cannot use {ir.func_shortname.name} on free shape',
                span=ir.span)


def validate_recursive_operator(
    opers: Iterable[s_func.CallableObject],
    larg: Tuple[s_types.Type, irast.Set],
    rarg: Tuple[s_types.Type, irast.Set],
    *,
    ctx: context.ContextLevel,
) -> List[polyres.BoundCall]:

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


def compile_func_call_args(
    expr: qlast.FunctionCall,
    funcname: sn.Name,
    typemods: Dict[Union[int, str], ft.TypeModifier],
    *,
    prefer_subquery_args: bool=False,
    ctx: context.ContextLevel
) -> Tuple[
    List[Tuple[s_types.Type, irast.Set]],
    Dict[str, Tuple[s_types.Type, irast.Set]],
]:
    args = []
    kwargs = {}

    for ai, arg in enumerate(expr.args):
        arg_ir = polyres.compile_arg(
            arg, typemods[ai], prefer_subquery_args=prefer_subquery_args,
            ctx=ctx)
        arg_type = setgen.get_set_type(arg_ir, ctx=ctx)
        if arg_type is None:
            raise errors.QueryError(
                f'could not resolve the type of positional argument '
                f'#{ai} of function {funcname}',
                span=arg.span)

        args.append((arg_type, arg_ir))

    for aname, arg in expr.kwargs.items():
        arg_ir = polyres.compile_arg(
            arg, typemods[aname], prefer_subquery_args=prefer_subquery_args,
            ctx=ctx)

        arg_type = setgen.get_set_type(arg_ir, ctx=ctx)
        if arg_type is None:
            raise errors.QueryError(
                f'could not resolve the type of named argument '
                f'${aname} of function {funcname}',
                span=arg.span)

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

    if (
        (
            ctx.env.options.func_name is None
            or ctx.env.options.func_params is None
        )
        and not ctx.env.options.json_parameters
    ):
        glob_set = setgen.get_globals_as_json(
            tuple(globs), ctx=ctx, span=expr.span)
    else:
        if ctx.env.options.func_params is not None:
            # Make sure that we properly track the globals we use in functions
            for glob in globs:
                setgen.get_global_param(glob, ctx=ctx)
        glob_set = setgen.get_func_global_json_arg(ctx=ctx)

    return [glob_set]


def finalize_args(
    bound_call: polyres.BoundCall,
    *,
    actual_typemods: Sequence[ft.TypeModifier] = (),
    guessed_typemods: Dict[Union[int, str], ft.TypeModifier],
    is_polymorphic: bool = False,
    ctx: context.ContextLevel,
) -> tuple[dict[int | str, irast.CallArg], dict[str, int | str]]:

    args: dict[int | str, irast.CallArg] = {}
    param_name_to_arg: dict[str, int | str] = {}
    position_index: int = 0

    for i, barg in enumerate(bound_call.args):
        param = barg.param
        arg_val = barg.val
        arg_type_path_id: Optional[irast.PathId] = None
        if param is None:
            # defaults bitmask
            param_name_to_arg['__defaults_mask__'] = -1
            args[-1] = irast.CallArg(
                expr=arg_val,
                param_typemod=ft.TypeModifier.SingletonType,
            )
            continue

        if actual_typemods:
            param_mod = actual_typemods[i]
        else:
            param_mod = param.get_typemod(ctx.env.schema)

        if param_mod is not ft.TypeModifier.SetOfType:
            param_shortname = param.get_parameter_name(ctx.env.schema)

            if param_shortname in bound_call.null_args:
                pathctx.register_set_in_scope(arg_val, optional=True, ctx=ctx)

            # If we guessed the argument was optional but it wasn't,
            # try to go back and make it *not* optional.
            elif (
                param_mod is ft.TypeModifier.SingletonType
                and barg.arg_id is not None
                and param_mod is not guessed_typemods[barg.arg_id]
            ):
                for child in ctx.path_scope.children:
                    if (
                        child.path_id == arg_val.path_id
                        or (
                            arg_val.path_scope_id is not None
                            and child.unique_id == arg_val.path_scope_id
                        )
                    ):
                        child.optional = False

            # Object type arguments to functions may be overloaded, so
            # we populate a path id field so that we can also pass the
            # type as an argument on the pgsql side. If the param type
            # is "anytype", though, then it can't be overloaded on
            # that argument.
            arg_type = setgen.get_set_type(arg_val, ctx=ctx)
            if (
                isinstance(arg_type, s_objtypes.ObjectType)
                and barg.param
                and not barg.param.get_type(ctx.env.schema).is_any(
                    ctx.env.schema)
            ):
                arg_type_path_id = pathctx.extend_path_id(
                    arg_val.path_id,
                    ptrcls=setgen.resolve_ptr(
                        arg_type, '__type__', track_ref=None, ctx=ctx
                    ),
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
                and isinstance(arg_val.expr, irast.SelectStmt)
                and arg_val.expr.limit is None
            ):
                arg_val.expr.limit = dispatch.compile(
                    qlast.Constant.integer(ctx.implicit_limit),
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
            orig_arg_val = arg_val
            arg_val = casts.compile_cast(
                arg_val, paramtype, span=None, ctx=ctx)
            if ctx.path_scope.is_optional(orig_arg_val.path_id):
                pathctx.register_set_in_scope(arg_val, optional=True, ctx=ctx)

        arg = irast.CallArg(expr=arg_val, expr_type_path_id=arg_type_path_id,
            is_default=barg.is_default, param_typemod=param_mod)
        param_shortname = param.get_parameter_name(ctx.env.schema)
        if param_kind is ft.ParameterKind.NamedOnlyParam:
            args[param_shortname] = arg
            param_name_to_arg[param_shortname] = param_shortname
        else:
            args[position_index] = arg
            if (
                # Variadic args will all have the same name, but different
                # indexes. We want to take the first index.
                param_shortname not in param_name_to_arg
            ):
                param_name_to_arg[param_shortname] = position_index
            position_index += 1

    return args, param_name_to_arg


@_special_case('ext::ai::search')
def compile_ext_ai_search(
    call: irast.FunctionCall, *, ctx: context.ContextLevel
) -> irast.Expr:
    indexes = _validate_object_search_call(
        call,
        context="ext::ai::search()",
        object_arg=call.args[0],
        index_name=sn.QualName("ext::ai", "index"),
        ctx=ctx,
    )

    schema = ctx.env.schema

    index_metadata = {}
    for typeref, index in indexes.items():
        dimensions = index.must_get_json_annotation(
            schema,
            sn.QualName("ext::ai", "embedding_dimensions"),
            int,
        )
        kwargs = index.get_concrete_kwargs(schema)
        df_expr = kwargs.get("distance_function")
        if df_expr is not None:
            df = df_expr.ensure_compiled(
                schema,
                as_fragment=True,
                context=None,
            ).as_python_value()
        else:
            df = "Cosine"

        match df:
            case "Cosine":
                distance_fname = "cosine_distance"
            case "InnerProduct":
                distance_fname = "neg_inner_product"
            case "L2":
                distance_fname = "euclidean_distance"
            case _:
                raise RuntimeError(f"unsupported distance_function: {df}")

        distance_func = schema.get_functions(
            sn.QualName("ext::pgvector", distance_fname),
        )[0]

        index_metadata[typeref] = {
            "id": s_indexes.get_ai_index_id(schema, index),
            "dimensions": dimensions,
            "distance_function": (
                distance_func.get_shortname(schema),
                distance_func.get_backend_name(schema),
            ),
        }
    call.extras = {"index_metadata": index_metadata}

    return call


@_special_case('ext::ai::to_context')
def compile_ext_ai_to_str(
    call: irast.FunctionCall, *, ctx: context.ContextLevel
) -> irast.Expr:
    indexes = _validate_object_search_call(
        call,
        context="ext::ai::to_context()",
        object_arg=call.args[0],
        index_name=sn.QualName("ext::ai", "index"),
        ctx=ctx,
    )

    index = next(iter(indexes.values()))
    index_expr = index.get_expr(ctx.env.schema)
    assert index_expr is not None

    with ctx.detached() as subctx:
        subctx.partial_path_prefix = call.args[0].expr
        subctx.anchors["__subject__"] = call.args[0].expr
        call.body = dispatch.compile(index_expr.parse(), ctx=subctx)

    return call


@_special_case('std::fts::search')
def compile_fts_search(
    call: irast.FunctionCall, *, ctx: context.ContextLevel
) -> irast.Expr:
    _validate_object_search_call(
        call,
        context="std::fts::search()",
        object_arg=call.args[0],
        index_name=sn.QualName("std::fts", "index"),
        ctx=ctx,
    )

    return call


def _validate_object_search_call(
    call: irast.FunctionCall,
    *,
    context: str,
    object_arg: irast.CallArg,
    index_name: sn.QualName,
    ctx: context.ContextLevel,
) -> dict[irast.TypeRef, s_indexes.Index]:
    # validate that object has std::fts::index index
    object_typeref = object_arg.expr.typeref
    object_typeref = object_typeref.material_type or object_typeref
    stype_id = object_typeref.id

    schema = ctx.env.schema
    span = object_arg.span

    stype = schema.get_by_id(stype_id, type=s_types.Type)
    indexes = {}

    if union_variants := stype.get_union_of(schema):
        for variant in union_variants.objects(schema):
            schema, variant = variant.material_type(schema)
            idx = _validate_has_object_index(
                variant, schema, span, context, index_name)
            indexes[typegen.type_to_typeref(variant, ctx.env)] = idx
    else:
        idx = _validate_has_object_index(
            stype, schema, span, context, index_name)
        indexes[object_typeref] = idx

    return indexes


def _validate_has_object_index(
    stype: s_types.Type,
    schema: s_schema.Schema,
    span: Optional[parsing.Span],
    context: str,
    index_name: sn.QualName,
) -> s_indexes.Index:
    if isinstance(stype, s_indexes.IndexableSubject):
        (obj_index, _) = s_indexes.get_effective_object_index(
            schema, stype, index_name
        )
    else:
        obj_index = None

    if not obj_index:
        raise errors.InvalidReferenceError(
            f"{context} requires an {index_name} index on type "
            f"'{stype.get_displayname(schema)}'",
            span=span,
        )

    return obj_index


@_special_case('std::fts::with_options')
def compile_fts_with_options(
    call: irast.FunctionCall, *, ctx: context.ContextLevel
) -> irast.Expr:
    # language has already been typechecked to be an enum
    lang = call.args['language'].expr
    assert lang.typeref
    lang_ty_id = lang.typeref.id
    lang_ty = ctx.env.schema.get_by_id(lang_ty_id, type=s_scalars.ScalarType)
    assert lang_ty

    lang_domain = set()  # languages that the fts index needs to support
    try:
        lang_const = staeval.evaluate_to_python_val(lang, ctx.env.schema)
    except staeval.UnsupportedExpressionError:
        lang_const = None

    if lang_const is not None:
        # language is constant
        # -> determine its only value at compile time
        lang_domain.add(lang_const.lower())
    else:
        # language is not constant
        # -> use all possible values of the enum
        enum_values = lang_ty.get_enum_values(ctx.env.schema)
        assert enum_values
        for enum_value in enum_values:
            lang_domain.add(enum_value.lower())

    # weight_category
    weight_expr = call.args['weight_category'].expr
    try:
        weight: str = staeval.evaluate_to_python_val(
            weight_expr, ctx.env.schema)
    except staeval.UnsupportedExpressionError:
        raise errors.InvalidValueError(
            f"std::fts::search weight_category must be a constant",
            span=weight_expr.span,
        ) from None

    return irast.FTSDocument(
        text=call.args[0].expr,
        language=lang,
        language_domain=lang_domain,
        weight=weight,
        typeref=typegen.type_to_typeref(
            ctx.env.schema.get('std::fts::document', type=s_scalars.ScalarType),
            env=ctx.env,
        )
    )


@_special_case('std::_warn_on_call')
def compile_warn_on_call(
    call: irast.FunctionCall, *, ctx: context.ContextLevel
) -> irast.Expr:
    ctx.log_warning(
        errors.QueryError('Test warning please ignore', span=call.span)
    )
    return call
