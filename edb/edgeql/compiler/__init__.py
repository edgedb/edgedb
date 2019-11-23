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


"""EdgeQL to IR compiler."""


from __future__ import annotations
from typing import *  # NoQA

from edb import errors

from edb.edgeql import parser as ql_parser
from edb.edgeql import qltypes

from edb.common import debug
from edb.common import markup  # NOQA

from edb.schema import functions as s_func
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types

from edb.edgeql import ast as qlast
from edb.ir import ast as irast
from edb.ir import staeval as ireval
from edb.ir import typeutils as irtyputils

from . import dispatch
from . import inference
from . import setgen
from . import stmtctx

from .config import get_config_type_shape  # NOQA

from . import expr as _expr_compiler  # NOQA
from . import config as _config_compiler  # NOQA
from . import stmt as _stmt_compiler  # NOQA

if TYPE_CHECKING:
    from edb.schema import name as s_name
    from edb.schema import objects as s_obj
    from edb.schema import schema as s_schema


def compile_ast_to_ir(
    tree: qlast.Base,
    schema: s_schema.Schema,
    *,
    parent_object_type: Optional[s_obj.ObjectMeta] = None,
    anchors: Optional[
        Mapping[
            Union[str, qlast.SpecialAnchorT],
            Union[irast.Base, s_obj.Object],
        ]
    ] = None,
    path_prefix_anchor: Optional[qlast.SpecialAnchorT] = None,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
    singletons: Sequence[s_types.Type] = (),
    func_params: Optional[s_func.ParameterLikeList] = None,
    derived_target_module: Optional[str] = None,
    result_view_name: Optional[s_name.SchemaName] = None,
    implicit_id_in_shapes: bool = False,
    implicit_tid_in_shapes: bool = False,
    schema_view_mode: bool = False,
    disable_constant_folding: bool = False,
    json_parameters: bool = False,
    session_mode: bool = False,
    allow_abstract_operators: bool = False,
    allow_generic_type_output: bool = False,
) -> irast.Command:
    """Compile given EdgeQL AST into EdgeDB IR."""

    if debug.flags.edgeql_compile:
        debug.header('EdgeQL AST')
        debug.dump(tree, schema=schema)

    ctx = stmtctx.init_context(
        schema=schema,
        anchors=anchors,
        singletons=singletons,
        modaliases=modaliases,
        func_params=func_params,
        derived_target_module=derived_target_module,
        result_view_name=result_view_name,
        implicit_id_in_shapes=implicit_id_in_shapes,
        implicit_tid_in_shapes=implicit_tid_in_shapes,
        schema_view_mode=schema_view_mode,
        disable_constant_folding=disable_constant_folding,
        json_parameters=json_parameters,
        session_mode=session_mode,
        allow_abstract_operators=allow_abstract_operators,
        allow_generic_type_output=allow_generic_type_output,
        parent_object_type=parent_object_type,
    )

    if path_prefix_anchor is not None:
        assert anchors is not None
        path_prefix = anchors[path_prefix_anchor]
        assert isinstance(path_prefix, s_types.Type)
        ctx.partial_path_prefix = setgen.class_set(path_prefix, ctx=ctx)
        ctx.partial_path_prefix.anchor = path_prefix_anchor
        ctx.partial_path_prefix.show_as_anchor = path_prefix_anchor

    ir_set = dispatch.compile(tree, ctx=ctx)
    ir_expr = stmtctx.fini_expression(ir_set, ctx=ctx)

    if ctx.env.query_parameters:
        first_argname = next(iter(ctx.env.query_parameters))
        if first_argname.isdecimal():
            args_decnames = {int(arg) for arg in ctx.env.query_parameters}
            args_tpl = set(range(len(ctx.env.query_parameters)))
            if args_decnames != args_tpl:
                missing_args = args_tpl - args_decnames
                missing_args_repr = ', '.join(f'${a}' for a in missing_args)
                raise errors.QueryError(
                    f'missing {missing_args_repr} positional argument'
                    f'{"s" if len(missing_args) > 1 else ""}')

    if debug.flags.edgeql_compile:
        debug.header('Scope Tree')
        if ctx.path_scope is not None:
            print(ctx.path_scope.pdebugformat())
        else:
            print('N/A')
        debug.header('EdgeDB IR')
        debug.dump(ir_expr, schema=getattr(ir_expr, 'schema', None))

    return ir_expr


def compile_ast_fragment_to_ir(
    tree: qlast.Base,
    schema: s_schema.Schema,
    *,
    location: Optional[str] = None,
    anchors: Optional[
        Mapping[Union[str, qlast.SpecialAnchorT], s_obj.Object]
    ] = None,
    path_prefix_anchor: Optional[qlast.SpecialAnchorT] = None,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
) -> irast.Statement:
    """Compile given EdgeQL AST fragment into EdgeDB IR."""
    ctx = stmtctx.init_context(
        schema=schema, anchors=anchors, modaliases=modaliases)
    ctx.clause = location or 'where'
    if path_prefix_anchor is not None:
        assert anchors is not None
        path_prefix = anchors[path_prefix_anchor]
        assert isinstance(path_prefix, s_types.Type)
        ctx.partial_path_prefix = setgen.class_set(path_prefix, ctx=ctx)
        ctx.partial_path_prefix.anchor = path_prefix_anchor
        ctx.partial_path_prefix.show_as_anchor = path_prefix_anchor

    ir_set = dispatch.compile(tree, ctx=ctx)

    result_type: Optional[s_types.Type]
    try:
        result_type = inference.infer_type(ir_set, ctx.env)
    except errors.QueryError:
        # Not all fragments can be resolved into a concrete type,
        # that's OK.
        result_type = None

    return irast.Statement(expr=ir_set, schema=ctx.env.schema,
                           stype=result_type)


def evaluate_to_python_val(
    expr: str,
    schema: s_schema.Schema,
    *,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
) -> Any:
    tree = ql_parser.parse_fragment(expr)
    return evaluate_ast_to_python_val(tree, schema, modaliases=modaliases)


def evaluate_ast_to_python_val(
    tree: qlast.Base,
    schema: s_schema.Schema,
    *,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
) -> Any:
    ir = compile_ast_fragment_to_ir(tree, schema, modaliases=modaliases)
    return ireval.evaluate_to_python_val(ir.expr, schema=ir.schema)


def get_param_anchors_for_callable(
    params: s_func.ParameterLikeList,
    schema: s_schema.Schema,
    *,
    inlined_defaults: bool,
) -> Tuple[
    Dict[str, irast.Parameter],
    List[qlast.AliasedExpr],
]:
    anchors = {}
    aliases = []

    if inlined_defaults:
        anchors['__defaults_mask__'] = irast.Parameter(
            name='__defaults_mask__',
            typeref=irtyputils.type_to_typeref(
                schema,
                cast(s_scalars.ScalarType, schema.get('std::bytes')),
            ),
        )

    pg_params = s_func.PgParams.from_params(schema, params)
    for pi, p in enumerate(pg_params.params):
        p_shortname = p.get_shortname(schema)
        anchors[p_shortname] = irast.Parameter(
            name=p_shortname,
            typeref=irtyputils.type_to_typeref(schema, p.get_type(schema)))

        if p.get_default(schema) is None:
            continue

        if not inlined_defaults:
            continue

        aliases.append(
            qlast.AliasedExpr(
                alias=p_shortname,
                expr=qlast.IfElse(
                    condition=qlast.BinOp(
                        left=qlast.FunctionCall(
                            func=('std', 'bytes_get_bit'),
                            args=[
                                qlast.Path(steps=[
                                    qlast.ObjectRef(
                                        name='__defaults_mask__')
                                ]),
                                qlast.IntegerConstant(value=str(pi)),
                            ]),
                        right=qlast.IntegerConstant(value='0'),
                        op='='),
                    if_expr=qlast.Path(
                        steps=[qlast.ObjectRef(name=p_shortname)]),
                    else_expr=qlast._Optional(expr=p.get_ql_default(schema)))))

    return anchors, aliases


def compile_func_to_ir(
    func: s_func.Function,
    schema: s_schema.Schema,
) -> irast.Statement:
    """Compile an EdgeQL function into EdgeDB IR."""

    if debug.flags.edgeql_compile:
        debug.header('EdgeQL Function')
        debug.print(func.get_code(schema))

    code = func.get_code(schema)
    assert code is not None
    trees = ql_parser.parse_block(code + ';')
    if len(trees) != 1:
        raise errors.InvalidFunctionDefinitionError(
            'functions can only contain one statement')

    tree = trees[0]

    param_anchors, param_aliases = get_param_anchors_for_callable(
        func.get_params(schema), schema,
        inlined_defaults=func.has_inlined_defaults(schema))

    tree.aliases.extend(param_aliases)

    ir = compile_ast_to_ir(
        tree, schema,
        anchors=param_anchors,  # type: ignore
                                # (typing#273)
        func_params=func.get_params(schema),
        # the body of a session_only function can contain calls to
        # other session_only functions
        session_mode=func.get_session_only(schema),
    )

    assert isinstance(ir, irast.Statement)

    return_type = func.get_return_type(schema)
    if (not ir.stype.issubclass(schema, return_type)
            and not ir.stype.implicitly_castable_to(return_type, schema)):
        raise errors.InvalidFunctionDefinitionError(
            f'return type mismatch in function declared to return '
            f'{return_type.get_verbosename(schema)}',
            details=f'Actual return type is '
                    f'{ir.stype.get_verbosename(schema)}',
            context=tree.context,
        )

    return_typemod = func.get_return_typemod(schema)
    if (return_typemod is not qltypes.TypeModifier.SET_OF
            and ir.cardinality is qltypes.Cardinality.MANY):
        raise errors.InvalidFunctionDefinitionError(
            f'return cardinality mismatch in function declared to return '
            f'a singleton',
            details=f'Function may return a set with more than one element.',
            context=tree.context,
        )

    return ir


def compile_constant_tree_to_ir(
    const: qlast.BaseConstant,
    schema: s_schema.Schema,
    *,
    styperef: Optional[irast.TypeRef] = None,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
) -> irast.Expr:

    ctx = stmtctx.init_context(
        schema=schema,
        modaliases=modaliases,
    )

    if not isinstance(const, qlast.BaseConstant):
        raise ValueError(f'unexpected input: {const!r} is not a constant')

    ir_set = dispatch.compile(const, ctx=ctx)
    assert isinstance(ir_set, irast.Set)
    result = ir_set.expr
    assert isinstance(result, irast.BaseConstant)
    if styperef is not None and result.typeref.id != styperef.id:
        result = type(result)(value=result.value, typeref=styperef)

    return result
