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


from edb import errors

from edb.edgeql import parser as ql_parser

from edb.common import debug
from edb.common import markup  # NOQA

from edb.schema import functions as s_func

from edb.edgeql import ast as qlast
from edb.ir import ast as irast
from edb.ir import staeval as ireval
from edb.ir import typeutils as irtyputils

from .decompiler import decompile_ir  # NOQA
from . import dispatch
from . import inference
from . import stmtctx

from . import expr as _expr_compiler  # NOQA
from . import stmt as _stmt_compiler  # NOQA


def compile_fragment_to_ir(expr,
                           schema,
                           *,
                           anchors=None,
                           location=None,
                           modaliases=None):
    """Compile given EdgeQL expression fragment into EdgeDB IR."""
    tree = ql_parser.parse_fragment(expr)
    return compile_ast_fragment_to_ir(
        tree, schema, anchors=anchors,
        location=location, modaliases=modaliases)


def compile_ast_fragment_to_ir(tree,
                               schema,
                               *,
                               anchors=None,
                               location=None,
                               modaliases=None):
    """Compile given EdgeQL AST fragment into EdgeDB IR."""
    ctx = stmtctx.init_context(
        schema=schema, anchors=anchors, modaliases=modaliases)
    ctx.clause = location or 'where'
    ir_set = dispatch.compile(tree, ctx=ctx)
    try:
        result_type = inference.infer_type(ir_set, ctx.env)
    except errors.QueryError:
        # Not all fragments can be resolved into a concrete type,
        # that's OK.
        result_type = None

    return irast.Statement(expr=ir_set, schema=ctx.env.schema,
                           stype=result_type)


def compile_to_ir(expr,
                  schema,
                  *,
                  anchors=None,
                  security_context=None,
                  modaliases=None,
                  implicit_id_in_shapes=False,
                  implicit_tid_in_shapes=False):
    """Compile given EdgeQL statement into EdgeDB IR."""

    if debug.flags.edgeql_compile:
        debug.header('EdgeQL TEXT')
        debug.print(expr)

    tree = ql_parser.parse(expr, modaliases)

    return compile_ast_to_ir(
        tree, schema, anchors=anchors,
        security_context=security_context, modaliases=modaliases,
        implicit_id_in_shapes=implicit_id_in_shapes,
        implicit_tid_in_shapes=implicit_tid_in_shapes)


def compile_ast_to_ir(tree,
                      schema,
                      *,
                      anchors=None,
                      singletons=None,
                      func=None,
                      security_context=None,
                      derived_target_module=None,
                      result_view_name=None,
                      modaliases=None,
                      implicit_id_in_shapes=False,
                      implicit_tid_in_shapes=False,
                      schema_view_mode=False,
                      disable_constant_folding=False,
                      json_parameters=False):
    """Compile given EdgeQL AST into EdgeDB IR."""

    if debug.flags.edgeql_compile:
        debug.header('EdgeQL AST')
        debug.dump(tree, schema=schema)

    ctx = stmtctx.init_context(
        schema=schema, anchors=anchors, singletons=singletons,
        modaliases=modaliases, security_context=security_context,
        func=func, derived_target_module=derived_target_module,
        result_view_name=result_view_name,
        implicit_id_in_shapes=implicit_id_in_shapes,
        implicit_tid_in_shapes=implicit_tid_in_shapes,
        schema_view_mode=schema_view_mode,
        disable_constant_folding=disable_constant_folding,
        json_parameters=json_parameters)

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


def evaluate_to_python_val(expr, schema, *, modaliases=None) -> object:
    tree = ql_parser.parse_fragment(expr)
    return evaluate_ast_to_python_val(tree, schema, modaliases=modaliases)


def evaluate_ast_to_python_val(tree, schema, *, modaliases=None) -> object:
    ir = compile_ast_fragment_to_ir(tree, schema, modaliases=modaliases)
    return ireval.evaluate_to_python_val(ir.expr, schema=ir.schema)


def compile_func_to_ir(func, schema, *,
                       anchors=None,
                       security_context=None,
                       modaliases=None,
                       implicit_id_in_shapes=False,
                       implicit_tid_in_shapes=False):
    """Compile an EdgeQL function into EdgeDB IR."""

    if debug.flags.edgeql_compile:
        debug.header('EdgeQL Function')
        debug.print(func.get_code(schema))

    trees = ql_parser.parse_block(func.get_code(schema) + ';')
    if len(trees) != 1:
        raise errors.InvalidFunctionDefinitionError(
            'functions can only contain one statement')

    tree = trees[0]
    if modaliases:
        ql_parser.append_module_aliases(tree, modaliases)

    if anchors is None:
        anchors = {}

    anchors['__defaults_mask__'] = irast.Parameter(
        name='__defaults_mask__',
        typeref=irtyputils.type_to_typeref(schema, schema.get('std::bytes')))

    func_params = func.get_params(schema)
    pg_params = s_func.PgParams.from_params(schema, func_params)
    for pi, p in enumerate(pg_params.params):
        p_shortname = p.get_shortname(schema)
        anchors[p_shortname] = irast.Parameter(
            name=p_shortname,
            typeref=irtyputils.type_to_typeref(schema, p.get_type(schema)))

        if p.get_default(schema) is None:
            continue

        tree.aliases.append(
            qlast.AliasedExpr(
                alias=p_shortname,
                expr=qlast.IfElse(
                    condition=qlast.BinOp(
                        left=qlast.FunctionCall(
                            func=('std', 'bytes_get_bit'),
                            args=[
                                qlast.FuncArg(
                                    arg=qlast.Path(steps=[
                                        qlast.ObjectRef(
                                            name='__defaults_mask__')
                                    ])),
                                qlast.FuncArg(
                                    arg=qlast.IntegerConstant(value=str(pi)))
                            ]),
                        right=qlast.IntegerConstant(value='0'),
                        op='='),
                    if_expr=qlast.Path(
                        steps=[qlast.ObjectRef(name=p_shortname)]),
                    else_expr=qlast._Optional(expr=p.get_ql_default(schema)))))

    ir = compile_ast_to_ir(
        tree, schema, anchors=anchors, func=func,
        security_context=security_context, modaliases=modaliases,
        implicit_id_in_shapes=implicit_id_in_shapes,
        implicit_tid_in_shapes=implicit_tid_in_shapes)

    return ir


def compile_constant_tree_to_ir(
        const, schema, *, styperef=None, modaliases=None):

    ctx = stmtctx.init_context(
        schema=schema,
        modaliases=modaliases,
    )

    if not isinstance(const, qlast.BaseConstant):
        raise ValueError(f'unexpected input: {const!r} is not a constant')

    ir_set = dispatch.compile(const, ctx=ctx)
    result = ir_set.expr
    if styperef is not None and result.typeref.id != styperef.id:
        result = type(result)(value=result.value, typeref=styperef)

    return result
