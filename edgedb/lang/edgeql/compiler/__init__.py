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


from edgedb.lang.edgeql import parser
from edgedb.lang.common import debug
from edgedb.lang.common import markup  # NOQA

from .decompiler import decompile_ir  # NOQA
from . import dispatch
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
    tree = parser.parse_fragment(expr)
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
    return dispatch.compile(tree, ctx=ctx)


def compile_to_ir(expr,
                  schema,
                  *,
                  anchors=None,
                  arg_types=None,
                  security_context=None,
                  modaliases=None,
                  implicit_id_in_shapes=True):
    """Compile given EdgeQL statement into EdgeDB IR."""

    if debug.flags.edgeql_compile:
        debug.header('EdgeQL TEXT')
        debug.print(expr)

    tree = parser.parse(expr, modaliases)

    return compile_ast_to_ir(
        tree, schema, anchors=anchors, arg_types=arg_types,
        security_context=security_context, modaliases=modaliases,
        implicit_id_in_shapes=implicit_id_in_shapes)


def compile_ast_to_ir(tree,
                      schema,
                      *,
                      anchors=None,
                      arg_types=None,
                      security_context=None,
                      derived_target_module=None,
                      result_view_name=None,
                      modaliases=None,
                      implicit_id_in_shapes=True):
    """Compile given EdgeQL AST into EdgeDB IR."""

    if debug.flags.edgeql_compile:
        debug.header('EdgeQL AST')
        debug.dump(tree)

    ctx = stmtctx.init_context(
        schema=schema, anchors=anchors, modaliases=modaliases,
        security_context=security_context, arg_types=arg_types,
        derived_target_module=derived_target_module,
        result_view_name=result_view_name,
        implicit_id_in_shapes=implicit_id_in_shapes)

    ir_set = dispatch.compile(tree, ctx=ctx)
    ir_expr = stmtctx.fini_expression(ir_set, ctx=ctx)

    if debug.flags.edgeql_compile:
        debug.header('Scope Tree')
        if ctx.path_scope is not None:
            print(ctx.path_scope.pdebugformat())
        else:
            print('N/A')
        debug.header('EdgeDB IR')
        debug.dump(ir_expr)

    return ir_expr
