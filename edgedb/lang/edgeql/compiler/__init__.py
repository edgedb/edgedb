##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
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
                  derived_target_module=None,
                  modaliases=None):
    """Compile given EdgeQL statement into EdgeDB IR."""

    if debug.flags.edgeql_compile:
        debug.header('EdgeQL TEXT')
        debug.print(expr)

    tree = parser.parse(expr, modaliases)

    return compile_ast_to_ir(
        tree, schema, anchors=anchors, arg_types=arg_types,
        security_context=security_context, modaliases=modaliases,
        derived_target_module=derived_target_module)


def compile_ast_to_ir(tree,
                      schema,
                      *,
                      anchors=None,
                      arg_types=None,
                      security_context=None,
                      derived_target_module=None,
                      modaliases=None):
    """Compile given EdgeQL AST into EdgeDB IR."""

    if debug.flags.edgeql_compile:
        debug.header('EdgeQL AST')
        debug.dump(tree)

    ctx = stmtctx.init_context(
        schema=schema, anchors=anchors, modaliases=modaliases,
        security_context=security_context, arg_types=arg_types,
        derived_target_module=derived_target_module)

    ir = dispatch.compile(tree, ctx=ctx)

    if debug.flags.edgeql_compile:
        if ir.path_scope:
            debug.header('Scope Tree')
            print(ir.path_scope.pformat())
        debug.header('EdgeDB IR')
        debug.dump(ir)

    return ir
