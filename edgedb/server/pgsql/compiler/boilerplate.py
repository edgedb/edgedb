##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.server.pgsql import ast as pgast

from . import context


def init_stmt(
        stmt: pgast.Query, ctx: context.CompilerContext,
        parent_ctx: context.CompilerContext) -> None:
    if ctx.toplevel_stmt is None:
        ctx.toplevel_stmt = ctx.stmt

    ctx.stmt_path_scope = stmt.path_scope.copy()

    ctx.stmt_specific_path_scope = \
        {s for s in stmt.specific_path_scope
         if s.path_id in ctx.stmt_path_scope}

    if stmt.parent_stmt is not None:
        ctx.parent_stmt_path_scope = stmt.parent_stmt.path_scope.copy()

    ctx.stmtmap[stmt] = ctx.stmt
    ctx.stmt_hierarchy[ctx.stmt] = parent_ctx.stmt

    ctx.stmt.view_path_id_map = parent_ctx.view_path_id_map.copy()


def fini_stmt(
        stmt: pgast.Query, ctx: context.CompilerContext,
        parent_ctx: context.CompilerContext) -> None:
    if stmt is ctx.toplevel_stmt:
        stmt.argnames = ctx.argmap
