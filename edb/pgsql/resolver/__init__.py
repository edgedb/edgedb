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

from __future__ import annotations
from typing import Optional, List

import copy
import dataclasses

from edb.common import debug
from edb.pgsql import ast as pgast
from edb.pgsql import codegen as pgcodegen
from edb.schema import schema as s_schema

from edb.server.compiler import dbstate

from . import dispatch
from . import context
from . import expr  # NOQA
from . import relation  # NOQA
from . import command  # NOQA

Options = context.Options


@dataclasses.dataclass(kw_only=True, eq=False, frozen=True, repr=False)
class ResolvedSQL:
    # AST representing the query that can be sent to PostgreSQL
    ast: pgast.Base

    # Optionally, AST representing the query returning data in EdgeQL
    # format (i.e. single-column output).
    edgeql_output_format_ast: Optional[pgast.Base]

    # Special behavior for "tag" of "CommandComplete" message of this query.
    command_complete_tag: Optional[dbstate.CommandCompleteTag]

    # query parameters
    params: List[dbstate.SQLParam]


def resolve(
    query: pgast.Query | pgast.CopyStmt,
    schema: s_schema.Schema,
    options: context.Options,
) -> ResolvedSQL:

    if debug.flags.sql_input:
        debug.header('SQL Input')

        debug_sql_text = pgcodegen.generate_source(
            query, reordered=True, pretty=True
        )
        debug.dump_code(debug_sql_text, lexer='sql')

    ctx = context.ResolverContextLevel(
        None, context.ContextSwitchMode.EMPTY, schema=schema, options=options
    )

    _ = context.ResolverContext(initial=ctx)

    command.init_external_params(query, ctx)
    top_level_ctes = command.compile_dml(query, ctx=ctx)

    resolved: pgast.Base
    if isinstance(query, pgast.Query):
        resolved, resolved_table = dispatch.resolve_relation(query, ctx=ctx)
    elif isinstance(query, pgast.CopyStmt):
        resolved = dispatch.resolve(query, ctx=ctx)
        resolved_table = None
    else:
        raise AssertionError()

    if limit := ctx.options.implicit_limit:
        resolved = apply_implicit_limit(resolved, limit, resolved_table, ctx)

    command.fini_external_params(ctx)

    if top_level_ctes:
        assert isinstance(resolved, pgast.Query)
        if not resolved.ctes:
            resolved.ctes = []
        resolved.ctes.extend(top_level_ctes)

    # when the top-level query is DML statement, clients will expect a tag in
    # the CommandComplete message that describes the number of modified rows.
    # Since our resolved SQL does not have a top-level DML stmt, we need to
    # override that tag.
    command_complete_tag: Optional[dbstate.CommandCompleteTag] = None
    if isinstance(query, pgast.DMLQuery):
        prefix: str
        if isinstance(query, pgast.InsertStmt):
            prefix = 'INSERT 0 '
        elif isinstance(query, pgast.DeleteStmt):
            prefix = 'DELETE '
        elif isinstance(query, pgast.UpdateStmt):
            prefix = 'UPDATE '

        if query.returning_list:
            # resolved SQL will return a result, we count those rows
            command_complete_tag = dbstate.TagCountMessages(prefix=prefix)
        else:
            # resolved SQL will contain an injected COUNT clause
            # we instruct io process to unpack that
            command_complete_tag = dbstate.TagUnpackRow(prefix=prefix)

    if debug.flags.sql_output:
        debug.header('SQL Output')

        debug_sql_text = pgcodegen.generate_source(
            resolved, pretty=True, reordered=True
        )
        debug.dump_code(debug_sql_text, lexer='sql')

    if options.include_edgeql_io_format_alternative:
        edgeql_output_format_ast = copy.copy(resolved)
        if e := as_plain_select(edgeql_output_format_ast, resolved_table, ctx):
            # Turn the query into one that returns a ROW.
            #
            # We need to do this by injecting a new query and putting
            # the old one in its FROM clause, since things like
            # DISTINCT/ORDER BY care about what exact columns are in
            # the target list.
            columns = []
            for i, target in enumerate(e.target_list):
                if not target.name:
                    e.target_list[i] = target = target.replace(name=f'__i~{i}')
                    assert target.name
                columns.append(pgast.ColumnRef(name=(target.name,)))

            edgeql_output_format_ast = pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(
                        val=expr.construct_row_expr(columns, ctx=ctx)
                    )
                ],
                from_clause=[pgast.RangeSubselect(subquery=e)],
                ctes=e.ctes,
            )
            e.ctes = []
    else:
        edgeql_output_format_ast = None

    return ResolvedSQL(
        ast=resolved,
        edgeql_output_format_ast=edgeql_output_format_ast,
        command_complete_tag=command_complete_tag,
        params=ctx.query_params,
    )


def as_plain_select(
    query: pgast.Base,
    table: Optional[context.Table],
    ctx: context.ResolverContextLevel,
) -> Optional[pgast.SelectStmt]:
    if not isinstance(query, pgast.Query):
        return None
    assert table

    if (
        isinstance(query, pgast.SelectStmt)
        and not query.op
        and not query.values
    ):
        return query

    table.alias = "t"
    return pgast.SelectStmt(
        from_clause=[
            pgast.RangeSubselect(
                subquery=query,
                alias=pgast.Alias(aliasname="t"),
            )
        ],
        target_list=[
            pgast.ResTarget(
                name=f'column{index + 1}',
                val=expr.resolve_column_kind(table, c.kind, ctx=ctx),
            )
            for index, c in enumerate(table.columns)
        ],
    )


def apply_implicit_limit(
    expr: pgast.Base,
    limit: int,
    table: Optional[context.Table],
    ctx: context.ResolverContextLevel,
) -> pgast.Base:
    e = as_plain_select(expr, table, ctx)
    if not e:
        return expr

    if e.limit_count is None:
        e.limit_count = pgast.NumericConstant(val=str(limit))
    return e
