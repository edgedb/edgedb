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
    query: pgast.Base,
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

    resolved = dispatch.resolve(query, ctx=ctx)

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
        if isinstance(edgeql_output_format_ast, pgast.SelectStmt):
            edgeql_output_format_ast.target_list = [
                pgast.ResTarget(
                    val=pgast.RowExpr(
                        args=[
                            rt.val
                            for rt in edgeql_output_format_ast.target_list
                        ]
                    )
                )
            ]
    else:
        edgeql_output_format_ast = None

    return ResolvedSQL(
        ast=resolved,
        edgeql_output_format_ast=edgeql_output_format_ast,
        command_complete_tag=command_complete_tag,
        params=ctx.query_params,
    )
