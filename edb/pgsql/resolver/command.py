#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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

"""SQL resolver that compiles public SQL to internal SQL which is executable
in our internal Postgres instance."""

from typing import *

from edb import errors

from edb.pgsql import ast as pgast

from . import dispatch
from . import context
from . import static

Context = context.ResolverContextLevel


@dispatch._resolve.register
def resolve_CopyStmt(
    stmt: pgast.CopyStmt, *, ctx: Context
) -> pgast.CopyStmt:

    # Query
    query = dispatch.resolve_opt(stmt.query, ctx=ctx)

    if query is None:
        # A table is going to be copied, which potentially is a view that
        # cannot be copied as a table, but needs to be converted to a `SELECT
        # ...`
        relation, table = dispatch.resolve_relation(stmt.relation, ctx=ctx)
        colnames: List[str] = []
        if stmt.colnames:
            colmap = {col.name: col.reference_as for col in table.columns}
            colnames = [colmap[name] for name in stmt.colnames]

        if relation.schemaname == 'edgedbpub':
            # This is probably a view based on edgedb schema, so convert it to
            # a select query.
            target_list: List[pgast.ResTarget] = []
            for colname in stmt.colnames:
                target_list.append(
                    pgast.ResTarget(
                        val=pgast.ColumnRef(
                            name=(colmap[colname],)
                        ),
                    )
                )

            query = pgast.SelectStmt(
                from_clause=[pgast.RelRangeVar(relation=relation)],
                target_list=target_list,
            )
            relation = None

    # WHERE
    where = dispatch.resolve_opt(stmt.where_clause, ctx=ctx)

    return pgast.CopyStmt(
        relation=relation,
        colnames=colnames,
        query=query,
        is_from=stmt.is_from,
        is_program=stmt.is_program,
        filename=stmt.filename,
        # FIXME: options are currently not handled
        where_clause=where,
    )
