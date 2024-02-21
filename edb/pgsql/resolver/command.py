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

from typing import Optional, Dict, List

from edb.pgsql import ast as pgast

from . import dispatch
from . import context

Context = context.ResolverContextLevel


@dispatch._resolve.register
def resolve_CopyStmt(stmt: pgast.CopyStmt, *, ctx: Context) -> pgast.CopyStmt:

    # Query
    query = dispatch.resolve_opt(stmt.query, ctx=ctx)
    relation: Optional[pgast.Relation] = None
    col_names: Optional[List[str]] = None

    if stmt.relation:
        # A table is going to be copied, which potentially is a view that
        # cannot be copied as a table, but needs to be wrapped into a
        # `SELECT * FROM view`.
        relation, table = dispatch.resolve_relation(stmt.relation, ctx=ctx)
        col_map: Dict[str, str] = {
            col.name: col.reference_as
            for col in table.columns
            if col.name and col.reference_as
        }
        if stmt.colnames:
            col_names = [col_map[name] for name in stmt.colnames]

        if relation.schemaname == 'edgedbpub':
            # This is probably a view based on edgedb schema, so wrap it into
            # a select query.
            if not col_names:
                # Avoid adding the system columns here. Also order the columns
                # the same way as in the information_schema: id first, then
                # alphabetically.
                target_list = [col_map['id']]
                for col_name, real_col in sorted(col_map.items()):
                    if col_name not in {'id', 'tableoid', 'xmin', 'cmin',
                                        'xmax', 'cmax', 'ctid'}:
                        target_list.append(real_col)
            else:
                target_list = col_names
            query = pgast.SelectStmt(
                from_clause=[pgast.RelRangeVar(relation=relation)],
                target_list=[
                    pgast.ResTarget(
                        val=pgast.ColumnRef(name=(cn,)),
                    )
                    for cn in target_list
                ],
            )
            relation = None

    # WHERE
    where = dispatch.resolve_opt(stmt.where_clause, ctx=ctx)

    return pgast.CopyStmt(
        relation=None,
        colnames=col_names,
        query=query,
        is_from=stmt.is_from,
        is_program=stmt.is_program,
        filename=stmt.filename,
        # TODO: forbid some options?
        options=stmt.options,
        where_clause=where,
    )
