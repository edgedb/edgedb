#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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

import asyncio
import json
import textwrap

from edb.tools.edb import edbcommands


async def run():
    import asyncpg

    # import os
    # localdev = os.path.expanduser('~/.local/share/edgedb/_localdev')
    # c = await asyncpg.connect(
    #     host=localdev, database='postgres', user='postgres'
    # )

    # docker run -it -p 5433:5432 --rm -e POSTGRES_PASSWORD=pass postgres:13
    c = await asyncpg.connect(
        host='localhost',
        database='postgres',
        user='postgres',
        password='pass',
        port='5433',
    )

    res = await c.fetch(
        r'''
    WITH
    columns AS (
        SELECT
            table_schema,
            table_name,
            TO_JSON(ARRAY [
                column_name, COALESCE(domain_name, data_type)
            ]) AS col
        FROM information_schema.columns
        WHERE (
            table_schema = 'information_schema' AND table_name NOT LIKE '\_pg%'
        ) OR (
            table_schema = 'pg_catalog'
        )
        ORDER BY table_schema, table_name, ordinal_position
    ),
    tables AS (
        SELECT table_schema, table_name, JSON_AGG(col) as cols
        FROM columns
        GROUP BY table_schema, table_name
    ),
    schemas AS (
        SELECT table_schema, JSON_OBJECT_AGG(table_name, cols) as tables
        FROM tables
        GROUP BY table_schema
    )
    SELECT JSON_OBJECT_AGG(table_schema, tables)
    FROM schemas
    '''
    )

    schema_json = res[0][0]
    schema = json.loads(schema_json)

    with open('./edb/pgsql/resolver/sql_introspection.py', 'w') as file:
        print_header(file)
        print_schema(file, "INFORMATION_SCHEMA", schema['information_schema'])
        print_schema(file, "PG_CATALOG", schema['pg_catalog'])


def print_header(f):
    print(
        textwrap.dedent(
            '''
        # DO NOT EDIT. Generated from _localdev postgres instance by running
        #    $ edb gen-sql-introspection

        """Declarations of information schema and pg_catalog"""

        from typing import *  # NoQA

        ColumnName = str
        ColumnType = str | None
        '''
        )[1:],
        file=f,
    )


def print_schema(f, name, tables):
    typ = ': Dict[str, List[Tuple[ColumnName, ColumnType]]]'
    print(name + typ + " = {", file=f)
    for (index, (table, columns)) in enumerate(tables.items()):
        print(f'    "{table}": [', file=f)
        for [col_name, col_typ] in columns:
            if col_typ == "ARRAY" or col_typ.startswith("any"):
                col_typ = "None"
            else:
                col_typ = col_typ.replace('"', '\\"')
                col_typ = f'"{col_typ}"'
            print(f'        ("{col_name}", {col_typ}),', file=f)

        last = index + 1 == len(tables)
        comma = ',' if not last else ''
        print(f'    ]{comma}', file=f)
    print('}', file=f)


@edbcommands.command("gen-sql-introspection")
def main():
    asyncio.run(run())
