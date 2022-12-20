import asyncpg
import os
import asyncio
import json


async def run():

    localdev = os.path.expanduser('~/.local/share/edgedb/_localdev')

    c = await asyncpg.connect(
        host=localdev, database='postgres', user='postgres'
    )

    res = await c.fetch(
        '''
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

    pos = 0
    with open('./edb/pgsql/resolver/sql_introspection.py', 'r') as file:
        while True:
            line = file.readline()
            if 'INFORMATION_SCHEMA' in line:
                break
            else:
                pos += len(line)

    with open('./edb/pgsql/resolver/sql_introspection.py', 'r+') as file:
        file.seek(pos)

        print_schema(file, "INFORMATION_SCHEMA", schema['information_schema'])
        print_schema(file, "PG_CATALOG", schema['pg_catalog'])

        file.flush()
        file.close()


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
    print('', file=f)


asyncio.run(run())
