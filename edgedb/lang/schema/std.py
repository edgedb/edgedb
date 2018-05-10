##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path

from edgedb.lang import edgeql
from edgedb.lang.edgeql import ast as qlast

from . import ddl as s_ddl
from . import schema as s_schema
from . import delta as sd


def load_std_schema():
    schema = s_schema.Schema()

    std_eql_f = os.path.join(os.path.dirname(__file__), '_std.eql')
    with open(std_eql_f) as f:
        std_eql = f.read()

    statements = edgeql.parse_block(std_eql)

    for statement in statements:
        cmd = s_ddl.delta_from_ddl(
            statement, schema=schema, modaliases={None: 'std'})
        cmd.apply(schema)

    return schema


def load_graphql_schema(schema=None):
    if schema is None:
        schema = s_schema.Schema()

    with open(os.path.join(os.path.dirname(__file__),
              '_graphql.eschema'), 'r') as f:
        eschema = f.read()

    script = f'''
        CREATE MODULE graphql;
        CREATE MIGRATION graphql::d0 TO eschema $${eschema}$$;
        COMMIT MIGRATION graphql::d0;
    '''
    statements = edgeql.parse_block(script)
    for stmt in statements:
        if isinstance(stmt, qlast.Delta):
            # CREATE/APPLY MIGRATION
            ddl_plan = s_ddl.cmd_from_ddl(stmt, schema=schema, modaliases={})

        elif isinstance(stmt, qlast.DDL):
            # CREATE/DELETE/ALTER (FUNCTION, TYPE, etc)
            ddl_plan = s_ddl.delta_from_ddl(stmt, schema=schema, modaliases={})

        context = sd.CommandContext()
        ddl_plan.apply(schema, context)

    return schema


def load_default_schema(schema=None):
    if schema is None:
        schema = s_schema.Schema()

    script = f'''
        CREATE MODULE default;
    '''
    statements = edgeql.parse_block(script)
    for stmt in statements:
        if isinstance(stmt, qlast.Delta):
            # CREATE/APPLY MIGRATION
            ddl_plan = s_ddl.cmd_from_ddl(stmt, schema=schema, modaliases={})

        elif isinstance(stmt, qlast.DDL):
            # CREATE/DELETE/ALTER (FUNCTION, TYPE, etc)
            ddl_plan = s_ddl.delta_from_ddl(stmt, schema=schema, modaliases={})

        context = sd.CommandContext()
        ddl_plan.apply(schema, context)

    return schema
