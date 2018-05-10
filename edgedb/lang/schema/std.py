#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
