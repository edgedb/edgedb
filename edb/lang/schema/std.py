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


import pathlib
import typing

from edb import lib as stdlib
from edb.lang import edgeql
from edb.lang.edgeql import ast as qlast

from . import ddl as s_ddl
from . import delta as sd
from . import error as s_err
from . import schema as s_schema


LIB_ROOT = pathlib.Path(stdlib.__path__[0])
STD_MODULES = ['std', 'schema']


def std_module_to_ddl(
        schema: s_schema.Schema,
        modname: str) -> typing.List[qlast.DDL]:

    module_eql = ''

    module_path = LIB_ROOT / modname
    module_files = []

    if module_path.is_dir():
        for entry in module_path.iterdir():
            if entry.is_file() and entry.suffix == '.eql':
                module_files.append(entry)
    else:
        module_path = module_path.with_suffix('.eql')
        if not module_path.exists():
            raise s_err.SchemaError(f'std module not found: {modname}')
        module_files.append(module_path)

    module_files.sort(key=lambda p: p.name)

    for module_file in module_files:
        with open(module_file) as f:
            module_eql += '\n' + f.read()

    return edgeql.parse_block(module_eql)


def load_std_module(
        schema: s_schema.Schema, modname: str) -> s_schema.Schema:

    modaliases = {}
    if modname == 'std':
        modaliases[None] = 'std'

    for statement in std_module_to_ddl(schema, modname):
        cmd = s_ddl.delta_from_ddl(
            statement, schema=schema, modaliases=modaliases, stdmode=True)
        cmd.apply(schema)

    return schema


def load_std_schema(
        schema: typing.Optional[s_schema.Schema]=None) -> s_schema.Schema:
    if schema is None:
        schema = s_schema.Schema()

    for modname in STD_MODULES:
        load_std_module(schema, modname)

    return schema


def load_graphql_schema(
        schema: typing.Optional[s_schema.Schema]=None) -> s_schema.Schema:
    if schema is None:
        schema = s_schema.Schema()

    return load_std_module(schema, 'stdgraphql')


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
