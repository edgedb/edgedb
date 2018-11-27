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
from edb import errors

from edb.lang import edgeql
from edb.lang import schema
from edb.lang.common import devmode
from edb.lang.edgeql import compiler as qlcompiler
from edb.lang.schema import delta as s_delta

from . import ddl as s_ddl
from . import schema as s_schema


SCHEMA_ROOT = pathlib.Path(schema.__path__[0])
LIB_ROOT = pathlib.Path(stdlib.__path__[0])
QL_COMPILER_ROOT = pathlib.Path(qlcompiler.__path__[0])

CACHE_SRC_DIRS = (
    (SCHEMA_ROOT, '.py'),
    (QL_COMPILER_ROOT, '.py'),
    (LIB_ROOT, '.eql'),
)


def get_std_module_text(modname: str) -> str:

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
            raise errors.SchemaError(f'std module not found: {modname}')
        module_files.append(module_path)

    module_files.sort(key=lambda p: p.name)

    for module_file in module_files:
        with open(module_file) as f:
            module_eql += '\n' + f.read()

    return module_eql


def load_std_module(
        schema: s_schema.Schema, modname: str) -> s_schema.Schema:

    modaliases = {}
    if modname == 'std':
        modaliases[None] = 'std'

    context = s_delta.CommandContext()
    context.stdmode = True

    modtext = get_std_module_text(modname)
    for statement in edgeql.parse_block(modtext):
        cmd = s_ddl.delta_from_ddl(
            statement, schema=schema, modaliases=modaliases, stdmode=True)
        schema, _ = cmd.apply(schema, context)

    return schema


def load_std_schema() -> s_schema.Schema:
    std_dirs_hash = devmode.hash_dirs(CACHE_SRC_DIRS)
    schema = None

    if devmode.is_in_dev_mode():
        schema = devmode.read_dev_mode_cache(
            std_dirs_hash, 'transient-stdschema.pickle')

    if schema is None:
        schema = s_schema.Schema()
        for modname in s_schema.STD_LIB:
            schema = load_std_module(schema, modname)

    if devmode.is_in_dev_mode():
        devmode.write_dev_mode_cache(
            schema, std_dirs_hash, 'transient-stdschema.pickle')

    return schema


def load_graphql_schema(
        schema: typing.Optional[s_schema.Schema]=None) -> s_schema.Schema:
    if schema is None:
        schema = s_schema.Schema()

    return load_std_module(schema, 'stdgraphql')
