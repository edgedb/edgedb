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


from __future__ import annotations

import pathlib
from typing import Tuple

from edb import lib as stdlib
from edb import errors
from edb.common import uuidgen

from edb import schema
from edb.schema import delta as sd
from edb.schema import version as s_ver

from edb.edgeql import compiler as qlcompiler
from edb.edgeql import parser as qlparser

from . import ddl as s_ddl
from . import name as sn
from . import schema as s_schema


SCHEMA_ROOT = pathlib.Path(schema.__path__[0])
LIB_ROOT = pathlib.Path(stdlib.__path__[0])
QL_COMPILER_ROOT = pathlib.Path(qlcompiler.__path__[0])
QL_PARSER_ROOT = pathlib.Path(qlparser.__path__[0])

CACHE_SRC_DIRS = (
    (SCHEMA_ROOT, '.py'),
    (QL_COMPILER_ROOT, '.py'),
    (QL_PARSER_ROOT, '.py'),
    (LIB_ROOT, '.edgeql'),
)


def get_std_module_text(modname: sn.Name) -> str:

    module_eql = ''

    module_path = LIB_ROOT / str(modname)
    module_files = []

    if module_path.is_dir():
        for entry in module_path.iterdir():
            if entry.is_file() and entry.suffix == '.edgeql':
                module_files.append(entry)
    else:
        module_path = module_path.with_suffix('.edgeql')
        if not module_path.exists():
            raise errors.SchemaError(f'std module not found: {modname}')
        module_files.append(module_path)

    module_files.sort(key=lambda p: p.name)

    for module_file in module_files:
        with open(module_file) as f:
            module_eql += '\n' + f.read()

    return module_eql


def load_std_module(
    schema: s_schema.Schema,
    modname: sn.Name,
) -> s_schema.Schema:

    return s_ddl.apply_ddl_script(
        get_std_module_text(modname),
        schema=schema,
        modaliases={},
        stdmode=True,
    )


BASE_VERSION = uuidgen.UUID('013d1e23-51ce-11ee-a29d-e1f01853d332')
GLOBAL_BASE_VERSION = uuidgen.UUID('013d235b-51ce-11ee-be76-bf15d10edfe5')


def make_schema_version(
    schema: s_schema.Schema,
) -> Tuple[s_schema.Schema, s_ver.CreateSchemaVersion]:
    context = sd.CommandContext(stdmode=True)
    sv = sn.UnqualName('__schema_version__')
    schema_version = s_ver.CreateSchemaVersion(classname=sv)
    schema_version.set_attribute_value('name', sv)
    schema_version.set_attribute_value('version', BASE_VERSION)
    schema_version.set_attribute_value('internal', True)
    schema = sd.apply(schema_version, schema=schema, context=context)
    return schema, schema_version


def make_global_schema_version(
    schema: s_schema.Schema,
) -> Tuple[s_schema.Schema, s_ver.CreateGlobalSchemaVersion]:
    context = sd.CommandContext(stdmode=True)
    sv = sn.UnqualName('__global_schema_version__')
    schema_version = s_ver.CreateGlobalSchemaVersion(classname=sv)
    schema_version.set_attribute_value('name', sv)
    schema_version.set_attribute_value('version', GLOBAL_BASE_VERSION)
    schema_version.set_attribute_value('internal', True)
    schema = sd.apply(schema_version, schema=schema, context=context)
    return schema, schema_version
