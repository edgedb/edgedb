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

import binascii
import hashlib
import base64
import re

from edb.common import uuidgen
from edb.schema import abc as s_abc
from edb.schema import casts as s_casts
from edb.schema import constraints as s_constr
from edb.schema import functions as s_func
from edb.schema import modules as s_mod
from edb.schema import name as s_name
from edb.schema import objtypes as s_objtypes
from edb.schema import operators as s_opers
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types

from . import keywords as pg_keywords


def quote_e_literal(string):
    def escape_sq(s):
        split = re.split(r"(\n|\\\\|\\')", s)

        if len(split) == 1:
            return s.replace(r"'", r"\'")

        return ''.join((r if i % 2 else r.replace(r"'", r"\'"))
                       for i, r in enumerate(split))

    return "E'" + escape_sq(string) + "'"


def quote_literal(string):
    return "'" + string.replace("'", "''") + "'"


def _quote_ident(string):
    return '"' + string.replace('"', '""') + '"'


def quote_ident(string, *, force=False):
    return _quote_ident(string) if needs_quoting(string) or force else string


def quote_bytea_literal(data: bytes) -> str:
    """Return valid SQL representation of a bytes value."""

    if data:
        b = binascii.b2a_hex(data).decode('ascii')
        return f"'\\x{b}'::bytea"
    else:
        return "''::bytea"


def needs_quoting(string):
    isalnum = (string and not string[0].isdecimal() and
               string.replace('_', 'a').isalnum())
    return (
        not isalnum or
        string.lower() in pg_keywords.by_type[pg_keywords.RESERVED_KEYWORD] or
        string.lower() != string
    )


def qname(*parts):
    return '.'.join([quote_ident(q) for q in parts])


def quote_type(type_):
    if isinstance(type_, tuple):
        first = qname(*type_[:-1]) + '.' if len(type_) > 1 else ''
        last = type_[-1]
    else:
        first = ''
        last = type_

    is_rowtype = last.endswith('%ROWTYPE')
    if is_rowtype:
        last = last[:-8]

    is_array = last.endswith('[]')
    if is_array:
        last = last[:-2]

    last = quote_ident(last)

    if is_rowtype:
        last += '%ROWTYPE'

    if is_array:
        last += '[]'

    return first + last


def get_module_backend_name(module, prefix='edgedb_'):
    return edgedb_name_to_pg_name(f'{prefix}{module}', len(prefix))


def edgedb_name_to_pg_name(name: str, prefix_length: int = 0) -> str:
    """Convert EdgeDB name to a valid PostgresSQL column name.

    PostgreSQL has a limit of 63 characters for column names.

    @param name: EdgeDB name to convert
    @return: PostgreSQL column name
    """
    if not (0 <= prefix_length < 63):
        raise ValueError('supplied name is too long '
                         'to be kept in original form')

    name = str(name)
    if len(name) > 63 - prefix_length:
        hash = base64.b64encode(hashlib.md5(name.encode()).digest()).decode(
        ).rstrip('=')
        name = name[:prefix_length] + hash + ':' + name[-(
            63 - prefix_length - 1 - len(hash)):]
    return name


def convert_name(name, suffix='', catenate=True, prefix='edgedb_'):
    schema = get_module_backend_name(name.module, prefix=prefix)
    if suffix:
        sname = f'{name.name}_{suffix}'
    else:
        sname = name.name

    dbname = edgedb_name_to_pg_name(sname)

    if catenate:
        return qname(schema, dbname)
    else:
        return schema, dbname


def get_scalar_backend_name(id, module_id, catenate=True, *, aspect=None):
    if aspect is None:
        aspect = 'domain'
    if aspect not in ('domain', 'sequence', 'enum'):
        raise ValueError(
            f'unexpected aspect for scalar backend name: {aspect!r}')
    name = s_name.Name(module=str(module_id), name=str(id))
    return convert_name(name, aspect, catenate)


def get_objtype_backend_name(id, module_id, *, catenate=True, aspect=None):
    if aspect is None:
        aspect = 'table'
    if aspect != 'table' and not re.match(
            r'(source|target)-del-(def|imm)-(inl|otl)-(f|t)', aspect):
        raise ValueError(
            f'unexpected aspect for object type backend name: {aspect!r}')

    name = s_name.Name(module=str(module_id), name=str(id))

    if aspect != 'table':
        suffix = aspect
    else:
        suffix = ''

    return convert_name(name, suffix=suffix, catenate=catenate)


def get_pointer_backend_name(id, module_id, *, catenate=False):
    name = s_name.Name(module=str(module_id), name=str(id))
    return convert_name(name, suffix='', catenate=catenate)


_operator_map = {
    'std::AND': 'AND',
    'std::NOT': 'NOT',
    'std::?=': 'IS NOT DISTINCT FROM',
    'std::?!=': 'IS DISTINCT FROM',
    'std::LIKE': 'LIKE',
    'std::ILIKE': 'ILIKE',
    'std::NOT LIKE': 'NOT LIKE',
    'std::NOT ILIKE': 'NOT ILIKE',
}


def get_operator_backend_name(name, module_id, catenate=False, *, aspect=None):
    if aspect is None:
        aspect = 'operator'

    if aspect == 'function':
        fullname = s_name.Name(module=str(module_id), name=name.name)
        return convert_name(fullname, 'f', catenate=catenate)
    elif aspect != 'operator':
        raise ValueError(
            f'unexpected aspect for operator backend name: {aspect!r}')

    oper_name = _operator_map.get(name)
    if oper_name is None:
        oper_name = name.name
        if re.search(r'[a-zA-Z]', oper_name):
            # Alphanumeric operator, cannot be expressed in Postgres as-is
            # Since this is a rare occasion, we hard-code the translation
            # table.
            if oper_name == 'OR':
                oper_name = '|||'
            else:
                raise ValueError(
                    f'cannot represent operator {oper_name} in Postgres')

        oper_name = f'`{oper_name}`'
        schema = 'edgedb'
    else:
        schema = ''

    if catenate:
        return qname(schema, oper_name)
    else:
        return schema, oper_name


def get_cast_backend_name(name, module_id, catenate=False, *, aspect=None):
    if aspect == 'function':
        fullname = s_name.Name(module=str(module_id), name=name.name)
        return convert_name(fullname, 'f', catenate=catenate)
    else:
        raise ValueError(
            f'unexpected aspect for cast backend name: {aspect!r}')


def get_function_backend_name(name, module_id, catenate=False):
    fullname = s_name.Name(module=str(module_id), name=name.name)
    schema, func_name = convert_name(fullname, catenate=False)
    if catenate:
        return qname(schema, func_name)
    else:
        return schema, func_name


def get_constraint_backend_name(id, module_id, catenate=True, *, aspect=None):
    if aspect not in ('trigproc',):
        raise ValueError(
            f'unexpected aspect for constraint backend name: {aspect!r}')

    name = s_name.Name(module=str(module_id), name=str(id))
    return convert_name(name, aspect, catenate)


def get_index_backend_name(id, module_id, catenate=True, *, aspect=None):
    if aspect is None:
        aspect = 'index'
    name = s_name.Name(module=str(module_id), name=str(id))
    return convert_name(name, aspect, catenate)


def get_tuple_backend_name(id, catenate=True, *, aspect=None):

    name = s_name.Name(module='edgedb', name=f'{id}_t')
    return convert_name(name, aspect, catenate, prefix='')


def get_backend_name(schema, obj, catenate=True, *, aspect=None):
    if isinstance(obj, s_objtypes.ObjectType):
        name = obj.get_name(schema)
        module = schema.get_global(s_mod.Module, name.module)
        return get_objtype_backend_name(
            obj.id, module.id, catenate=catenate, aspect=aspect)

    elif isinstance(obj, s_abc.Pointer):
        name = obj.get_name(schema)
        module = schema.get_global(s_mod.Module, name.module)
        return get_pointer_backend_name(obj.id, module.id, catenate=catenate)

    elif isinstance(obj, s_scalars.ScalarType):
        name = obj.get_name(schema)
        module = schema.get_global(s_mod.Module, name.module)
        return get_scalar_backend_name(obj.id, module.id, catenate=catenate,
                                       aspect=aspect)

    elif isinstance(obj, s_opers.Operator):
        name = obj.get_shortname(schema)
        module = schema.get_global(s_mod.Module, name.module)
        return get_operator_backend_name(
            name, module.id, catenate, aspect=aspect)

    elif isinstance(obj, s_casts.Cast):
        name = obj.get_name(schema)
        module = schema.get_global(s_mod.Module, name.module)
        return get_cast_backend_name(
            name, module.id, catenate, aspect=aspect)

    elif isinstance(obj, s_func.Function):
        name = obj.get_shortname(schema)
        module = schema.get_global(s_mod.Module, name.module)
        return get_function_backend_name(
            name, module.id, catenate)

    elif isinstance(obj, s_mod.Module):
        return get_module_backend_name(str(obj.id))

    elif isinstance(obj, s_constr.Constraint):
        name = obj.get_name(schema)
        module = schema.get_global(s_mod.Module, name.module)
        return get_constraint_backend_name(
            obj.id, module.id, catenate, aspect=aspect)

    elif isinstance(obj, s_types.BaseTuple):
        return get_tuple_backend_name(
            obj.id, catenate, aspect=aspect)

    else:
        raise ValueError(f'cannot determine backend name for {obj!r}')


def get_object_from_backend_name(schema, metaclass, name, *, aspect=None):

    if metaclass is s_objtypes.ObjectType:
        table_name = name[1]
        obj_id = uuidgen.UUID(table_name)
        return schema.get_by_id(obj_id)

    else:
        raise ValueError(
            f'cannot determine object from backend name for {metaclass!r}')
