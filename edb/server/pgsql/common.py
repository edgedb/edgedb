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


import hashlib
import base64
import re
import uuid

from edb.lang.schema import casts as s_casts
from edb.lang.schema import constraints as s_constr
from edb.lang.schema import functions as s_func
from edb.lang.schema import links as s_links
from edb.lang.schema import lproperties as s_props
from edb.lang.schema import modules as s_mod
from edb.lang.schema import name as s_name
from edb.lang.schema import objtypes as s_objtypes
from edb.lang.schema import operators as s_opers
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import scalars as s_scalars

from edb.server.pgsql.parser import keywords as pg_keywords


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


def _edgedb_module_name_to_schema_name(module, prefix='edgedb_'):
    return edgedb_name_to_pg_name(prefix + module, len(prefix))


def edgedb_name_to_pg_name(name, prefix_length=0):
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
    schema = _edgedb_module_name_to_schema_name(name.module, prefix=prefix)
    if suffix:
        sname = f'{name.name}_{suffix}'
    else:
        sname = name.name

    dbname = edgedb_name_to_pg_name(sname)

    if catenate:
        return qname(schema, dbname)
    else:
        return schema, dbname


def _scalar_name_to_domain_name(name, catenate=True, prefix='edgedb_', *,
                                aspect=None):
    if aspect is None:
        aspect = 'domain'
    if aspect not in ('domain', 'sequence'):
        raise ValueError(
            f'unexpected aspect for scalar backend name: {aspect!r}')
    return convert_name(name, aspect, catenate)


def _get_backend_objtype_name(schema, objtype, catenate=True, aspect=None):
    if aspect is None:
        aspect = 'table'
    if aspect not in (
            'table',
            'target-del-def-t', 'target-del-imm-t',
            'source-del-def-t', 'source-del-imm-t',
            'target-del-def-f', 'target-del-imm-f',
            'source-del-def-f', 'source-del-imm-f'):
        raise ValueError(
            f'unexpected aspect for object type backend name: {aspect!r}')

    name = s_name.Name(module=objtype.get_name(schema).module,
                       name=str(objtype.id))

    if aspect != 'table':
        suffix = aspect
    else:
        suffix = ''

    return convert_name(name, suffix=suffix, catenate=catenate)


def _link_name_to_table_name(name, catenate=True):
    return convert_name(name, 'link', catenate)


def _prop_name_to_table_name(name, catenate=True):
    return convert_name(name, 'prop', catenate)


def schema_name_to_pg_name(name: s_name.Name):
    return (
        _edgedb_module_name_to_schema_name(name.module),
        edgedb_name_to_pg_name(name.name)
    )


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


def get_backend_operator_name(name, catenate=False, *, aspect=None):
    if aspect is None:
        aspect = 'operator'

    if aspect == 'function':
        return convert_name(name, 'f', catenate=catenate)
    elif aspect != 'operator':
        raise ValueError(
            f'unexpected aspect for operator backend name: {aspect!r}')

    oper_name = _operator_map.get(name)
    if oper_name is not None:
        schema = ''
    else:
        schema, oper_name = convert_name(name, catenate=False)
        if re.search(r'[a-zA-Z]', oper_name):
            # Alphanumeric operator, cannot be expressed in Postgres as-is
            # Since this is a rare occasion, we hard-code the translation
            # table.
            if oper_name == 'OR':
                oper_name = '||'
            else:
                raise ValueError(
                    f'cannot represent operator {oper_name} in Postgres')

        oper_name = f'`{oper_name}`'

    if catenate:
        return qname(schema, oper_name)
    else:
        return schema, oper_name


def get_backend_cast_name(name, catenate=False, *, aspect=None):
    if aspect == 'function':
        return convert_name(name, 'f', catenate=catenate)
    else:
        raise ValueError(
            f'unexpected aspect for cast backend name: {aspect!r}')


def _get_backend_function_name(name, catenate=False):
    schema, func_name = convert_name(name, catenate=False)
    if catenate:
        return qname(schema, func_name)
    else:
        return schema, func_name


def _get_backend_constraint_name(
        schema, constraint, catenate=True, prefix='edgedb_', *, aspect=None):
    if aspect not in ('trigproc',):
        raise ValueError(
            f'unexpected aspect for constraint backend name: {aspect!r}')

    name = s_name.Name(module=constraint.get_name(schema).module,
                       name=str(constraint.id))

    return convert_name(name, aspect, catenate, prefix=prefix)


def get_backend_name(schema, obj, catenate=True, *, aspect=None):
    if isinstance(obj, s_objtypes.ObjectType):
        return _get_backend_objtype_name(schema, obj, catenate, aspect=aspect)

    elif isinstance(obj, s_props.Property):
        return _prop_name_to_table_name(obj.get_name(schema), catenate)

    elif isinstance(obj, (s_links.Link, s_pointers.PointerLike)):
        return _link_name_to_table_name(obj.get_name(schema), catenate)

    elif isinstance(obj, s_scalars.ScalarType):
        return _scalar_name_to_domain_name(
            obj.get_name(schema), catenate, aspect=aspect)

    elif isinstance(obj, s_opers.Operator):
        return get_backend_operator_name(
            obj.get_shortname(schema), catenate, aspect=aspect)

    elif isinstance(obj, s_casts.Cast):
        return get_backend_cast_name(
            obj.get_name(schema), catenate, aspect=aspect)

    elif isinstance(obj, s_func.Function):
        return _get_backend_function_name(obj.get_shortname(schema), catenate)

    elif isinstance(obj, s_mod.Module):
        return _edgedb_module_name_to_schema_name(obj.get_name(schema))

    elif isinstance(obj, s_constr.Constraint):
        return _get_backend_constraint_name(
            schema, obj, catenate, aspect=aspect)

    else:
        raise ValueError(f'cannot determine backend name for {obj!r}')


def get_object_from_backend_name(schema, metaclass, name, *, aspect=None):

    if metaclass is s_objtypes.ObjectType:
        table_name = name[1]
        obj_id = uuid.UUID(table_name)
        return schema.get_by_id(obj_id)

    else:
        raise ValueError(
            f'cannot determine object from backend name for {metaclass!r}')
