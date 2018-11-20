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

from edb.lang.schema import links as s_links
from edb.lang.schema import lproperties as s_props
from edb.lang.schema import name as s_name
from edb.lang.schema import objtypes as s_objtypes
from edb.lang.schema import pointers as s_pointers

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


def edgedb_module_name_to_schema_name(module, prefix='edgedb_'):
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
    schema = edgedb_module_name_to_schema_name(name.module, prefix=prefix)
    if suffix:
        sname = f'{name.name}_{suffix}'
    else:
        sname = name.name

    dbname = edgedb_name_to_pg_name(sname)

    if catenate:
        return qname(schema, dbname)
    else:
        return schema, dbname


def scalar_name_to_domain_name(name, catenate=True, prefix='edgedb_'):
    return convert_name(name, 'domain', catenate)


def scalar_name_to_sequence_name(name, catenate=True, prefix='edgedb_'):
    return convert_name(name, 'sequence', catenate)


def objtype_name_to_table_name(name, catenate=True, prefix='edgedb_'):
    return convert_name(name, 'data', catenate)


def link_name_to_table_name(name, catenate=True):
    return convert_name(name, 'link', catenate)


def prop_name_to_table_name(name, catenate=True):
    return convert_name(name, 'prop', catenate)


def schema_name_to_pg_name(name: s_name.Name):
    return (
        edgedb_module_name_to_schema_name(name.module),
        edgedb_name_to_pg_name(name.name)
    )


def get_backend_operator_name(name, catenate=False):
    schema, oper_name = convert_name(name, catenate=False)
    oper_name = f'`{oper_name}`'
    if catenate:
        return qname(schema, oper_name)
    else:
        return schema, oper_name


def get_table_name(schema, obj, catenate=True):
    if isinstance(obj, s_objtypes.ObjectType):
        return objtype_name_to_table_name(obj.get_name(schema), catenate)
    elif isinstance(obj, s_props.Property):
        return prop_name_to_table_name(obj.get_name(schema), catenate)
    elif isinstance(obj, (s_links.Link, s_pointers.PointerLike)):
        return link_name_to_table_name(obj.get_name(schema), catenate)
    else:
        raise ValueError(f'cannot determine table for {obj!r}')
