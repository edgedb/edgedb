##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import hashlib
import base64

import postgresql


def quote_ident(text):
    """
    Quotes the identifier
    """
    result = postgresql.string.quote_ident(text)
    if result[0] != '"':
        return '"' + result + '"'
    return result


def qname(*parts):
    return '.'.join([quote_ident(q) for q in parts])


def caos_module_name_to_schema_name(module):
    return 'caos_' + module


def schema_name_to_caos_module_name(schema_name):
    if schema_name.startswith('caos_'):
        return schema_name[5:]
    else:
        return schema_name


def caos_name_to_pg_colname(name):
    """
    Convert Caos name to a valid PostgresSQL column name

    PostgreSQL has a limit of 63 characters for column names.

    @param name: Caos name to convert
    @return: PostgreSQL column name
    """

    name = str(name)
    if len(name) > 63:
        hash = base64.b64encode(hashlib.md5(name.encode()).digest()).decode().rstrip('=')
        name = hash + ':' + name[-(63 - 1 - len(hash)):]
    return name


def atom_name_to_domain_name(name):
    return qname(caos_module_name_to_schema_name(name.module), name.name + '_domain')


def domain_name_to_atom_name(name):
    name = name.split('.')[-1]
    if name.endswith('_domain'):
        name = name[:-7]
    return name


def concept_name_to_table_name(name, catenate=True):
    schema = caos_module_name_to_schema_name(name.module)
    table_name = name.name + '_data'

    if catenate:
        return qname(schema, table_name)
    else:
        return schema, table_name


def table_name_to_concept_name(name):
    if name.endswith('_data') or name.endswith('_link'):
        name = name[:-5]
    return name


def link_name_to_table_name(name):
    return qname(caos_module_name_to_schema_name(name.module), name.name + '_link')


def table_name_to_link_name(name):
    if name.endswith('_link'):
        name = name[:-5]
    return name
