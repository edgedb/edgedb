##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import hashlib
import base64

import postgresql

from semantix.caos import proto


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


def convert_name(name, suffix, catenate=True):
    schema = caos_module_name_to_schema_name(name.module)
    name = '%s_%s' % (name.name, suffix)

    if catenate:
        return qname(schema, name)
    else:
        return schema, name


def atom_name_to_domain_name(name, catenate=True):
    return convert_name(name, 'domain', catenate)


def domain_name_to_atom_name(name):
    name = name.split('.')[-1]
    if name.endswith('_domain'):
        name = name[:-7]
    return name


def concept_name_to_table_name(name, catenate=True):
    return convert_name(name, 'data', catenate)


def table_name_to_concept_name(name):
    if name.endswith('_data') or name.endswith('_link'):
        name = name[:-5]
    return name


def link_name_to_table_name(name, catenate=True):
    return convert_name(name, 'link', catenate)


def table_name_to_link_name(name):
    if name.endswith('_link'):
        name = name[:-5]
    return name


def get_table_name(obj, catenate=True):
    if isinstance(obj, proto.Concept):
        return concept_name_to_table_name(obj.name, catenate)
    elif isinstance(obj, proto.Link):
        return link_name_to_table_name(obj.name, catenate)
    else:
        assert False
