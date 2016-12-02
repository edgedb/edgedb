##
# Copyright (c) 2008-2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import hashlib
import base64

from edgedb.lang.common.algos import persistent_hash

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import links as s_links

from edgedb.server.pgsql.parser import keywords as pg_keywords


def quote_literal(string):
    return "'" + string.replace("'", "''") + "'"


def quote_ident_if_needed(string):
    return quote_ident(string) if needs_quoting(string) else string


def needs_quoting(string):
    isalnum = (string and not string[0].isdecimal() and
               string.replace('_', 'a').isalnum())
    return (
        not isalnum or
        string.lower() in pg_keywords.by_type[pg_keywords.RESERVED_KEYWORD]
    )


def quote_ident(string):
    return '"' + string.replace('"', '""') + '"'


def qname(*parts):
    return '.'.join([quote_ident(q) for q in parts])


def quote_type(type_):
    if isinstance(type_, tuple):
        first = qname(*type_[:-1]) + '.' if len(type_) > 1 else ''
        last = type_[-1]
    else:
        first = ''
        last = type_

    is_array = last.endswith('[]')
    if is_array:
        last = last[:-2]

    last = quote_ident_if_needed(last)
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


def convert_name(name, suffix, catenate=True, prefix='edgedb_'):
    schema = edgedb_module_name_to_schema_name(name.module, prefix=prefix)
    name = edgedb_name_to_pg_name('%s_%s' % (name.name, suffix))

    if catenate:
        return qname(schema, name)
    else:
        return schema, name


def atom_name_to_domain_name(name, catenate=True, prefix='edgedb_'):
    return convert_name(name, 'domain', catenate)


def atom_name_to_sequence_name(name, catenate=True, prefix='edgedb_'):
    return convert_name(name, 'sequence', catenate)


def concept_name_to_table_name(name, catenate=True, prefix='edgedb_'):
    return convert_name(name, 'data', catenate)


def concept_name_to_record_name(name, catenate=False, prefix='edgedb_'):
    return convert_name(name, 'concept_record', catenate)


def link_name_to_table_name(name, catenate=True):
    return convert_name(name, 'link', catenate)


def get_table_name(obj, catenate=True):
    if isinstance(obj, s_concepts.Concept):
        return concept_name_to_table_name(obj.name, catenate)
    elif isinstance(obj, s_links.Link):
        return link_name_to_table_name(obj.name, catenate)
    else:
        assert False


class RecordInfo:
    def __init__(
            self, *, attribute_map, metaclass=None, classname=None,
            recursive_link=False, virtuals_map=None):
        self.attribute_map = attribute_map
        self.virtuals_map = virtuals_map
        self.metaclass = metaclass
        self.classname = classname
        self.recursive_link = recursive_link
        self.id = str(persistent_hash.persistent_hash(self))

    def persistent_hash(self):
        return persistent_hash.persistent_hash((
            tuple(self.attribute_map), frozenset(self.virtuals_map.items())
            if self.virtuals_map else None, self.metaclass, self.classname,
            self.recursive_link))

    def __mm_serialize__(self):
        return dict(
            attribute_map=self.attribute_map, virtuals_map=self.virtuals_map,
            metaclass=self.metaclass, classname=self.classname,
            recursive_link=self.recursive_link, id=self.id)


FREEFORM_RECORD_ID = '6e51108d-7440-47f7-8c65-dc4d43fd90d2'
