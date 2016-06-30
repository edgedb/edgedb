##
# Copyright (c) 2008-2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib
import itertools
import hashlib
import base64

import postgresql

from edgedb.lang.common.algos import persistent_hash

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import objects as s_obj


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


def caos_module_name_to_schema_name(module, prefix='edgedb_'):
    return caos_name_to_pg_name(prefix + module, len(prefix))


def caos_name_to_pg_name(name, prefix_length=0):
    """
    Convert Caos name to a valid PostgresSQL column name

    PostgreSQL has a limit of 63 characters for column names.

    @param name: Caos name to convert
    @return: PostgreSQL column name
    """

    assert 0 <= prefix_length < 63, "supplied name is too long to be kept in original form"

    name = str(name)
    if len(name) > 63 - prefix_length:
        hash = base64.b64encode(hashlib.md5(name.encode()).digest()).decode().rstrip('=')
        name = name[:prefix_length] + hash + ':' + name[-(63 - prefix_length - 1 - len(hash)):]
    return name


def convert_name(name, suffix, catenate=True, prefix='edgedb_'):
    schema = caos_module_name_to_schema_name(name.module, prefix=prefix)
    name = caos_name_to_pg_name('%s_%s' % (name.name, suffix))

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

_type_index = None

def py_type_to_pg_type(typ):
    global _type_index

    if _type_index is None:
        import postgresql.types
        import postgresql.types.io

        _type_index = {}

        postgres_io_mods = {'postgresql.types.io.{}'.format(m) for m in
                            postgresql.types.io.io_modules}

        caos_io_mods = {'edgedb.server.pgsql.driver.io.{}'.format(m) for m in
                        custom_type_io.io_modules}

        for mod in itertools.chain(postgres_io_mods, caos_io_mods):
            try:
                mod = importlib.import_module(mod)
            except ImportError:
                continue

            oid_to_type = getattr(mod, 'oid_to_type', None)
            if oid_to_type:
                _type_index.update({k: postgresql.types.oid_to_name[v]
                                    for k, v in zip(oid_to_type.values(), oid_to_type.keys())})

        _type_index.update({k: postgresql.types.oid_to_name[v]
                            for k, v in zip(driver.oid_to_type.values(), driver.oid_to_type.keys())})

    if isinstance(typ, tuple):
        supertyp, typ = typ
        assert issubclass(supertyp, list)

        if isinstance(typ, s_obj.PrototypeClass):
            basetyp = 'int'
        else:
            basetyp = _type_index[typ]

        return '%s[]' % basetyp
    else:
        if isinstance(typ, s_obj.PrototypeClass):
            return 'int'
        else:
            return _type_index[typ]


class RecordInfo:
    def __init__(self, *, attribute_map, proto_class=None, proto_name=None, is_xvalue=False,
                          recursive_link=False, virtuals_map=None):
        self.attribute_map = attribute_map
        self.virtuals_map = virtuals_map
        self.proto_class = proto_class
        self.proto_name = proto_name
        self.is_xvalue = is_xvalue
        self.recursive_link = recursive_link
        self.id = str(persistent_hash.persistent_hash(self))

    def persistent_hash(self):
        return persistent_hash.persistent_hash((tuple(self.attribute_map),
                                                frozenset(self.virtuals_map.items())
                                                    if self.virtuals_map else None,
                                                self.proto_class,
                                                self.proto_name, self.is_xvalue,
                                                self.recursive_link))

    def __mm_serialize__(self):
        return dict(
            attribute_map=self.attribute_map,
            virtuals_map=self.virtuals_map,
            proto_class=self.proto_class,
            proto_name=self.proto_name,
            is_xvalue=self.is_xvalue,
            recursive_link=self.recursive_link,
            id=self.id
        )


FREEFORM_RECORD_ID = '6e51108d-7440-47f7-8c65-dc4d43fd90d2'
