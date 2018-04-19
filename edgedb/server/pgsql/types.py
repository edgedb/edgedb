##
# Copyright (c) 2010-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import functools
import typing

from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import scalars as s_scalars
from edgedb.lang.schema import objtypes as s_objtypes
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import schema as s_schema
from edgedb.lang.schema import types as s_types

from . import common

base_type_name_map = {
    sn.Name('std::str'): 'text',
    sn.Name('std::int'): 'bigint',
    sn.Name('std::sequence'): 'text',
    sn.Name('std::decimal'): 'numeric',
    sn.Name('std::bool'): 'boolean',
    sn.Name('std::float'): 'float8',
    sn.Name('std::uuid'): 'uuid',
    sn.Name('std::datetime'): 'timestamptz',
    sn.Name('std::date'): 'date',
    sn.Name('std::time'): 'timetz',
    sn.Name('std::timedelta'): 'interval',
    sn.Name('std::bytes'): 'bytea',
    sn.Name('std::tsquery'): 'tsquery',
    sn.Name('std::tsvector'): 'tsvector',
    sn.Name('std::json'): 'jsonb',
}

base_type_name_map_r = {
    'character varying': sn.Name('std::str'),
    'character': sn.Name('std::str'),
    'text': sn.Name('std::str'),
    'numeric': sn.Name('std::decimal'),
    'integer': sn.Name('std::int'),
    'bigint': sn.Name('std::int'),
    'int8': sn.Name('std::int'),
    'smallint': sn.Name('std::int'),
    'boolean': sn.Name('std::bool'),
    'bool': sn.Name('std::bool'),
    'double precision': sn.Name('std::float'),
    'float8': sn.Name('std::float'),
    'uuid': sn.Name('std::uuid'),
    'timestamp with time zone': sn.Name('std::datetime'),
    'timestamptz': sn.Name('std::datetime'),
    'date': sn.Name('std::date'),
    'timetz': sn.Name('std::time'),
    'time': sn.Name('std::time'),
    'interval': sn.Name('std::timedelta'),
    'bytea': sn.Name('std::bytes'),
    'tsquery': sn.Name('std::tsquery'),
    'tsvector': sn.Name('std::tsvector'),
    'json': sn.Name('std::json'),
}


def get_scalar_base(schema, scalar):
    if scalar.bases:
        # Base is another ScalarType, check if it is fundamental, if not,
        # then it is another domain.
        #
        try:
            base = base_type_name_map[scalar.bases[0].name]
        except KeyError:
            base = common.scalar_name_to_domain_name(scalar.bases[0].name)
    else:
        # Base is a Python type, must correspond to PostgreSQL type
        try:
            base = base_type_name_map[scalar.name]
        except KeyError:
            base = 'text'

    return base


def pg_type_from_scalar(
        schema: s_schema.Schema,
        scalar: s_scalars.ScalarType,
        topbase: bool=False) -> typing.Tuple[str, ...]:

    if topbase:
        base = scalar.get_topmost_base()
    else:
        base = get_scalar_base(schema, scalar)

    if topbase:
        column_type = base_type_name_map.get(base.name)
        if not column_type:
            base_class = base.bases[0]
            column_type = (base_type_name_map[base_class.adapts],)
        else:
            column_type = (column_type,)
    else:
        column_type = base_type_name_map.get(scalar.name)
        if column_type:
            column_type = (base,)
        else:
            column_type = common.scalar_name_to_domain_name(
                scalar.name, catenate=False)

    return column_type


def pg_type_from_object(
        schema: s_schema.Schema,
        obj: s_obj.Object,
        topbase: bool=False) -> typing.Tuple[str, ...]:

    if isinstance(obj, s_scalars.ScalarType):
        return pg_type_from_scalar(schema, obj, topbase=topbase)

    elif isinstance(obj, s_types.Tuple):
        return ('record',)

    elif isinstance(obj, s_types.Map):
        return ('jsonb',)

    elif isinstance(obj, s_types.Array):
        if obj.element_type.name == 'std::any':
            return ('anyarray',)
        else:
            st = schema.get(obj.element_type.name)
            tp = pg_type_from_scalar(schema, st, topbase=True)
            if len(tp) == 1:
                return (tp[0] + '[]',)
            else:
                return (tp[0], tp[1] + '[]')

    elif isinstance(obj, s_objtypes.ObjectType):
        return ('uuid',)

    else:
        raise ValueError(f'could not determine PG type for {obj!r}')


class PointerStorageInfo:
    @classmethod
    def _source_table_info(cls, pointer):
        table = common.get_table_name(pointer.source, catenate=False)
        ptr_name = pointer.shortname
        col_name = common.edgedb_name_to_pg_name(ptr_name)
        table_type = 'ObjectType'

        return table, table_type, col_name

    @classmethod
    def _pointer_table_info(cls, pointer):
        table = common.get_table_name(pointer, catenate=False)
        col_name = 'std::target'

        if pointer.scalar():
            col_name += '@inline'

        table_type = 'link'
        col_name = common.edgedb_name_to_pg_name(col_name)

        return table, table_type, col_name

    @classmethod
    def _resolve_type(cls, pointer, schema):
        if pointer.target is not None:
            if isinstance(pointer.target, s_objtypes.ObjectType):
                column_type = ('uuid',)
            else:
                column_type = pg_type_from_object(schema, pointer.target)
        else:
            # The target may not be known in circular object-to-object
            # linking scenarios.
            column_type = ('uuid',)

        return column_type

    @classmethod
    def _storable_in_source(cls, pointer):
        return (
            pointer.singular() and pointer.scalar() or
            pointer.shortname in {
                'std::__type__',
                'schema::element_type',
                'schema::element_types',
                'schema::key_type',
                'schema::type'
            }
        )

    @classmethod
    def _storable_in_pointer(cls, pointer):
        return (
            not pointer.singular() or not pointer.scalar() or
            pointer.has_user_defined_properties())

    def __new__(
            cls, schema, pointer, source=None, resolve_type=True,
            link_bias=False):
        is_prop = isinstance(pointer, s_lprops.LinkProperty)

        if resolve_type and schema is None:
            msg = 'PointerStorageInfo needs a schema to resolve column_type'
            raise ValueError(msg)

        if source is None:
            source = pointer.source

        if is_prop and pointer.shortname == 'std::target':
            # Normalize link@target to link
            pointer = source
            is_prop = False

        if isinstance(pointer, irutils.TupleIndirectionLink):
            table = None
            table_type = 'ObjectType'
            col_name = common.edgedb_name_to_pg_name(pointer.shortname.name)
        elif is_prop:
            table = common.get_table_name(source, catenate=False)
            table_type = 'link'
            col_name = common.edgedb_name_to_pg_name(pointer.shortname)
        else:
            if isinstance(source, s_scalars.ScalarType):
                # This is a pseudo-link on an scalar (__type__)
                table = None
                table_type = 'ObjectType'
                col_name = None
            elif cls._storable_in_source(pointer) and not link_bias:
                table, table_type, col_name = cls._source_table_info(pointer)
            elif cls._storable_in_pointer(pointer):
                table, table_type, col_name = cls._pointer_table_info(pointer)
            else:
                return None

        if resolve_type:
            column_type = cls._resolve_type(pointer, schema)
        else:
            column_type = None

        result = super().__new__(cls)

        result.table_name = table
        result.table_type = table_type
        result.column_name = col_name
        result.column_type = column_type

        return result

    def __init__(self, *args, **kwargs):
        pass

    def __repr__(self):
        return \
            '<{} (table_name={}, table_type={}, column_name={}, ' \
            'column_type={}) at 0x{:x}>'.format(
                self.__class__.__name__, '.'.join(self.table_name),
                self.table_type, self.column_name, self.column_type, id(self))


@functools.lru_cache()
def get_pointer_storage_info(
        pointer, *, schema=None, source=None, resolve_type=True,
        link_bias=False):
    assert not pointer.generic(), "only specialized pointers can be stored"
    return PointerStorageInfo(
        schema, pointer, source=source, resolve_type=resolve_type,
        link_bias=link_bias)
