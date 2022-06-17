# mypy: ignore-errors

#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2010-present MagicStack Inc. and the EdgeDB authors.
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

import functools
from typing import *

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from edb.schema import abc as s_abc
from edb.schema import scalars as s_scalars
from edb.schema import objtypes as s_objtypes
from edb.schema import name as sn
from edb.schema import objects as s_obj
from edb.schema import schema as s_schema

from . import common


base_type_name_map = {
    s_obj.get_known_type_id('std::str'): ('text',),
    s_obj.get_known_type_id('std::int64'): ('int8',),
    s_obj.get_known_type_id('std::int32'): ('int4',),
    s_obj.get_known_type_id('std::int16'): ('int2',),
    s_obj.get_known_type_id('std::decimal'): ('numeric',),
    s_obj.get_known_type_id('std::bigint'): ('edgedb', 'bigint_t'),
    s_obj.get_known_type_id('std::bool'): ('bool',),
    s_obj.get_known_type_id('std::float64'): ('float8',),
    s_obj.get_known_type_id('std::float32'): ('float4',),
    s_obj.get_known_type_id('std::uuid'): ('uuid',),
    s_obj.get_known_type_id('std::datetime'): ('edgedb', 'timestamptz_t'),
    s_obj.get_known_type_id('std::duration'): ('edgedb', 'duration_t',),
    s_obj.get_known_type_id('std::bytes'): ('bytea',),
    s_obj.get_known_type_id('std::json'): ('jsonb',),

    s_obj.get_known_type_id('cal::local_datetime'): ('edgedb', 'timestamp_t'),
    s_obj.get_known_type_id('cal::local_date'): ('edgedb', 'date_t'),
    s_obj.get_known_type_id('cal::local_time'): ('time',),
    s_obj.get_known_type_id('cal::relative_duration'):
        ('edgedb', 'relative_duration_t'),
    s_obj.get_known_type_id('cal::date_duration'):
        ('edgedb', 'date_duration_t'),

    s_obj.get_known_type_id('cfg::memory'): ('edgedb', 'memory_t'),
}

base_type_name_map_r = {
    'character varying': sn.QualName('std', 'str'),
    'character': sn.QualName('std', 'str'),
    'text': sn.QualName('std', 'str'),
    'numeric': sn.QualName('std', 'decimal'),
    'edgedb.bigint_t': sn.QualName('std', 'bigint'),
    'bigint_t': sn.QualName('std', 'bigint'),
    'int4': sn.QualName('std', 'int32'),
    'integer': sn.QualName('std', 'int32'),
    'bigint': sn.QualName('std', 'int64'),
    'int8': sn.QualName('std', 'int64'),
    'int2': sn.QualName('std', 'int16'),
    'smallint': sn.QualName('std', 'int16'),
    'boolean': sn.QualName('std', 'bool'),
    'bool': sn.QualName('std', 'bool'),
    'double precision': sn.QualName('std', 'float64'),
    'float8': sn.QualName('std', 'float64'),
    'real': sn.QualName('std', 'float32'),
    'float4': sn.QualName('std', 'float32'),
    'uuid': sn.QualName('std', 'uuid'),
    'timestamp with time zone': sn.QualName('std', 'datetime'),
    'edgedb.timestamptz_t': sn.QualName('std', 'datetime'),
    'timestamptz_t': sn.QualName('std', 'datetime'),
    'timestamptz': sn.QualName('std', 'datetime'),
    'duration_t': sn.QualName('std', 'duration'),
    'edgedb.duration_t': sn.QualName('std', 'duration'),
    'interval': sn.QualName('std', 'duration'),
    'bytea': sn.QualName('std', 'bytes'),
    'jsonb': sn.QualName('std', 'json'),

    'timestamp': sn.QualName('cal', 'local_datetime'),
    'timestamp_t': sn.QualName('cal', 'local_datetime'),
    'edgedb.timestamp_t': sn.QualName('cal', 'local_datetime'),
    'date': sn.QualName('cal', 'local_date'),
    'date_t': sn.QualName('cal', 'local_date'),
    'edgedb.date_t': sn.QualName('cal', 'local_date'),
    'time': sn.QualName('cal', 'local_time'),
    'relative_duration_t': sn.QualName('cal', 'relative_duration'),
    'edgedb.relative_duration_t': sn.QualName('cal', 'relative_duration'),
    'date_duration_t': sn.QualName('cal', 'date_duration'),
    'edgedb.date_duration_t': sn.QualName('cal', 'date_duration'),

    'edgedb.memory_t': sn.QualName('cfg', 'memory'),
    'memory_t': sn.QualName('cfg', 'memory'),
}


def is_builtin_scalar(schema, scalar):
    return scalar.id in base_type_name_map


def type_has_stable_oid(typ):
    pg_type = base_type_name_map.get(typ.id)
    return pg_type is not None and len(pg_type) == 1


def get_scalar_base(schema, scalar) -> Tuple[str, ...]:
    base = base_type_name_map.get(scalar.id)
    if base is not None:
        return base

    for ancestor in scalar.get_ancestors(schema).objects(schema):
        if not ancestor.get_abstract(schema):
            # Check if base is fundamental, if not, then it is
            # another domain.
            try:
                base = base_type_name_map[ancestor.id]
            except KeyError:
                base = common.get_backend_name(
                    schema, ancestor, catenate=False)

            return base

    raise ValueError(f'cannot determine backend type for scalar type '
                     f'{scalar.get_name(schema)}')


def pg_type_from_scalar(
        schema: s_schema.Schema,
        scalar: s_scalars.ScalarType) -> Tuple[str, ...]:

    if scalar.is_polymorphic(schema):
        return ('anynonarray',)

    is_enum = scalar.is_enum(schema)

    if is_enum:
        base = scalar
    else:
        base = get_scalar_base(schema, scalar)

    column_type = base_type_name_map.get(scalar.id)
    if column_type:
        column_type = base
    else:
        column_type = common.get_backend_name(schema, scalar, catenate=False)

    return column_type


def pg_type_array(tp: Tuple[str, ...]) -> Tuple[str, ...]:
    if len(tp) == 1:
        return (tp[0] + '[]',)
    else:
        return (tp[0], tp[1] + '[]')


def pg_type_from_object(
        schema: s_schema.Schema,
        obj: s_obj.Object,
        persistent_tuples: bool=False) -> Tuple[str, ...]:

    if isinstance(obj, s_scalars.ScalarType):
        return pg_type_from_scalar(schema, obj)

    elif obj.is_type() and obj.is_anytuple(schema):
        return ('record',)

    elif isinstance(obj, s_abc.Tuple):
        if persistent_tuples:
            return common.get_tuple_backend_name(obj.id, catenate=False)
        else:
            return ('record',)

    elif isinstance(obj, s_abc.Array):
        if obj.is_polymorphic(schema):
            return ('anyarray',)
        else:
            tp = pg_type_from_object(
                schema, obj.get_subtypes(schema)[0],
                persistent_tuples=persistent_tuples)
            return pg_type_array(tp)

    elif isinstance(obj, s_objtypes.ObjectType):
        return ('uuid',)

    elif obj.is_type() and obj.is_any(schema):
        return ('anyelement',)

    else:
        raise ValueError(f'could not determine PG type for {obj!r}')


def pg_type_from_ir_typeref(
        ir_typeref: irast.TypeRef, *,
        serialized: bool = False,
        persistent_tuples: bool = False) -> Tuple[str, ...]:

    if irtyputils.is_array(ir_typeref):
        if (irtyputils.is_generic(ir_typeref)
                or (irtyputils.is_abstract(ir_typeref.subtypes[0])
                    and irtyputils.is_scalar(ir_typeref.subtypes[0]))):
            return ('anyarray',)
        else:
            tp = pg_type_from_ir_typeref(
                ir_typeref.subtypes[0],
                serialized=serialized,
                persistent_tuples=persistent_tuples)
            if len(tp) == 1:
                return (tp[0] + '[]',)
            else:
                return (tp[0], tp[1] + '[]')

    elif irtyputils.is_anytuple(ir_typeref):
        return ('record',)

    elif irtyputils.is_tuple(ir_typeref):
        if ir_typeref.material_type:
            material = ir_typeref.material_type
        else:
            material = ir_typeref

        if persistent_tuples or material.in_schema:
            return common.get_tuple_backend_name(material.id, catenate=False)
        else:
            return ('record',)

    elif irtyputils.is_any(ir_typeref):
        return ('anyelement',)

    else:
        if ir_typeref.material_type:
            material = ir_typeref.material_type
        else:
            material = ir_typeref

        if irtyputils.is_object(material):
            if serialized:
                return ('record',)
            else:
                return ('uuid',)
        elif irtyputils.is_abstract(material):
            return ('anynonarray',)
        else:
            pg_type = base_type_name_map.get(material.id)
            if pg_type is None:
                # User-defined scalar type
                pg_type = common.get_scalar_backend_name(
                    material.id, material.name_hint.module, catenate=False)

            return pg_type


class _PointerStorageInfo:
    @classmethod
    def _source_table_info(cls, schema, pointer):
        table = common.get_backend_name(
            schema, pointer.get_source(schema), catenate=False)
        ptr_name = pointer.get_shortname(schema).name
        if ptr_name.startswith('__') or ptr_name == 'id':
            col_name = ptr_name
        else:
            col_name = str(pointer.id)
        table_type = 'ObjectType'

        return table, table_type, col_name

    @classmethod
    def _pointer_table_info(cls, schema, pointer):
        table = common.get_backend_name(
            schema, pointer, catenate=False)
        col_name = 'target'
        table_type = 'link'

        return table, table_type, col_name

    @classmethod
    def _resolve_type(cls, schema, pointer):
        pointer_target = pointer.get_target(schema)
        if pointer_target is not None:
            if pointer_target.is_object_type():
                column_type = ('uuid',)
            elif pointer_target.is_tuple(schema):
                column_type = common.get_backend_name(schema, pointer_target,
                                                      catenate=False)
            else:
                column_type = pg_type_from_object(
                    schema, pointer.get_target(schema),
                    persistent_tuples=True)
        else:
            # The target may not be known in circular object-to-object
            # linking scenarios.
            column_type = ('uuid',)

        return column_type

    @classmethod
    def _storable_in_source(cls, schema, pointer):
        return pointer.singular(schema)

    @classmethod
    def _storable_in_pointer(cls, schema, pointer):
        return (
            not pointer.singular(schema) or
            pointer.has_user_defined_properties(schema))

    def __new__(cls, schema, pointer, source=None, resolve_type=True,
                link_bias=False):

        if source is None:
            source = pointer.get_source(schema)

        is_lprop = pointer.is_link_property(schema)

        if resolve_type and schema is None:
            msg = 'PointerStorageInfo needs a schema to resolve column_type'
            raise ValueError(msg)

        if is_lprop and pointer.issubclass(schema, schema.get('std::target')):
            # Normalize link@target to link
            pointer = source
            is_lprop = False

        if isinstance(pointer, irast.TupleIndirectionLink):
            table = None
            table_type = 'ObjectType'
            col_name = pointer.get_shortname(schema).name
        elif is_lprop:
            table = common.get_backend_name(
                schema, source, catenate=False)
            table_type = 'link'
            if pointer.get_shortname(schema).name == 'source':
                col_name = 'source'
            else:
                col_name = str(pointer.id)
        else:
            if isinstance(source, s_scalars.ScalarType):
                # This is a pseudo-link on an scalar (__type__)
                table = None
                table_type = 'ObjectType'
                col_name = None
            elif cls._storable_in_source(schema, pointer) and not link_bias:
                table, table_type, col_name = cls._source_table_info(
                    schema, pointer)
            elif cls._storable_in_pointer(schema, pointer):
                table, table_type, col_name = cls._pointer_table_info(
                    schema, pointer)
            else:
                return None

        if resolve_type:
            column_type = cls._resolve_type(schema, pointer)
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
        pointer, *, schema, source=None, resolve_type=True,
        link_bias=False):
    assert not pointer.generic(schema), \
        "only specialized pointers can be stored"
    if pointer.get_computable(schema):
        material_ptrcls = None
    else:
        schema, material_ptrcls = pointer.material_type(schema)
    if material_ptrcls is not None:
        pointer = material_ptrcls
    return _PointerStorageInfo(
        schema, pointer, source=source, resolve_type=resolve_type,
        link_bias=link_bias)


class PointerStorageInfo(NamedTuple):

    table_name: Optional[Tuple[str, str]]
    table_type: str
    column_name: str
    column_type: Tuple[str, str]


@overload
def get_ptrref_storage_info(
    ptrref: irast.BasePointerRef, *,
    resolve_type: bool=...,
    link_bias: Literal[False]=False,
    allow_missing: Literal[False]=False,
) -> PointerStorageInfo:
    ...


@overload
def get_ptrref_storage_info(  # NoQA: F811
    ptrref: irast.BasePointerRef, *,
    resolve_type: bool=...,
    link_bias: bool=...,
    allow_missing: bool=...,
) -> Optional[PointerStorageInfo]:
    ...


def get_ptrref_storage_info(  # NoQA: F811
        ptrref: irast.BasePointerRef, *,
        resolve_type=True, link_bias=False,
        allow_missing=False) -> Optional[PointerStorageInfo]:
    # We wrap the real version because of bad mypy interactions
    # with lru_cache.
    return _get_ptrref_storage_info(
        ptrref,
        resolve_type=resolve_type,
        link_bias=link_bias,
        allow_missing=allow_missing,
    )


@functools.lru_cache()
def _get_ptrref_storage_info(
        ptrref: irast.BasePointerRef, *,
        resolve_type=True, link_bias=False,
        allow_missing=False) -> Optional[PointerStorageInfo]:

    if ptrref.material_ptr:
        ptrref = ptrref.material_ptr

    if ptrref.out_cardinality is None:
        # Guard against the IR generator failure to populate the PointerRef
        # cardinality correctly.
        raise RuntimeError(
            f'cannot determine backend storage parameters for the '
            f'{ptrref.name!r} pointer: the cardinality is not known')

    is_lprop = ptrref.source_ptr is not None

    if is_lprop:
        source = ptrref.source_ptr
    else:
        source = ptrref.out_source

    target = ptrref.out_target

    if isinstance(ptrref, irast.TupleIndirectionPointerRef):
        table = None
        table_type = 'ObjectType'
        col_name = ptrref.shortname.name

    elif is_lprop:
        table = common.get_pointer_backend_name(
            source.id, source.name.module, catenate=False)
        table_type = 'link'
        if ptrref.shortname.name in ('source', 'target'):
            col_name = ptrref.shortname.name
        else:
            col_name = str(ptrref.id)
    else:
        if irtyputils.is_scalar(source):
            # This is a pseudo-link on an scalar (__type__)
            table = None
            table_type = 'ObjectType'
            col_name = None

        elif _storable_in_source(ptrref) and not link_bias:
            table = common.get_objtype_backend_name(
                source.id, source.name_hint.module, catenate=False)
            ptrname = ptrref.shortname.name
            if ptrname.startswith('__') or ptrname == 'id':
                col_name = ptrname
            else:
                col_name = str(ptrref.id)
            table_type = 'ObjectType'

        elif _storable_in_pointer(ptrref):
            table = common.get_pointer_backend_name(
                ptrref.id, ptrref.name.module, catenate=False)
            col_name = 'target'
            table_type = 'link'

        elif not link_bias and not allow_missing:
            raise RuntimeError(
                f'cannot determine backend storage parameters for the '
                f'{ptrref.name} pointer: unexpected characteristics')

        else:
            return None

    if resolve_type:
        if irtyputils.is_object(target):
            column_type = ('uuid',)
        else:
            column_type = pg_type_from_ir_typeref(
                target, persistent_tuples=True)
    else:
        column_type = None

    return PointerStorageInfo(
        table_name=table, table_type=table_type,
        column_name=col_name, column_type=column_type
    )


def _storable_in_source(ptrref: irast.PointerRef) -> bool:
    return ptrref.out_cardinality.is_single()


def _storable_in_pointer(ptrref: irast.PointerRef) -> bool:
    if ptrref.union_components:
        return all(_storable_in_pointer(c) for c in ptrref.union_components)
    else:
        return (
            ptrref.out_cardinality.is_multi()
            or ptrref.has_properties
        )
