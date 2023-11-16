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
import dataclasses
import uuid
from typing import *
from typing import overload

from edb.common.typeutils import not_none

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from edb.schema import abc as s_abc
from edb.schema import scalars as s_scalars
from edb.schema import objtypes as s_objtypes
from edb.schema import name as sn
from edb.schema import objects as s_obj
from edb.schema import schema as s_schema
from edb.schema import types as s_types
from edb.schema import pointers as s_pointers

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

type_to_range_name_map = {
    ('int4',): ('int4range',),
    ('int8',): ('int8range',),
    ('numeric',): ('numrange',),
    ('float4',): ('edgedb', 'float32_range_t'),
    ('float8',): ('edgedb', 'float64_range_t'),
    ('edgedb', 'timestamptz_t'): ('edgedb', 'datetime_range_t'),
    ('edgedb', 'timestamp_t'): ('edgedb', 'local_datetime_range_t'),
    # cal::local_date uses the built-in daterange instead of a custom
    # one that actually uses edgedb.date_t as its subtype. This is
    # because cal::local_date is discrete, and its range type should
    # get canonicalized. Defining a canonicalization function for a
    # custom range is a big hassle, and daterange already has the
    # correct canonicalization function
    ('edgedb', 'date_t'): ('daterange',),
}

# Construct a multirange map based on type_to_range_name_map by replacing
# 'range' with 'multirange' in the names.
#
# The multiranges are created automatically when ranges are created. They
# have the same names except with "multi" in front of the "range".
type_to_multirange_name_map = {}
for key, val in type_to_range_name_map.items():
    *pre, name = val
    pre.append(name.replace('range', 'multirange'))
    type_to_multirange_name_map[key] = tuple(pre)


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

pg_tsvector_typeref = irast.TypeRef(
    id=uuid.UUID('44d73839-8882-419f-80e5-84f7a3402919'),
    name_hint=sn.QualName('pg_catalog', 'tsvector'),
    is_scalar=True,
    sql_type='pg_catalog.tsvector',
)

pg_oid_typeref = irast.TypeRef(
    id=uuid.UUID('44d73839-8882-419f-80e5-84f7a3402920'),
    name_hint=sn.QualName('pg_catalog', 'oid'),
    is_scalar=True,
    sql_type='pg_catalog.oid',
)

pg_langs = {
    'simple',
    'arabic',
    'armenian',
    'basque',
    'catalan',
    'danish',
    'dutch',
    'english',
    'finnish',
    'french',
    'german',
    'greek',
    'hindi',
    'hungarian',
    'indonesian',
    'irish',
    'italian',
    'lithuanian',
    'nepali',
    'norwegian',
    'portuguese',
    'romanian',
    'russian',
    'serbian',
    'spanish',
    'swedish',
    'tamil',
    'turkish',
    'yiddish',
}


pg_langs_by_iso_639_3 = {
    'ara': 'arabic',
    'hye': 'armenian',
    'eus': 'basque',
    'cat': 'catalan',
    'dan': 'danish',
    'nld': 'dutch',
    'eng': 'english',
    'fin': 'finnish',
    'fra': 'french',
    'deu': 'german',
    'ell': 'greek',
    'hin': 'hindi',
    'hun': 'hungarian',
    'ind': 'indonesian',
    'gle': 'irish',
    'ita': 'italian',
    'lit': 'lithuanian',
    'npi': 'nepali',
    'nor': 'norwegian',
    'por': 'portuguese',
    'ron': 'romanian',
    'rus': 'russian',
    'srp': 'serbian',
    'spa': 'spanish',
    'swe': 'swedish',
    'tam': 'tamil',
    'tur': 'turkish',
    'yid': 'yiddish',
}


def to_regconfig(language: str) -> str:
    "Analogous to edgedb.fts_to_regconfig function in metaschema"
    language = language.lower()
    if language.startswith('xxx_'):
        return language[4:]
    else:
        return pg_langs_by_iso_639_3.get(language, language)


def is_builtin_scalar(
    schema: s_schema.Schema, scalar: s_scalars.ScalarType
) -> bool:
    return scalar.id in base_type_name_map


def type_has_stable_oid(typ: s_types.Type) -> bool:
    pg_type = base_type_name_map.get(typ.id)
    return pg_type is not None and len(pg_type) == 1


def get_scalar_base(
    schema: s_schema.Schema, scalar: s_scalars.ScalarType
) -> Tuple[str, ...]:
    if base := base_type_name_map.get(scalar.id):
        return base

    for ancestor in scalar.get_ancestors(schema).objects(schema):
        if not ancestor.get_abstract(schema):
            # Check if base is fundamental, if not, then it is
            # another domain.
            if base := base_type_name_map.get(ancestor.id):
                pass
            elif typstr := ancestor.resolve_sql_type(schema):
                base = tuple(typstr.split('.'))
            else:
                base = common.get_backend_name(
                    schema, ancestor, catenate=False)
                assert base

            return base

    raise ValueError(f'cannot determine backend type for scalar type '
                     f'{scalar.get_name(schema)}')


def pg_type_from_scalar(
    schema: s_schema.Schema, scalar: s_scalars.ScalarType
) -> Tuple[str, ...]:

    if scalar.is_polymorphic(schema):
        return ('anynonarray',)

    column_type = base_type_name_map.get(scalar.id)
    if column_type:
        pass
    elif typstr := scalar.resolve_sql_type(schema):
        column_type = tuple(typstr.split('.'))
    else:
        column_type = common.get_backend_name(schema, scalar, catenate=False)
    assert column_type

    return column_type


def pg_type_array(tp: Tuple[str, ...]) -> Tuple[str, ...]:
    if len(tp) == 1:
        return (tp[0] + '[]',)
    else:
        return (tp[0], tp[1] + '[]')


def pg_type_range(tp: Tuple[str, ...]) -> Tuple[str, ...]:
    return type_to_range_name_map[tp]


def pg_type_multirange(tp: Tuple[str, ...]) -> Tuple[str, ...]:
    return type_to_multirange_name_map[tp]


def pg_type_from_object(
    schema: s_schema.Schema, obj: s_obj.Object, persistent_tuples: bool = False
) -> Tuple[str, ...]:

    if isinstance(obj, s_scalars.ScalarType):
        return pg_type_from_scalar(schema, obj)

    elif isinstance(obj, s_types.Type) and obj.is_anytuple(schema):
        return ('record',)

    elif isinstance(obj, s_abc.Tuple):
        if persistent_tuples:
            return cast(
                Tuple[str, ...],
                common.get_tuple_backend_name(obj.id, catenate=False),
            )
        else:
            return ('record',)

    elif isinstance(obj, s_types.Array):
        if obj.is_polymorphic(schema):
            return ('anyarray',)
        else:
            tp = pg_type_from_object(
                schema, obj.get_subtypes(schema)[0],
                persistent_tuples=persistent_tuples)
            return pg_type_array(tp)

    elif isinstance(obj, s_types.Range):
        if obj.is_polymorphic(schema):
            return ('anyrange',)
        else:
            tp = pg_type_from_object(
                schema, obj.get_subtypes(schema)[0],
                persistent_tuples=persistent_tuples)
            return pg_type_range(tp)

    elif isinstance(obj, s_types.MultiRange):
        if obj.is_polymorphic(schema):
            return ('anymultirange',)
        else:
            tp = pg_type_from_object(
                schema, obj.get_subtypes(schema)[0],
                persistent_tuples=persistent_tuples)
            return pg_type_multirange(tp)

    elif isinstance(obj, s_objtypes.ObjectType):
        return ('uuid',)

    elif isinstance(obj, s_types.Type) and obj.is_any(schema):
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
            return pg_type_array(tp)

    elif irtyputils.is_range(ir_typeref):
        if (irtyputils.is_generic(ir_typeref)
                or (irtyputils.is_abstract(ir_typeref.subtypes[0])
                    and irtyputils.is_scalar(ir_typeref.subtypes[0]))):
            return ('anyrange',)
        else:
            tp = pg_type_from_ir_typeref(
                ir_typeref.subtypes[0],
                serialized=serialized,
                persistent_tuples=persistent_tuples)
            return pg_type_range(tp)

    elif irtyputils.is_multirange(ir_typeref):
        if (irtyputils.is_generic(ir_typeref)
                or (irtyputils.is_abstract(ir_typeref.subtypes[0])
                    and irtyputils.is_scalar(ir_typeref.subtypes[0]))):
            return ('anymultirange',)
        else:
            tp = pg_type_from_ir_typeref(
                ir_typeref.subtypes[0],
                serialized=serialized,
                persistent_tuples=persistent_tuples)
            return pg_type_multirange(tp)

    elif irtyputils.is_anytuple(ir_typeref):
        return ('record',)

    elif irtyputils.is_tuple(ir_typeref):
        if ir_typeref.material_type:
            material = ir_typeref.material_type
        else:
            material = ir_typeref

        if persistent_tuples or material.in_schema:
            return cast(
                Tuple[str, str],
                common.get_tuple_backend_name(material.id, catenate=False),
            )
        else:
            return ('record',)

    elif irtyputils.is_any(ir_typeref) or irtyputils.is_anyobject(ir_typeref):
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
        elif material.sql_type:
            return tuple(material.sql_type.split('.'))
        else:
            pg_type = base_type_name_map.get(material.id)
            if pg_type is None:
                real_name_hint = material.orig_name_hint or material.name_hint
                assert isinstance(real_name_hint, sn.QualName)
                # User-defined scalar type
                pg_type = common.get_scalar_backend_name(
                    material.id, real_name_hint.module, catenate=False)

            return pg_type


TableInfo = Tuple[Tuple[str, str], str, str]


def _source_table_info(
    schema: s_schema.Schema, pointer: s_pointers.Pointer
) -> TableInfo:
    table = common.get_backend_name(
        schema, not_none(pointer.get_source(schema)), catenate=False
    )
    ptr_name = pointer.get_shortname(schema).name
    if ptr_name.startswith('__') or ptr_name == 'id':
        col_name = ptr_name
    else:
        col_name = str(pointer.id)
    table_type = 'ObjectType'

    return table, table_type, col_name


def _pointer_table_info(
    schema: s_schema.Schema, pointer: s_pointers.Pointer
) -> TableInfo:
    table = common.get_backend_name(schema, pointer, catenate=False)
    col_name = 'target'
    table_type = 'link'

    return table, table_type, col_name


def _resolve_type(
    schema: s_schema.Schema, pointer: s_pointers.Pointer
) -> Tuple[str, ...]:
    column_type: Tuple[str, ...]

    pointer_target = pointer.get_target(schema)
    if pointer_target is not None:
        if pointer_target.is_object_type():
            column_type = ('uuid',)
        elif pointer_target.is_tuple(schema):
            column_type = common.get_backend_name(
                schema, pointer_target, catenate=False
            )
        else:
            column_type = pg_type_from_object(
                schema, pointer_target, persistent_tuples=True
            )
    else:
        # The target may not be known in circular object-to-object
        # linking scenarios.
        column_type = ('uuid',)

    return column_type


def _pointer_storable_in_source(
    schema: s_schema.Schema, pointer: s_pointers.Pointer
) -> bool:
    return pointer.singular(schema)


def _pointer_storable_in_pointer(
    schema: s_schema.Schema, pointer: s_pointers.Pointer
) -> bool:
    return not pointer.singular(schema) or pointer.has_user_defined_properties(
        schema
    )


@functools.lru_cache()
def get_pointer_storage_info(
    pointer: s_pointers.Pointer,
    *,
    schema: s_schema.Schema,
    source: Optional[s_obj.InheritingObject] = None,
    resolve_type: bool = True,
    link_bias: bool = False,
) -> PointerStorageInfo:
    assert not pointer.generic(
        schema
    ), "only specialized pointers can be stored"
    if pointer.get_computable(schema):
        material_ptrcls = None
    else:
        schema, material_ptrcls = pointer.material_type(schema)
    if material_ptrcls is not None:
        pointer = material_ptrcls

    if source is None:
        source = pointer.get_source(schema)

    is_lprop = pointer.is_link_property(schema)

    if resolve_type and schema is None:
        msg = 'PointerStorageInfo needs a schema to resolve column_type'
        raise ValueError(msg)

    if is_lprop and pointer.issubclass(
        schema, schema.get('std::target', type=s_obj.SubclassableObject)
    ):
        # Normalize link@target to link
        assert isinstance(source, s_pointers.Pointer)
        pointer = source
        is_lprop = False

    if isinstance(pointer, irast.TupleIndirectionLink):
        table = None
        table_type = 'ObjectType'
        col_name = pointer.get_shortname(schema).name
    elif is_lprop:
        assert source
        table = common.get_backend_name(schema, source, catenate=False)
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
        elif _pointer_storable_in_source(schema, pointer) and not link_bias:
            table, table_type, col_name = _source_table_info(schema, pointer)
        elif _pointer_storable_in_pointer(schema, pointer):
            table, table_type, col_name = _pointer_table_info(schema, pointer)
        else:
            return None  # type: ignore

    if resolve_type:
        column_type = _resolve_type(schema, pointer)
    else:
        column_type = None

    return PointerStorageInfo(
        table_name=table,
        table_type=table_type,
        column_name=col_name,  # type: ignore
        column_type=column_type,  # type: ignore
    )


@dataclasses.dataclass(kw_only=True, eq=False, slots=True)
class PointerStorageInfo:

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
def get_ptrref_storage_info(
    ptrref: irast.BasePointerRef, *,
    resolve_type: bool=...,
    link_bias: bool=...,
    allow_missing: bool=...,
) -> Optional[PointerStorageInfo]:
    ...


def get_ptrref_storage_info(
    ptrref: irast.BasePointerRef,
    *,
    resolve_type: bool = True,
    link_bias: bool = False,
    allow_missing: bool = False,
) -> Optional[PointerStorageInfo]:
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
    ptrref: irast.BasePointerRef,
    *,
    resolve_type: bool = True,
    link_bias: bool = False,
    allow_missing: bool = False,
) -> Optional[PointerStorageInfo]:

    if ptrref.material_ptr:
        ptrref = ptrref.material_ptr

    if ptrref.out_cardinality is None:
        # Guard against the IR generator failure to populate the PointerRef
        # cardinality correctly.
        raise RuntimeError(
            f'cannot determine backend storage parameters for the '
            f'{ptrref.name!r} pointer: the cardinality is not known')

    target = ptrref.out_target

    if isinstance(
        ptrref, (irast.TupleIndirectionPointerRef, irast.SpecialPointerRef)
    ):
        table = None
        table_type = 'ObjectType'
        col_name = ptrref.shortname.name

    elif ptrref.source_ptr is not None:
        # link property
        assert isinstance(ptrref, irast.PointerRef)
        source_ptr = ptrref.source_ptr

        table = common.get_pointer_backend_name(
            source_ptr.id, source_ptr.name.module, catenate=False
        )
        table_type = 'link'
        if ptrref.shortname.name in ('source', 'target'):
            col_name = ptrref.shortname.name
        else:
            col_name = str(ptrref.id)
    else:
        assert isinstance(ptrref, irast.PointerRef)
        source = ptrref.out_source

        if irtyputils.is_scalar(source):
            # This is a pseudo-link on an scalar (__type__)
            table = None
            table_type = 'ObjectType'
            col_name = None

        elif _ptrref_storable_in_source(ptrref) and not link_bias:
            assert isinstance(source.name_hint, sn.QualName)
            table = common.get_objtype_backend_name(
                source.id, source.name_hint.module, catenate=False
            )
            ptrname = ptrref.shortname.name
            if ptrname.startswith('__') or ptrname == 'id':
                col_name = ptrname
            else:
                col_name = str(ptrref.id)
            table_type = 'ObjectType'

        elif _ptrref_storable_in_pointer(ptrref):
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

    column_type: Tuple[str, ...] | None
    if resolve_type:
        if irtyputils.is_object(target):
            column_type = ('uuid',)
        else:
            column_type = pg_type_from_ir_typeref(
                target, persistent_tuples=True)
    else:
        column_type = None

    return PointerStorageInfo(
        table_name=table,
        table_type=table_type,
        column_name=col_name,  # type: ignore
        column_type=column_type,  # type: ignore
    )


def _ptrref_storable_in_source(ptrref: irast.BasePointerRef) -> bool:
    return ptrref.out_cardinality.is_single()


def _ptrref_storable_in_pointer(ptrref: irast.BasePointerRef) -> bool:
    if ptrref.union_components:
        return all(
            _ptrref_storable_in_pointer(c) for c in ptrref.union_components
        )
    else:
        return (
            ptrref.out_cardinality.is_multi()
            or ptrref.has_properties
        )
