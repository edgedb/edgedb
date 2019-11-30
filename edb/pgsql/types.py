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

import collections
import functools
from typing import *  # NoQA

from edb.common import uuidgen
from edb.edgeql import qltypes

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from edb.schema import abc as s_abc
from edb.schema import scalars as s_scalars
from edb.schema import objtypes as s_objtypes
from edb.schema import name as sn
from edb.schema import objects as s_obj
from edb.schema import schema as s_schema
from edb.schema import types as s_types

from . import common
from .common import quote_literal as ql


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
    s_obj.get_known_type_id('std::datetime'): ('timestamptz',),
    s_obj.get_known_type_id('std::local_datetime'): ('timestamp',),
    s_obj.get_known_type_id('std::local_date'): ('date',),
    s_obj.get_known_type_id('std::local_time'): ('time',),
    s_obj.get_known_type_id('std::duration'): ('interval',),
    s_obj.get_known_type_id('std::bytes'): ('bytea',),
    s_obj.get_known_type_id('std::json'): ('jsonb',),
}

base_type_name_map_r = {
    'character varying': sn.Name('std::str'),
    'character': sn.Name('std::str'),
    'text': sn.Name('std::str'),
    'numeric': sn.Name('std::decimal'),
    'edgedb.bigint_t': sn.Name('std::bigint'),
    'bigint_t': sn.Name('std::bigint'),
    'int4': sn.Name('std::int32'),
    'integer': sn.Name('std::int32'),
    'bigint': sn.Name('std::int64'),
    'int8': sn.Name('std::int64'),
    'int2': sn.Name('std::int16'),
    'smallint': sn.Name('std::int16'),
    'boolean': sn.Name('std::bool'),
    'bool': sn.Name('std::bool'),
    'double precision': sn.Name('std::float64'),
    'float8': sn.Name('std::float64'),
    'real': sn.Name('std::float32'),
    'float4': sn.Name('std::float32'),
    'uuid': sn.Name('std::uuid'),
    'timestamp with time zone': sn.Name('std::datetime'),
    'timestamptz': sn.Name('std::datetime'),
    'timestamp': sn.Name('std::local_datetime'),
    'date': sn.Name('std::local_date'),
    'time': sn.Name('std::local_time'),
    'interval': sn.Name('std::duration'),
    'bytea': sn.Name('std::bytes'),
    'jsonb': sn.Name('std::json'),
}


def is_builtin_scalar(schema, scalar):
    return scalar.id in base_type_name_map


def get_scalar_base(schema, scalar) -> Tuple[str, ...]:
    base = base_type_name_map.get(scalar.id)
    if base is not None:
        return base

    for ancestor in scalar.get_ancestors(schema).objects(schema):
        if not ancestor.get_is_abstract(schema):
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

    scalar = scalar.material_type(schema)
    is_enum = scalar.is_enum(schema)

    if is_enum:
        base = scalar
    else:
        base = get_scalar_base(schema, scalar.material_type(schema))

    column_type = base_type_name_map.get(scalar.id)
    if column_type:
        column_type = base
    else:
        column_type = common.get_backend_name(schema, scalar, catenate=False)

    return column_type


def pg_type_from_object(
        schema: s_schema.Schema,
        obj: s_obj.Object,
        persistent_tuples: bool=False) -> Tuple[str, ...]:

    if isinstance(obj, s_scalars.ScalarType):
        return pg_type_from_scalar(schema, obj)

    elif obj.is_type() and obj.is_anytuple():
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
            if len(tp) == 1:
                return (tp[0] + '[]',)
            else:
                return (tp[0], tp[1] + '[]')

    elif isinstance(obj, s_objtypes.ObjectType):
        return ('uuid',)

    elif obj.is_type() and obj.is_any():
        return ('anyelement',)

    else:
        raise ValueError(f'could not determine PG type for {obj!r}')


def pg_type_from_ir_typeref(
        ir_typeref: irast.TypeRef, *,
        serialized: bool = False,
        persistent_tuples: bool = False) -> Tuple[str, ...]:

    if irtyputils.is_array(ir_typeref):
        if (irtyputils.is_generic(ir_typeref)
                or irtyputils.is_abstract(ir_typeref.subtypes[0])):
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
        if persistent_tuples:
            return common.get_tuple_backend_name(ir_typeref.id, catenate=False)
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
                    material.id, material.module_id, catenate=False)

            return pg_type


class _PointerStorageInfo:
    @classmethod
    def _source_table_info(cls, schema, pointer):
        table = common.get_backend_name(
            schema, pointer.get_source(schema), catenate=False)
        col_name = pointer.get_shortname(schema).name
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
            elif pointer_target.is_tuple():
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
            col_name = pointer.get_shortname(schema).name
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
    material_ptrcls = pointer.material_type(schema)
    if material_ptrcls is not None:
        pointer = material_ptrcls
    return _PointerStorageInfo(
        schema, pointer, source=source, resolve_type=resolve_type,
        link_bias=link_bias)


class PointerStorageInfo(NamedTuple):

    table_name: Tuple[str, str]
    table_type: str
    column_name: str
    column_type: Tuple[str, str]


@functools.lru_cache()
def get_ptrref_storage_info(
        ptrref: irast.PointerRef, *,
        source=None, resolve_type=True, link_bias=False):

    if ptrref.material_ptr:
        ptrref = ptrref.material_ptr

    if ptrref.out_cardinality is None:
        # Guard against the IR generator failure to populate the PointerRef
        # cardinality correctly.
        raise RuntimeError(
            f'cannot determine backend storage parameters for the '
            f'{ptrref.name!r} pointer: the cardinality is not known')

    is_lprop = ptrref.parent_ptr is not None

    if source is None:
        if is_lprop:
            source = ptrref.parent_ptr
        else:
            source = ptrref.out_source

        target = ptrref.out_target

    if is_lprop and ptrref.std_parent_name == 'std::target':
        # Normalize link@target to link
        ptrref = source
        is_lprop = False

    if isinstance(ptrref, irast.TupleIndirectionPointerRef):
        table = None
        table_type = 'ObjectType'
        col_name = ptrref.shortname.name

    elif is_lprop:
        table = common.get_pointer_backend_name(source.id, source.module_id)
        table_type = 'link'
        col_name = ptrref.shortname.name
    else:
        if irtyputils.is_scalar(source):
            # This is a pseudo-link on an scalar (__type__)
            table = None
            table_type = 'ObjectType'
            col_name = None

        elif _storable_in_source(ptrref) and not link_bias:
            table = common.get_objtype_backend_name(
                source.id, source.module_id)
            col_name = ptrref.shortname.name
            table_type = 'ObjectType'

        elif _storable_in_pointer(ptrref):
            table = common.get_pointer_backend_name(
                ptrref.id, ptrref.module_id)
            col_name = 'target'
            table_type = 'link'

        elif not link_bias:
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
    return ptrref.out_cardinality is qltypes.Cardinality.ONE


def _storable_in_pointer(ptrref: irast.PointerRef) -> bool:
    return (
        ptrref.out_cardinality is qltypes.Cardinality.MANY
        or ptrref.has_properties
    )


_TypeDescNode = collections.namedtuple(
    '_TypeDescNode', ['id', 'maintype', 'name', 'position',
                      'collection', 'subtypes', 'dimensions'],
    module=__name__)


class TypeDescNode(_TypeDescNode):

    def __new__(cls, **kwargs):
        if not kwargs.get('id'):
            kwargs['id'] = cls._get_id(kwargs)
        return super().__new__(cls, **kwargs)

    @classmethod
    def _get_id(cls, data):
        if data['collection'] == 'tuple' and not data['subtypes']:
            return s_obj.get_known_type_id('empty-tuple')
        if data['name'] == 'anytype':
            return s_obj.get_known_type_id('anytype')

        # A type desc node is uniquely identified by it's type,
        # and the name and position within a parent type.
        id_str = f"{data['maintype']!r}::{data['name']!r}::{data['position']}"
        return uuidgen.uuid5(s_types.TYPE_ID_NAMESPACE, id_str)

    def to_sql_expr(self):
        if self.subtypes:
            subtype_list = ', '.join(ql(str(st)) for st in self.subtypes)
            subtypes = f'ARRAY[{subtype_list}]::uuid[]'
        else:
            subtypes = 'ARRAY[]::uuid[]'

        if self.dimensions:
            dimensions_list = ', '.join(str(d) for d in self.dimensions)
            dimensions = f'ARRAY[{dimensions_list}]::smallint[]'
        else:
            dimensions = 'ARRAY[]::smallint[]'

        items = [
            ql(str(self.id)),
            ql(str(self.maintype)),
            ql(self.name) if self.name else 'NULL',
            str(self.position) if self.position is not None else 'NULL',
            ql(self.collection) if self.collection else 'NULL',
            subtypes,
            dimensions,
        ]

        return 'ROW(' + ', '.join(items) + ')::edgedb.type_desc_node_t'


class TypeDesc:

    def __init__(self, types: List[TypeDescNode]) -> None:
        self.types = types

    def to_sql_expr(self):
        nodes = ', '.join(node.to_sql_expr() for node in self.types)
        return (
            f'ROW(ARRAY[{nodes}]::edgedb.type_desc_node_t[])'
            f'::edgedb.typedesc_t'
        )

    @classmethod
    def from_type(cls, schema, type: s_abc.Type) -> TypeDesc:
        nodes = []
        cls._get_typedesc(schema, [(None, type)], nodes)
        return cls(nodes)

    @classmethod
    def _get_typedesc(cls, schema, types, typedesc, *, is_root=True):
        result = []
        indexes = []
        for _ in types:
            # Fill the result with placeholders as we want the
            # parent types to go first.
            typedesc.append(())
            indexes.append(len(typedesc) - 1)

        for i, (tn, t) in enumerate(types):
            if isinstance(t, s_abc.Collection):
                if isinstance(t, s_abc.Tuple) and t.is_named(schema):
                    stypes = list(t.iter_subtypes(schema))
                else:
                    stypes = [(None, st) for st in t.get_subtypes(schema)]

                subtypes = cls._get_typedesc(
                    schema, stypes, typedesc, is_root=False)
                if isinstance(t, s_abc.Array):
                    dimensions = t.get_dimensions(schema)
                else:
                    dimensions = []
                desc = TypeDescNode(
                    maintype=t.id, name=tn, collection=t.schema_name,
                    subtypes=subtypes, dimensions=dimensions,
                    position=i if not is_root else None)
            elif t.is_type() and t.is_any():
                desc = TypeDescNode(
                    maintype=t.id, name=tn, collection=None,
                    subtypes=[], dimensions=[],
                    position=i if not is_root else None)
            elif t.is_type() and t.is_anytuple():
                desc = TypeDescNode(
                    maintype=t.id, name=tn, collection=None,
                    subtypes=[], dimensions=[],
                    position=i if not is_root else None)
            else:
                desc = TypeDescNode(
                    maintype=t.id,
                    name=tn,
                    collection=None,
                    subtypes=[],
                    dimensions=[],
                    position=i if not is_root else None)

            typedesc[indexes[i]] = desc
            result.append(desc.id)

        return result
