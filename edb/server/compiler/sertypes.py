#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
from typing import *

import collections.abc
import dataclasses
import enum
import functools
import io
import struct
import typing
import uuid

import immutables

from edb import errors
from edb.common import binwrapper
from edb.common import value_dispatch
from edb.common import uuidgen

from edb.protocol import enums as p_enums
from edb.server import config

from edb.edgeql import qltypes

from edb.schema import globals as s_globals
from edb.schema import links as s_links
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import scalars as s_scalars
from edb.schema import schema as s_schema
from edb.schema import types as s_types

from edb.ir import ast as irast

from edb.ir import statypes

from . import enums


_int32_packer = cast(Callable[[int], bytes], struct.Struct('!l').pack)
_uint32_packer = cast(Callable[[int], bytes], struct.Struct('!L').pack)
_uint16_packer = cast(Callable[[int], bytes], struct.Struct('!H').pack)
_uint8_packer = cast(Callable[[int], bytes], struct.Struct('!B').pack)
_int64_struct = struct.Struct('!q')


EMPTY_TUPLE_ID = s_obj.get_known_type_id('empty-tuple')
EMPTY_TUPLE_DESC = b'\x04' + EMPTY_TUPLE_ID.bytes + b'\x00\x00'

UUID_TYPE_ID = s_obj.get_known_type_id('std::uuid')

STR_TYPE_ID = s_obj.get_known_type_id('std::str')
STR_TYPE_DESC = b'\x02' + STR_TYPE_ID.bytes

NULL_TYPE_ID = uuidgen.UUID(b'\x00' * 16)
NULL_TYPE_DESC = b''


class DescriptorTag(bytes, enum.Enum):
    SET = b'\x00'
    SHAPE = b'\x01'
    BASE_SCALAR = b'\x02'
    SCALAR = b'\x03'
    TUPLE = b'\x04'
    NAMEDTUPLE = b'\x05'
    ARRAY = b'\x06'
    ENUM = b'\x07'
    INPUT_SHAPE = b'\x08'
    RANGE = b'\x09'

    ANNO_TYPENAME = b'\xff'


class ShapePointerFlags(enum.IntFlag):
    IS_IMPLICIT = enum.auto()
    IS_LINKPROP = enum.auto()
    IS_LINK = enum.auto()


EMPTY_BYTEARRAY = bytearray()


def _encode_str(data: str) -> bytes:
    return data.encode('utf-8')


def _decode_str(data: bytes) -> str:
    return data.decode('utf-8')


def _encode_bool(data: bool) -> bytes:
    return b'\x01' if data else b'\x00'


def _decode_bool(data: bytes) -> bool:
    return bool(data[0])


def _encode_int64(data: int) -> bytes:
    return _int64_struct.pack(data)


def _decode_int64(data: bytes) -> int:
    return _int64_struct.unpack(data)[0]  # type: ignore [no-any-return]


def _string_packer(s: str) -> bytes:
    s_bytes = s.encode('utf-8')
    return _uint32_packer(len(s_bytes)) + s_bytes


def cardinality_from_ptr(
    ptr: s_pointers.Pointer | s_globals.Global,
    schema: s_schema.Schema,
) -> enums.Cardinality:
    required = ptr.get_required(schema)
    schema_card = ptr.get_cardinality(schema)
    ir_card = qltypes.Cardinality.from_schema_value(required, schema_card)
    return enums.cardinality_from_ir_value(ir_card)


InputShapeElement = tuple[str, s_types.Type, enums.Cardinality]
InputShapeMap = Mapping[s_types.Type, Iterable[InputShapeElement]]

ViewShapeMap = Mapping[s_obj.Object, list[s_pointers.Pointer]]
ViewShapeMetadataMap = Mapping[s_types.Type, irast.ViewShapeMetadata]


class Context:
    def __init__(
        self,
        *,
        schema: s_schema.Schema,
        protocol_version: tuple[int, int],
        view_shapes: ViewShapeMap = immutables.Map(),
        view_shapes_metadata: ViewShapeMetadataMap = immutables.Map(),
        follow_links: bool = True,
        inline_typenames: bool = False,
        name_filter: str = "",
    ) -> None:
        self.schema = schema
        self.view_shapes = view_shapes
        self.view_shapes_metadata = view_shapes_metadata
        self.protocol_version = protocol_version
        self.follow_links = follow_links
        self.inline_typenames = inline_typenames
        self.name_filter = name_filter

        self.buffer: list[bytes] = []
        self.anno_buffer: list[bytes] = []
        self.uuid_to_pos: dict[uuid.UUID, int] = {}

    def derive(self) -> Context:
        ctx = type(self)(
            schema=self.schema,
            protocol_version=self.protocol_version,
            view_shapes=self.view_shapes,
            view_shapes_metadata=self.view_shapes_metadata,
            follow_links=self.follow_links,
            inline_typenames=self.inline_typenames,
            name_filter=self.name_filter,
        )
        ctx.buffer = self.buffer.copy()
        ctx.anno_buffer = self.anno_buffer.copy()
        ctx.uuid_to_pos = self.uuid_to_pos.copy()
        return ctx


def _get_collection_type_id(
    coll_type: str,
    subtypes: list[uuid.UUID],
    element_names: list[str] | None = None,
) -> uuid.UUID:
    if coll_type == 'tuple' and not subtypes:
        return s_obj.get_known_type_id('empty-tuple')

    string_id = f'{coll_type}\x00{":".join(map(str, subtypes))}'
    if element_names:
        string_id += f'\x00{":".join(element_names)}'
    return uuidgen.uuid5(s_types.TYPE_ID_NAMESPACE, string_id)


def _get_object_shape_id(
    coll_type: str,
    subtypes: list[uuid.UUID],
    element_names: Optional[list[str]] = None,
    cardinalities: Optional[list[enums.Cardinality]] = None,
    *,
    links_props: Optional[list[bool]] = None,
    links: Optional[list[bool]] = None,
    has_implicit_fields: bool = False,
) -> uuid.UUID:
    parts = [coll_type]
    parts.append(":".join(map(str, subtypes)))
    if element_names:
        parts.append(":".join(element_names))
    if cardinalities:
        parts.append(":".join(chr(c._value_) for c in cardinalities))
    string_id = "\x00".join(parts)
    string_id += f'{has_implicit_fields!r};{links_props!r};{links!r}'
    return uuidgen.uuid5(s_types.TYPE_ID_NAMESPACE, string_id)


def _get_set_type_id(basetype_id: uuid.UUID) -> uuid.UUID:
    return uuidgen.uuid5(
        s_types.TYPE_ID_NAMESPACE, 'set-of::' + str(basetype_id))


def _register_type_id(
    type_id: uuid.UUID,
    ctx: Context,
) -> uuid.UUID:
    if type_id not in ctx.uuid_to_pos:
        ctx.uuid_to_pos[type_id] = len(ctx.uuid_to_pos)
    return type_id


def _describe_set(
    t: s_types.Type,
    *,
    ctx: Context,
) -> uuid.UUID:
    type_id = _describe_type(t, ctx=ctx)
    set_id = _get_set_type_id(type_id)
    if set_id in ctx.uuid_to_pos:
        return set_id

    buf = ctx.buffer

    # .tag
    buf.append(DescriptorTag.SET)
    # .id
    buf.append(set_id.bytes)
    # .type
    buf.append(_type_ref_id_packer(type_id, ctx=ctx))

    return _register_type_id(set_id, ctx=ctx)


# The encoding format is documented in edb/api/types.txt.
@functools.singledispatch
def _describe_type(t: s_types.Type, *, ctx: Context) -> uuid.UUID:
    raise errors.InternalServerError(
        f'cannot describe type {t.get_name(ctx.schema)}')


def _type_ref_packer(t: s_types.Type, *, ctx: Context) -> bytes:
    """Return typedesc representation of a type reference."""
    return _type_ref_id_packer(_describe_type(t, ctx=ctx), ctx=ctx)


def _type_ref_id_packer(type_id: uuid.UUID, *, ctx: Context) -> bytes:
    """Return typedesc representation of a type reference by type id."""
    return _uint16_packer(ctx.uuid_to_pos[type_id])


# Tuple -> TupleTypeDescriptor
@_describe_type.register
def _describe_tuple(t: s_types.Tuple, *, ctx: Context) -> uuid.UUID:
    subtypes = [
        _describe_type(st, ctx=ctx)
        for st in t.get_subtypes(ctx.schema)
    ]

    is_named = t.is_named(ctx.schema)
    if is_named:
        element_names = list(t.get_element_names(ctx.schema))
        assert len(element_names) == len(subtypes)
        tag = DescriptorTag.NAMEDTUPLE
    else:
        element_names = None
        tag = DescriptorTag.TUPLE

    type_id = _get_collection_type_id(
        t.get_schema_name(), subtypes, element_names)

    if type_id in ctx.uuid_to_pos:
        return type_id

    buf = []

    # .tag
    buf.append(tag._value_)
    # .id
    buf.append(type_id.bytes)
    # .element_count
    buf.append(_uint16_packer(len(subtypes)))
    if element_names is not None:
        # .elements
        for el_name, el_type_id in zip(element_names, subtypes):
            # TupleElement.name
            buf.append(_string_packer(el_name))
            # TupleElement.type
            buf.append(_type_ref_id_packer(el_type_id, ctx=ctx))
    else:
        # .element_types
        for el_type_id in subtypes:
            buf.append(_type_ref_id_packer(el_type_id, ctx=ctx))

    ctx.buffer.extend(buf)

    return _register_type_id(type_id, ctx=ctx)


# Array -> ArrayTypeDescriptor
@_describe_type.register
def _describe_array(t: s_types.Array, *, ctx: Context) -> uuid.UUID:
    subtypes = [
        _describe_type(st, ctx=ctx)
        for st in t.get_subtypes(ctx.schema)
    ]

    assert len(subtypes) == 1
    type_id = _get_collection_type_id(t.get_schema_name(), subtypes)

    if type_id in ctx.uuid_to_pos:
        return type_id

    buf = []

    # .tag
    buf.append(DescriptorTag.ARRAY._value_)
    # .id
    buf.append(type_id.bytes)
    # .type
    buf.append(_type_ref_id_packer(subtypes[0], ctx=ctx))
    # .dimension_count (currently always 1)
    buf.append(_uint16_packer(1))
    # .dimensions (currently always unbounded)
    buf.append(_int32_packer(-1))

    ctx.buffer.extend(buf)

    return _register_type_id(type_id, ctx=ctx)


# Range -> RangeTypeDescriptor
@_describe_type.register
def _describe_range(t: s_types.Range, *, ctx: Context) -> uuid.UUID:
    subtypes = [
        _describe_type(st, ctx=ctx)
        for st in t.get_subtypes(ctx.schema)
    ]

    assert len(subtypes) == 1
    type_id = _get_collection_type_id(t.get_schema_name(), subtypes)

    if type_id in ctx.uuid_to_pos:
        return type_id

    buf = []

    # .tag
    buf.append(DescriptorTag.RANGE._value_)
    # .id
    buf.append(type_id.bytes)
    # .type
    buf.append(_type_ref_id_packer(subtypes[0], ctx=ctx))

    ctx.buffer.extend(buf)

    return _register_type_id(type_id, ctx=ctx)


# ObjectType (representing a shape) -> ObjectShapeDescriptor
@_describe_type.register
def _describe_object_shape(
    t: s_objtypes.ObjectType,
    *,
    ctx: Context,
) -> uuid.UUID:
    ctx.schema, mt = t.material_type(ctx.schema)
    base_type_name = str(mt.get_name(ctx.schema))

    subtypes = []
    element_names = []
    link_props = []
    links = []
    cardinalities: list[enums.Cardinality] = []

    metadata = ctx.view_shapes_metadata.get(t)
    implicit_id = metadata is not None and metadata.has_implicit_id

    for ptr in ctx.view_shapes.get(t, ()):
        name = ptr.get_shortname(ctx.schema).name
        if not name.startswith(ctx.name_filter):
            continue
        name = name.removeprefix(ctx.name_filter)
        if ptr.singular(ctx.schema):
            if isinstance(ptr, s_links.Link) and not ctx.follow_links:
                uuid_t = ctx.schema.get('std::uuid', type=s_scalars.ScalarType)
                subtype_id = _describe_type(uuid_t, ctx=ctx)
            else:
                tgt = ptr.get_target(ctx.schema)
                assert tgt is not None
                subtype_id = _describe_type(tgt, ctx=ctx)
        else:
            if isinstance(ptr, s_links.Link) and not ctx.follow_links:
                raise errors.InternalServerError(
                    'cannot describe multi links when follow_links=False'
                )
            else:
                tgt = ptr.get_target(ctx.schema)
                assert tgt is not None
                subtype_id = _describe_set(tgt, ctx=ctx)
        subtypes.append(subtype_id)
        element_names.append(name)
        link_props.append(False)
        links.append(not ptr.is_property(ctx.schema))
        cardinalities.append(cardinality_from_ptr(ptr, ctx.schema))

    t_rptr = t.get_rptr(ctx.schema)
    if t_rptr is not None and (rptr_ptrs := ctx.view_shapes.get(t_rptr)):
        # There are link properties in the mix
        for ptr in rptr_ptrs:
            tgt = ptr.get_target(ctx.schema)
            assert tgt is not None
            if ptr.singular(ctx.schema):
                subtype_id = _describe_type(tgt, ctx=ctx)
            else:
                subtype_id = _describe_set(tgt, ctx=ctx)
            subtypes.append(subtype_id)
            element_names.append(ptr.get_shortname(ctx.schema).name)
            link_props.append(True)
            links.append(False)
            cardinalities.append(cardinality_from_ptr(ptr, ctx.schema))

    assert len(subtypes) == len(element_names)
    type_id = _get_object_shape_id(
        base_type_name,
        subtypes,
        element_names,
        cardinalities,
        links_props=link_props,
        links=links,
        has_implicit_fields=implicit_id,
    )

    if type_id in ctx.uuid_to_pos:
        return type_id

    buf = []

    # .tag
    buf.append(DescriptorTag.SHAPE._value_)
    # .id
    buf.append(type_id.bytes)
    # .element_count
    buf.append(_uint16_packer(len(subtypes)))
    # .elements
    for el_name, el_type_id, el_lp, el_l, el_c in (
        zip(element_names, subtypes, link_props, links, cardinalities)
    ):
        flags = 0
        if el_lp:
            flags |= ShapePointerFlags.IS_LINKPROP
        if (implicit_id and el_name == 'id') or el_name == '__tid__':
            if el_type_id != UUID_TYPE_ID:
                raise errors.InternalServerError(
                    f"{el_name!r} is expected to be a 'std::uuid' singleton")
            flags |= ShapePointerFlags.IS_IMPLICIT
        elif el_name == '__tname__':
            if el_type_id != STR_TYPE_ID:
                raise errors.InternalServerError(
                    f"{el_name!r} is expected to be a 'std::str' singleton")
            flags |= ShapePointerFlags.IS_IMPLICIT
        if el_l:
            flags |= ShapePointerFlags.IS_LINK

        if ctx.protocol_version >= (0, 11):
            # ShapeElement.flags
            buf.append(_uint32_packer(flags))
            # ShapeElement.cardinality
            buf.append(_uint8_packer(el_c.value))
        else:
            # ShapeElement.flags
            buf.append(_uint8_packer(flags))

        # ShapeElement.name
        buf.append(_string_packer(el_name))
        # ShapeElement.type
        buf.append(_type_ref_id_packer(el_type_id, ctx=ctx))

    ctx.buffer.extend(buf)

    return _register_type_id(type_id, ctx=ctx)


@_describe_type.register
def _describe_scalar_type(
    t: s_scalars.ScalarType,
    *,
    ctx: Context,
) -> uuid.UUID:
    ctx.schema, smt = t.material_type(ctx.schema)
    type_id = smt.id
    if type_id in ctx.uuid_to_pos:
        # already described
        return type_id

    if smt.is_enum(ctx.schema):
        return _describe_enum(smt, ctx=ctx)
    else:
        return _describe_regular_scalar(smt, ctx=ctx)


# ScalarType (regular) -> [Base]ScalarTypeDescriptor
def _describe_regular_scalar(
    t: s_scalars.ScalarType,
    *,
    ctx: Context,
) -> uuid.UUID:
    buf = []
    fundamental_type = t.get_topmost_concrete_base(ctx.schema)
    type_id = t.id
    type_is_fundamental = t == fundamental_type

    if type_is_fundamental:
        # .tag
        buf.append(DescriptorTag.BASE_SCALAR._value_)
        # .id
        buf.append(type_id.bytes)
    else:
        # .tag
        buf.append(DescriptorTag.SCALAR._value_)
        # .id
        buf.append(type_id.bytes)
        # .base_type_pos
        buf.append(_type_ref_packer(fundamental_type, ctx=ctx))

        if ctx.inline_typenames:
            _add_annotation(t, ctx=ctx)

    ctx.buffer.extend(buf)

    return _register_type_id(type_id, ctx=ctx)


# ScalarType (enum) -> EnumTypeDescriptor
def _describe_enum(
    enum: s_scalars.ScalarType,
    *,
    ctx: Context,
) -> uuid.UUID:
    buf = []
    enum_values = enum.get_enum_values(ctx.schema)
    assert enum_values is not None

    # .tag
    buf.append(DescriptorTag.ENUM._value_)
    # .id
    buf.append(enum.id.bytes)
    # .member_count
    buf.append(_uint16_packer(len(enum_values)))
    # .members
    for enum_val in enum_values:
        buf.append(_string_packer(enum_val))

    if ctx.inline_typenames:
        _add_annotation(enum, ctx=ctx)

    ctx.buffer.extend(buf)

    return _register_type_id(enum.id, ctx=ctx)


@overload
def describe_input_shape(
    t: s_types.Type,
    input_shapes: InputShapeMap,
    *,
    prepare_state: Literal[False],
    ctx: Context,
) -> uuid.UUID:
    ...


@overload
def describe_input_shape(  # noqa: F811
    t: s_types.Type,
    input_shapes: InputShapeMap,
    *,
    ctx: Context,
) -> uuid.UUID:
    ...


@overload
def describe_input_shape(  # noqa: F811
    t: s_types.Type,
    input_shapes: InputShapeMap,
    *,
    prepare_state: Literal[True],
    ctx: Context,
) -> None:
    ...


def describe_input_shape(  # noqa: F811
    t: s_types.Type,
    input_shapes: InputShapeMap,
    *,
    prepare_state: bool = False,
    ctx: Context,
) -> Optional[uuid.UUID]:
    if t in input_shapes:
        element_names = []
        subtypes = []
        cardinalities = []
        for name, subtype, cardinality in input_shapes[t]:
            if (
                cardinality == enums.Cardinality.MANY or
                cardinality == enums.Cardinality.AT_LEAST_ONE
            ):
                subtype_id = _describe_set(subtype, ctx=ctx)
            else:
                subtype_id = describe_input_shape(
                    subtype, input_shapes, ctx=ctx)
            element_names.append(name)
            subtypes.append(subtype_id)
            cardinalities.append(cardinality)

        assert len(subtypes) == len(element_names)

        if prepare_state:
            return None

        ctx.schema, mt = t.material_type(ctx.schema)
        base_type_name = str(mt.get_name(ctx.schema))

        type_id = _get_object_shape_id(
            base_type_name, subtypes, element_names, cardinalities)

        if type_id in ctx.uuid_to_pos:
            return type_id

        buf = []
        # .tag
        buf.append(DescriptorTag.INPUT_SHAPE._value_)
        # .id
        buf.append(type_id.bytes)
        # .element_count
        buf.append(_uint16_packer(len(subtypes)))
        # .elements
        for el_name, el_type_id, el_c in (
            zip(element_names, subtypes, cardinalities)
        ):
            # ShapeElement.flags
            buf.append(_uint32_packer(0))
            # ShapeElement.cardinality
            buf.append(_uint8_packer(el_c.value))
            # ShapeElement.name
            buf.append(_string_packer(el_name))
            # ShapeElement.type
            buf.append(_type_ref_id_packer(el_type_id, ctx=ctx))

        ctx.buffer.extend(buf)

        return _register_type_id(type_id, ctx=ctx)
    else:
        return _describe_type(t, ctx=ctx)


def _add_annotation(t: s_types.Type, *, ctx: Context) -> None:
    buf = ctx.anno_buffer
    # .tag
    buf.append(DescriptorTag.ANNO_TYPENAME._value_)
    # .id
    buf.append(t.id.bytes)
    # .annotation
    buf.append(_string_packer(t.get_displayname(ctx.schema)))


def describe_params(
    *,
    schema: s_schema.Schema,
    params: list[tuple[str, s_types.Type, bool]],
    protocol_version: tuple[int, int],
) -> tuple[bytes, uuid.UUID]:
    assert protocol_version >= (0, 12)

    if not params:
        return NULL_TYPE_DESC, NULL_TYPE_ID

    ctx = Context(
        schema=schema,
        protocol_version=protocol_version,
    )
    params_buf = []

    for param_name, param_type, param_req in params:
        param_type_id = _describe_type(param_type, ctx=ctx)
        # ShapeElement.flags
        params_buf.append(_uint32_packer(0))
        # ShapeElement.cardinality
        params_buf.append(_uint8_packer(
            p_enums.Cardinality.ONE.value if param_req else
            p_enums.Cardinality.AT_MOST_ONE.value
        ))
        # ShapeElement.name
        params_buf.append(_string_packer(param_name))
        # ShapeElement.type
        params_buf.append(_type_ref_id_packer(param_type_id, ctx=ctx))

    buffer_encoded = b''.join(ctx.buffer)

    full_params = EMPTY_BYTEARRAY.join([
        buffer_encoded,
        DescriptorTag.SHAPE._value_,
        NULL_TYPE_ID.bytes,  # will be replaced with `params_id` later
        _uint16_packer(len(params)),
        *params_buf,
        *ctx.anno_buffer,
    ])

    params_id = uuidgen.uuid5_bytes(s_types.TYPE_ID_NAMESPACE, full_params)
    id_pos = len(buffer_encoded) + 1
    full_params[id_pos : id_pos + 16] = params_id.bytes

    return bytes(full_params), params_id


def describe(
    schema: s_schema.Schema,
    typ: s_types.Type,
    view_shapes: ViewShapeMap = immutables.Map(),
    view_shapes_metadata: ViewShapeMetadataMap = immutables.Map(),
    *,
    protocol_version: tuple[int, int],
    follow_links: bool = True,
    inline_typenames: bool = False,
    name_filter: str = "",
) -> typing.Tuple[bytes, uuid.UUID]:
    ctx = Context(
        schema=schema,
        view_shapes=view_shapes,
        view_shapes_metadata=view_shapes_metadata,
        protocol_version=protocol_version,
        follow_links=follow_links,
        inline_typenames=inline_typenames,
        name_filter=name_filter,
    )
    type_id = _describe_type(typ, ctx=ctx)
    out = b''.join(ctx.buffer) + b''.join(ctx.anno_buffer)
    return out, type_id


def describe_str() -> tuple[bytes, uuid.UUID]:
    return STR_TYPE_DESC, STR_TYPE_ID


#
# Type descriptor parsing
#

class ParseContext:
    def __init__(
        self,
        protocol_version: tuple[int, int],
    ) -> None:
        self.protocol_version = protocol_version
        self.codecs_list: list[TypeDesc] = []


def parse(
    typedesc: bytes,
    protocol_version: tuple[int, int],
) -> TypeDesc:
    """Unmarshal a byte stream with one or more type descriptors."""
    ctx = ParseContext(protocol_version)
    buf = io.BytesIO(typedesc)
    wrapped = binwrapper.BinWrapper(buf)
    while buf.tell() < len(typedesc):
        _parse(wrapped, ctx=ctx)
    if not ctx.codecs_list:
        raise errors.InternalServerError('could not parse type descriptor')
    return ctx.codecs_list[-1]


def _parse(desc: binwrapper.BinWrapper, ctx: ParseContext) -> None:
    """Unmarshal the next type descriptor from the byte stream."""
    t = desc.read_bytes(1)

    try:
        tag = DescriptorTag(t)
    except ValueError:
        if (t[0] >= 0x80 and t[0] <= 0xff):
            # Ignore all type annotations.
            _parse_string(desc)
            return
        else:
            raise NotImplementedError(
                f'no codec implementation for EdgeDB data class {hex(t[0])}')
    else:
        ctx.codecs_list.append(_parse_descriptor(tag, desc, ctx=ctx))


#
# Parsing helpers
#

def _parse_type_id(desc: binwrapper.BinWrapper) -> uuid.UUID:
    return uuidgen.from_bytes(desc.read_bytes(16))


def _parse_string(desc: binwrapper.BinWrapper) -> str:
    b = desc.read_len32_prefixed_bytes()
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError as e:
        raise errors.InternalServerError(
            f"malformed type descriptor: invalid UTF-8 string "
            f"at stream position {desc.tell()}") from e


def _parse_strings(desc: binwrapper.BinWrapper) -> list[str]:
    num = desc.read_ui16()
    return [_parse_string(desc) for _ in range(num)]


def _parse_type_ref(
    desc: binwrapper.BinWrapper,
    *,
    ctx: ParseContext,
) -> TypeDesc:
    offset = desc.read_ui16()
    try:
        return ctx.codecs_list[offset]
    except KeyError:
        raise errors.InternalServerError(
            f"malformed type descriptor: dangling type reference: {offset} "
            f"at stream position {desc.tell()}") from None


def _parse_type_refs(
    desc: binwrapper.BinWrapper,
    *,
    ctx: ParseContext,
) -> list[TypeDesc]:
    els = desc.read_ui16()
    return [_parse_type_ref(desc, ctx=ctx) for _ in range(els)]


#
# Parsing dispatch.
#

@value_dispatch.value_dispatch
def _parse_descriptor(
    tag: DescriptorTag,
    desc: binwrapper.BinWrapper,
    ctx: ParseContext,
) -> TypeDesc:
    raise AssertionError(
        f'no codec implementation for EdgeDB data class {tag}')


@_parse_descriptor.register(DescriptorTag.SET)
def _parse_set_descriptor(
    _tag: DescriptorTag,
    desc: binwrapper.BinWrapper,
    ctx: ParseContext,
) -> SetDesc:
    # .id
    tid = _parse_type_id(desc)
    # .type
    subtype = _parse_type_ref(desc, ctx=ctx)

    return SetDesc(tid=tid, subtype=subtype)


@_parse_descriptor.register(DescriptorTag.SHAPE)
def _parse_shape_descriptor(
    _tag: DescriptorTag,
    desc: binwrapper.BinWrapper,
    ctx: ParseContext,
) -> ShapeDesc:
    # .id
    tid = _parse_type_id(desc)
    # .element_count
    els = desc.read_ui16()
    # .elements
    fields = {}
    flags = {}
    cardinalities = {}
    for _ in range(els):
        if ctx.protocol_version >= (0, 11):
            # ShapeElement.flags
            flag = desc.read_ui32()
            # ShapeElement.cardinality
            cardinality = enums.Cardinality(desc.read_bytes(1)[0])
        else:
            # ShapeElement.flags
            flag = desc.read_bytes(1)[0]
            cardinality = None
        # ShapeElement.name
        name = _parse_string(desc)
        # ShapeElement.type
        subtype = _parse_type_ref(desc, ctx=ctx)

        fields[name] = subtype
        flags[name] = flag
        if cardinality:
            cardinalities[name] = cardinality

    return ShapeDesc(
        tid=tid,
        flags=flags,
        fields=fields,
        cardinalities=cardinalities,
    )


@_parse_descriptor.register(DescriptorTag.INPUT_SHAPE)
def _parse_input_shape_descriptor(
    _tag: DescriptorTag,
    desc: binwrapper.BinWrapper,
    ctx: ParseContext,
) -> InputShapeDesc:
    # .id
    tid = _parse_type_id(desc)
    # .element_count
    els = desc.read_ui16()
    # .elements
    input_fields = {}
    flags = {}
    cardinalities = {}
    fields_list = []
    for idx in range(els):
        if ctx.protocol_version >= (0, 11):
            # ShapeElement.flags
            flag = desc.read_ui32()
            # ShapeElement.cardinality
            cardinality = enums.Cardinality(desc.read_bytes(1)[0])
        else:
            # ShapeElement.flags
            flag = desc.read_bytes(1)[0]
            cardinality = None
        # ShapeElement.name
        name = _parse_string(desc)
        # ShapeElement.type
        subtype = _parse_type_ref(desc, ctx=ctx)

        fields_list.append((name, subtype))
        input_fields[name] = idx, subtype
        flags[name] = flag
        if cardinality:
            cardinalities[name] = cardinality

    return InputShapeDesc(
        fields_list=fields_list,
        tid=tid,
        flags=flags,
        fields=input_fields,
        cardinalities=cardinalities,
    )


@_parse_descriptor.register(DescriptorTag.BASE_SCALAR)
def _parse_base_scalar_descriptor(
    _tag: DescriptorTag,
    desc: binwrapper.BinWrapper,
    ctx: ParseContext,
) -> BaseScalarDesc:
    # .id
    tid = _parse_type_id(desc)

    return BaseScalarDesc(tid=tid)


@_parse_descriptor.register(DescriptorTag.SCALAR)
def _parse_scalar_descriptor(
    _tag: DescriptorTag,
    desc: binwrapper.BinWrapper,
    ctx: ParseContext,
) -> ScalarDesc:
    # .id
    tid = _parse_type_id(desc)
    # .type
    subtype = _parse_type_ref(desc, ctx=ctx)

    return ScalarDesc(tid=tid, subtype=subtype)


@_parse_descriptor.register(DescriptorTag.TUPLE)
def _parse_tuple_descriptor(
    _tag: DescriptorTag,
    desc: binwrapper.BinWrapper,
    ctx: ParseContext,
) -> TupleDesc:
    # .id
    tid = _parse_type_id(desc)
    # .element_count
    # .elements
    tuple_fields = _parse_type_refs(desc, ctx=ctx)

    return TupleDesc(tid=tid, fields=tuple_fields)


@_parse_descriptor.register(DescriptorTag.NAMEDTUPLE)
def _parse_namedtuple_descriptor(
    _tag: DescriptorTag,
    desc: binwrapper.BinWrapper,
    ctx: ParseContext,
) -> NamedTupleDesc:
    # .id
    tid = _parse_type_id(desc)
    # .element_count
    els = desc.read_ui16()
    fields = {}
    for _ in range(els):
        # TupleElement.name
        name = _parse_string(desc)
        # TupleElement.type
        fields[name] = _parse_type_ref(desc, ctx=ctx)

    return NamedTupleDesc(tid=tid, fields=fields)


@_parse_descriptor.register(DescriptorTag.ENUM)
def _parse_enum_descriptor(
    _tag: DescriptorTag,
    desc: binwrapper.BinWrapper,
    ctx: ParseContext,
) -> EnumDesc:
    # .id
    tid = _parse_type_id(desc)
    # .member_count
    # .members
    names = _parse_strings(desc)

    return EnumDesc(tid=tid, names=names)


@_parse_descriptor.register(DescriptorTag.ARRAY)
def _parse_array_descriptor(
    _tag: DescriptorTag,
    desc: binwrapper.BinWrapper,
    ctx: ParseContext,
) -> ArrayDesc:
    # .id
    tid = _parse_type_id(desc)
    # .type
    subtype = _parse_type_ref(desc, ctx=ctx)
    # .dimension_count
    els = desc.read_ui16()
    if els != 1:
        raise NotImplementedError(
            'cannot handle arrays with more than one dimension')
    # .dimensions
    dim_len = desc.read_i32()
    if dim_len != -1:
        raise NotImplementedError(
            'cannot handle arrays with non-infinite dimensions')

    return ArrayDesc(tid=tid, dim_len=dim_len, subtype=subtype)


@_parse_descriptor.register(DescriptorTag.RANGE)
def _parse_range_descriptor(
    _tag: DescriptorTag,
    desc: binwrapper.BinWrapper,
    ctx: ParseContext,
) -> RangeDesc:
    # .id
    tid = _parse_type_id(desc)
    # .type
    subtype = _parse_type_ref(desc, ctx=ctx)

    return RangeDesc(tid=tid, inner=subtype)


def _make_global_rep(typ: s_types.Type, ctx: Context) -> object:
    if isinstance(typ, s_types.Tuple):
        subtyps = typ.get_subtypes(ctx.schema)
        return (
            int(enums.TypeTag.TUPLE),
            tuple(subtyp.id for subtyp in subtyps),
            tuple(_make_global_rep(subtyp, ctx) for subtyp in subtyps),
        )
    elif isinstance(typ, s_types.Array):
        subtyp = typ.get_element_type(ctx.schema)
        return (
            int(enums.TypeTag.ARRAY), subtyp.id, _make_global_rep(subtyp, ctx))
    else:
        return None


class StateSerializerFactory:
    def __init__(self, std_schema: s_schema.Schema):
        """
        {
            module := 'default',
            aliases := [ ('alias', 'module::target'), ... ],
            config := cfg::Config {
                session_idle_transaction_timeout: <duration>'0:05:00',
                query_execution_timeout: <duration>'0:00:00',
                allow_dml_in_functions: false,
                allow_bare_ddl: AlwaysAllow,
                apply_access_policies: true,
            },
            globals := { key := value, ... },
        }

        """
        schema = std_schema
        str_type = schema.get('std::str', type=s_scalars.ScalarType)
        free_obj = schema.get('std::FreeObject', type=s_objtypes.ObjectType)
        schema, self._state_type = derive_alias(schema, free_obj, 'state_type')

        # aliases := { ('alias1', 'mod::type'), ... }
        schema, alias_tuple = s_types.Tuple.from_subtypes(
            schema, [str_type, str_type])
        schema, aliases_array = s_types.Array.from_subtypes(
            schema, [alias_tuple])

        schema, self.globals_type = derive_alias(
            schema, free_obj, 'state_globals')

        # config := cfg::Config { session_cfg1, session_cfg2, ... }
        schema, config_type = derive_alias(
            schema, free_obj, 'state_config'
        )
        config_shape: list[tuple[str, s_types.Type, enums.Cardinality]] = []
        for setting in config.get_settings().values():
            if not setting.system:
                setting_type_name = setting.schema_type_name
                setting_type = schema.get(setting_type_name, type=s_types.Type)
                config_shape.append(
                    (
                        setting.name,
                        setting_type,
                        enums.Cardinality.MANY if setting.set_of else
                        enums.Cardinality.AT_MOST_ONE,
                    )
                )

        self._input_shapes: immutables.Map[
            s_types.Type,
            tuple[InputShapeElement, ...],
        ] = immutables.Map([
            (config_type, tuple(sorted(config_shape))),
            (self._state_type, (
                ("module", str_type, enums.Cardinality.AT_MOST_ONE),
                ("aliases", aliases_array, enums.Cardinality.AT_MOST_ONE),
                ("config", config_type, enums.Cardinality.AT_MOST_ONE),
            ))
        ])
        self._schema = schema
        self._contexts: dict[tuple[int, int], Context] = {}

    def make(
        self,
        user_schema: s_schema.Schema,
        global_schema: s_schema.Schema,
        protocol_version: tuple[int, int],
    ) -> StateSerializer:
        ctx = self._contexts.get(protocol_version)
        if ctx is None:
            ctx = Context(
                schema=self._schema,
                protocol_version=protocol_version,
            )
            self._contexts[protocol_version] = ctx
            describe_input_shape(
                self._state_type,
                self._input_shapes,
                prepare_state=True,
                ctx=ctx,
            )

        ctx = ctx.derive()
        ctx.schema = s_schema.ChainedSchema(
            self._schema, user_schema, global_schema)

        globals_shape = []
        global_reps = {}
        for g in ctx.schema.get_objects(type=s_globals.Global):
            if g.is_computable(ctx.schema):
                continue
            name = str(g.get_name(ctx.schema))
            s_type = g.get_target(ctx.schema)
            if isinstance(s_type, (s_types.Array, s_types.Tuple)):
                global_reps[name] = _make_global_rep(s_type, ctx)
            cardinality = cardinality_from_ptr(g, ctx.schema)
            globals_shape.append((name, s_type, cardinality))

        type_id = describe_input_shape(
            self._state_type,
            self._input_shapes.update({
                self.globals_type: tuple(sorted(globals_shape)),
                self._state_type: self._input_shapes[self._state_type] + (
                    (
                        "globals",
                        self.globals_type,
                        enums.Cardinality.AT_MOST_ONE,
                    ),
                )
            }),
            ctx=ctx,
        )

        type_data = b''.join(ctx.buffer)
        codec = parse(type_data, protocol_version)
        assert isinstance(codec, InputShapeDesc)
        codec.fields['globals'][1].__dict__['data_raw'] = True

        return StateSerializer(type_id, type_data, codec, global_reps)


class StateSerializer:
    def __init__(
        self,
        type_id: uuid.UUID,
        type_data: bytes,
        codec: TypeDesc,
        global_reps: typing.Dict[str, object],
    ) -> None:
        self._type_id = type_id
        self._type_data = type_data
        self._codec = codec
        self._global_reps = global_reps

    @property
    def type_id(self) -> uuid.UUID:
        return self._type_id

    def describe(self) -> typing.Tuple[uuid.UUID, bytes]:
        return self._type_id, self._type_data

    def encode(self, state: Any) -> bytes:
        return self._codec.encode(state)

    def decode(self, state: bytes) -> Any:
        return self._codec.decode(state)

    def get_global_type_rep(
        self,
        global_name: str,
    ) -> Optional[object]:
        return self._global_reps.get(global_name)


def derive_alias(
    schema: s_schema.Schema,
    parent: s_objtypes.ObjectType,
    qualifier: str,
) -> tuple[s_schema.Schema, s_types.InheritingType]:
    return parent.derive_subtype(
        schema,
        name=s_obj.derive_name(
            schema,
            qualifier,
            module='__derived__',
            parent=parent,
        ),
        mark_derived=True,
        inheritance_refdicts={'pointers'},
        attrs={'expr_type': s_types.ExprType.Select},
    )


@dataclasses.dataclass(frozen=True, kw_only=True)
class TypeDesc:
    tid: uuid.UUID

    def encode(self, data: Any) -> bytes:
        raise NotImplementedError

    def decode(self, data: bytes) -> Any:
        raise NotImplementedError


@dataclasses.dataclass(frozen=True, kw_only=True)
class SequenceDesc(TypeDesc):
    subtype: TypeDesc
    impl: typing.ClassVar[Type[s_obj.CollectionFactory[Any]]]

    def encode(self, data: collections.abc.Collection[Any]) -> bytes:
        if not data:
            return b''.join((
                _uint32_packer(0),
                _uint32_packer(0),
                _uint32_packer(0),
            ))
        bufs = [
            _uint32_packer(1),
            _uint32_packer(0),
            _uint32_packer(0),
            _uint32_packer(len(data)),
            _uint32_packer(1),
        ]
        for item in data:
            if item is None:
                bufs.append(_int32_packer(-1))
            else:
                item_bytes = self.subtype.encode(item)
                bufs.append(_uint32_packer(len(item_bytes)))
                bufs.append(item_bytes)
        return b''.join(bufs)

    def decode(self, data: bytes) -> collections.abc.Collection[Any]:
        buf = io.BytesIO(data)
        wrapped = binwrapper.BinWrapper(buf)
        ndims = wrapped.read_ui32()
        if ndims == 0:
            return self.impl()
        assert ndims == 1
        wrapped.read_ui32()
        wrapped.read_ui32()
        data_len = wrapped.read_ui32()
        assert wrapped.read_ui32() == 1
        return self.impl(
            self.subtype.decode(wrapped.read_len32_prefixed_bytes())
            for _ in range(data_len)
        )


@dataclasses.dataclass(frozen=True, kw_only=True)
class SetDesc(SequenceDesc):
    impl = frozenset


@dataclasses.dataclass(frozen=True, kw_only=True)
class ShapeDesc(TypeDesc):
    fields: typing.Dict[str, TypeDesc]
    flags: typing.Dict[str, int]
    cardinalities: typing.Dict[str, enums.Cardinality]


@dataclasses.dataclass(frozen=True, kw_only=True)
class SchemaTypeDesc(TypeDesc):
    pass


@dataclasses.dataclass(frozen=True, kw_only=True)
class BaseScalarDesc(SchemaTypeDesc):
    codecs: ClassVar[Dict[
        uuid.UUID,
        tuple[Callable[[Any], bytes], Callable[[bytes], Any]]
    ]] = {
        s_obj.get_known_type_id('std::duration'): (
            statypes.Duration.encode,
            statypes.Duration.decode,
        ),
        s_obj.get_known_type_id('std::str'): (
            _encode_str,
            _decode_str,
        ),
        s_obj.get_known_type_id('std::bool'): (
            _encode_bool,
            _decode_bool,
        ),
        s_obj.get_known_type_id('std::int64'): (
            _encode_int64,
            _decode_int64,
        ),
    }

    def encode(self, data: Any) -> bytes:
        if codecs := self.codecs.get(self.tid):
            return codecs[0](data)
        raise NotImplementedError

    def decode(self, data: bytes) -> Any:
        if codecs := self.codecs.get(self.tid):
            return codecs[1](data)
        raise NotImplementedError


@dataclasses.dataclass(frozen=True, kw_only=True)
class ScalarDesc(BaseScalarDesc):
    subtype: TypeDesc


@dataclasses.dataclass(frozen=True, kw_only=True)
class NamedTupleDesc(SchemaTypeDesc):
    fields: typing.Dict[str, TypeDesc]


@dataclasses.dataclass(frozen=True, kw_only=True)
class TupleDesc(SchemaTypeDesc):
    fields: typing.List[TypeDesc]

    def encode(self, data: collections.abc.Sequence[Any]) -> bytes:
        bufs = [_uint32_packer(len(self.fields))]
        for idx, desc in enumerate(self.fields):
            bufs.append(_uint32_packer(0))
            item = desc.encode(data[idx])
            bufs.append(_uint32_packer(len(item)))
            bufs.append(item)
        return b''.join(bufs)

    def decode(self, data: bytes) -> tuple[Any, ...]:
        buf = io.BytesIO(data)
        wrapped = binwrapper.BinWrapper(buf)
        assert wrapped.read_ui32() == len(self.fields)
        rv = []
        for desc in self.fields:
            wrapped.read_ui32()
            rv.append(desc.decode(wrapped.read_len32_prefixed_bytes()))
        return tuple(rv)


@dataclasses.dataclass(frozen=True, kw_only=True)
class EnumDesc(SchemaTypeDesc):
    names: typing.List[str]

    def encode(self, data: str) -> bytes:
        return _encode_str(data)

    def decode(self, data: bytes) -> str:
        return _decode_str(data)


@dataclasses.dataclass(frozen=True, kw_only=True)
class ArrayDesc(SequenceDesc, SchemaTypeDesc):
    dim_len: int
    impl = list


@dataclasses.dataclass(frozen=True, kw_only=True)
class RangeDesc(TypeDesc):
    inner: TypeDesc


@dataclasses.dataclass(frozen=True, kw_only=True)
class InputShapeDesc(TypeDesc):
    fields: typing.Dict[str, typing.Tuple[int, TypeDesc]]
    fields_list: typing.List[typing.Tuple[str, TypeDesc]]
    flags: typing.Dict[str, int]
    cardinalities: typing.Dict[str, enums.Cardinality]
    data_raw: bool = False

    def encode(self, data: Mapping[str, Any]) -> bytes:
        bufs = [b'']
        count = 0
        for key, desc_tuple in self.fields.items():
            if key not in data:
                continue
            value = data[key]

            idx, desc = desc_tuple
            bufs.append(_uint32_packer(idx))

            if value is None:
                bufs.append(_int32_packer(-1))
            else:
                if not self.data_raw:
                    value = desc.encode(value)
                bufs.append(_uint32_packer(len(value)))
                bufs.append(value)

            count += 1
        bufs[0] = _uint32_packer(count)
        return b''.join(bufs)

    def decode(self, data: bytes) -> dict[str, Any]:
        rv = {}
        buf = io.BytesIO(data)
        wrapped = binwrapper.BinWrapper(buf)
        for _ in range(wrapped.read_ui32()):
            idx = wrapped.read_ui32()
            name, desc = self.fields_list[idx]
            item_data = wrapped.read_nullable_len32_prefixed_bytes()
            if item_data is None:
                cardinality = self.cardinalities.get(name)
                if cardinality == enums.Cardinality.ONE:
                    raise errors.CardinalityViolationError(
                        f"State '{name}' expects exactly 1 value, 0 given"
                    )
                elif cardinality == enums.Cardinality.AT_LEAST_ONE:
                    raise errors.CardinalityViolationError(
                        f"State '{name}' expects at least 1 value, 0 given"
                    )

            if self.data_raw or item_data is None:
                rv[name] = item_data
            else:
                rv[name] = desc.decode(item_data)
        return rv
