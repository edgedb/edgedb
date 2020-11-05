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

import dataclasses
import io
import struct
import typing
import uuid

from edb import errors
from edb.common import binwrapper
from edb.common import uuidgen

from edb.schema import links as s_links
from edb.schema import objects as s_obj
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types


_int32_packer = struct.Struct('!l').pack
_uint32_packer = struct.Struct('!L').pack
_uint16_packer = struct.Struct('!H').pack
_uint8_packer = struct.Struct('!B').pack


EMPTY_TUPLE_ID = s_obj.get_known_type_id('empty-tuple').bytes
EMPTY_TUPLE_DESC = b'\x04' + EMPTY_TUPLE_ID + b'\x00\x00'

UUID_TYPE_ID = s_obj.get_known_type_id('std::uuid')
STR_TYPE_ID = s_obj.get_known_type_id('std::str')

NULL_TYPE_ID = b'\x00' * 16
NULL_TYPE_DESC = b''

CTYPE_SET = b'\x00'
CTYPE_SHAPE = b'\x01'
CTYPE_BASE_SCALAR = b'\x02'
CTYPE_SCALAR = b'\x03'
CTYPE_TUPLE = b'\x04'
CTYPE_NAMEDTUPLE = b'\x05'
CTYPE_ARRAY = b'\x06'
CTYPE_ENUM = b'\x07'

CTYPE_ANNO_TYPENAME = b'\xff'


class TypeSerializer:

    EDGE_POINTER_IS_IMPLICIT = 1 << 0
    EDGE_POINTER_IS_LINKPROP = 1 << 1
    EDGE_POINTER_IS_LINK = 1 << 2

    _JSON_DESC = None

    def __init__(
        self,
        schema,
        *,
        inline_typenames: bool = False,
    ):
        self.schema = schema
        self.buffer = []
        self.anno_buffer = []
        self.uuid_to_pos = {}
        self.inline_typenames = inline_typenames

    def _get_collection_type_id(self, coll_type, subtypes,
                                element_names=None):
        if coll_type == 'tuple' and not subtypes:
            return s_obj.get_known_type_id('empty-tuple')

        subtypes = (f"{st}" for st in subtypes)
        string_id = f'{coll_type}\x00{":".join(subtypes)}'
        if element_names:
            string_id += f'\x00{":".join(element_names)}'
        return uuidgen.uuid5(s_types.TYPE_ID_NAMESPACE, string_id)

    def _get_object_type_id(self, coll_type, subtypes,
                            element_names=None, *,
                            links_props=None,
                            links=None,
                            has_implicit_fields=False):
        subtypes = (f"{st}" for st in subtypes)
        string_id = f'{coll_type}\x00{":".join(subtypes)}'
        if element_names:
            string_id += f'\x00{":".join(element_names)}'
        string_id += f'{has_implicit_fields!r};{links_props!r};{links!r}'
        return uuidgen.uuid5(s_types.TYPE_ID_NAMESPACE, string_id)

    def _get_union_type_id(self, union_type):
        base_type_id = ','.join(
            str(c.id) for c in union_type.children(self.schema))

        return uuidgen.uuid5(s_types.TYPE_ID_NAMESPACE, base_type_id)

    @classmethod
    def _get_set_type_id(cls, basetype_id):
        return uuidgen.uuid5(s_types.TYPE_ID_NAMESPACE,
                             'set-of::' + str(basetype_id))

    def _register_type_id(self, type_id):
        if type_id not in self.uuid_to_pos:
            self.uuid_to_pos[type_id] = len(self.uuid_to_pos)

    def _describe_set(self, t, view_shapes, view_shapes_metadata):
        type_id = self._describe_type(t, view_shapes, view_shapes_metadata)
        set_id = self._get_set_type_id(type_id)
        if set_id in self.uuid_to_pos:
            return set_id

        self.buffer.append(CTYPE_SET)
        self.buffer.append(set_id.bytes)
        self.buffer.append(_uint16_packer(self.uuid_to_pos[type_id]))

        self._register_type_id(set_id)
        return set_id

    def _describe_type(self, t, view_shapes, view_shapes_metadata,
                       follow_links: bool = True):
        # The encoding format is documented in edb/api/types.txt.

        buf = self.buffer

        if isinstance(t, s_types.Tuple):
            subtypes = [self._describe_type(st, view_shapes,
                                            view_shapes_metadata)
                        for st in t.get_subtypes(self.schema)]

            if t.is_named(self.schema):
                element_names = list(t.get_element_names(self.schema))
                assert len(element_names) == len(subtypes)

                type_id = self._get_collection_type_id(
                    t.schema_name, subtypes, element_names)

                if type_id in self.uuid_to_pos:
                    return type_id

                buf.append(CTYPE_NAMEDTUPLE)
                buf.append(type_id.bytes)
                buf.append(_uint16_packer(len(subtypes)))
                for el_name, el_type in zip(element_names, subtypes):
                    el_name_bytes = el_name.encode('utf-8')
                    buf.append(_uint32_packer(len(el_name_bytes)))
                    buf.append(el_name_bytes)
                    buf.append(_uint16_packer(self.uuid_to_pos[el_type]))

            else:
                type_id = self._get_collection_type_id(t.schema_name, subtypes)

                if type_id in self.uuid_to_pos:
                    return type_id

                buf.append(CTYPE_TUPLE)
                buf.append(type_id.bytes)
                buf.append(_uint16_packer(len(subtypes)))
                for el_type in subtypes:
                    buf.append(_uint16_packer(self.uuid_to_pos[el_type]))

            self._register_type_id(type_id)
            return type_id

        elif isinstance(t, s_types.Array):
            subtypes = [self._describe_type(st, view_shapes,
                                            view_shapes_metadata)
                        for st in t.get_subtypes(self.schema)]

            assert len(subtypes) == 1
            type_id = self._get_collection_type_id(t.schema_name, subtypes)

            if type_id in self.uuid_to_pos:
                return type_id

            buf.append(CTYPE_ARRAY)
            buf.append(type_id.bytes)
            buf.append(_uint16_packer(self.uuid_to_pos[subtypes[0]]))
            # Number of dimensions (currently always 1)
            buf.append(_uint16_packer(1))
            # Dimension cardinality (currently always unbound)
            buf.append(_int32_packer(-1))

            self._register_type_id(type_id)
            return type_id

        elif isinstance(t, s_types.Collection):
            raise errors.SchemaError(f'unsupported collection type {t!r}')

        elif view_shapes.get(t):
            # This is a view
            self.schema, mt = t.material_type(self.schema)
            base_type_id = mt.id

            subtypes = []
            element_names = []
            link_props = []
            links = []

            metadata = view_shapes_metadata.get(t)
            implicit_id = metadata is not None and metadata.has_implicit_id

            for ptr in view_shapes[t]:
                if ptr.singular(self.schema):
                    if isinstance(ptr, s_links.Link) and not follow_links:
                        subtype_id = self._describe_type(
                            self.schema.get('std::uuid'), view_shapes,
                            view_shapes_metadata,
                        )
                    else:
                        subtype_id = self._describe_type(
                            ptr.get_target(self.schema), view_shapes,
                            view_shapes_metadata)
                else:
                    if isinstance(ptr, s_links.Link) and not follow_links:
                        raise errors.InternalServerError(
                            'cannot describe multi links when '
                            'follow_links=False'
                        )
                    else:
                        subtype_id = self._describe_set(
                            ptr.get_target(self.schema), view_shapes,
                            view_shapes_metadata)
                subtypes.append(subtype_id)
                element_names.append(ptr.get_shortname(self.schema).name)
                link_props.append(False)
                links.append(not ptr.is_property(self.schema))

            t_rptr = t.get_rptr(self.schema)
            if t_rptr is not None and (rptr_ptrs := view_shapes.get(t_rptr)):
                # There are link properties in the mix
                for ptr in rptr_ptrs:
                    if ptr.singular(self.schema):
                        subtype_id = self._describe_type(
                            ptr.get_target(self.schema), view_shapes,
                            view_shapes_metadata)
                    else:
                        subtype_id = self._describe_set(
                            ptr.get_target(self.schema), view_shapes,
                            view_shapes_metadata)
                    subtypes.append(subtype_id)
                    element_names.append(
                        ptr.get_shortname(self.schema).name)
                    link_props.append(True)
                    links.append(False)

            type_id = self._get_object_type_id(
                base_type_id, subtypes, element_names,
                links_props=link_props, links=links,
                has_implicit_fields=implicit_id)

            if type_id in self.uuid_to_pos:
                return type_id

            buf.append(CTYPE_SHAPE)
            buf.append(type_id.bytes)

            assert len(subtypes) == len(element_names)
            buf.append(_uint16_packer(len(subtypes)))

            for el_name, el_type, el_lp, el_l in zip(element_names,
                                                     subtypes, link_props,
                                                     links):
                flags = 0
                if el_lp:
                    flags |= self.EDGE_POINTER_IS_LINKPROP
                if (implicit_id and el_name == 'id') or el_name == '__tid__':
                    if el_type != UUID_TYPE_ID:
                        raise errors.InternalServerError(
                            f"{el_name!r} is expected to be a 'std::uuid' "
                            f"singleton")
                    flags |= self.EDGE_POINTER_IS_IMPLICIT
                elif el_name == '__tname__':
                    if el_type != STR_TYPE_ID:
                        raise errors.InternalServerError(
                            f"{el_name!r} is expected to be a 'std::str' "
                            f"singleton")
                    flags |= self.EDGE_POINTER_IS_IMPLICIT
                if el_l:
                    flags |= self.EDGE_POINTER_IS_LINK
                buf.append(_uint8_packer(flags))

                el_name_bytes = el_name.encode('utf-8')
                buf.append(_uint32_packer(len(el_name_bytes)))
                buf.append(el_name_bytes)
                buf.append(_uint16_packer(self.uuid_to_pos[el_type]))

            self._register_type_id(type_id)
            return type_id

        elif isinstance(t, s_scalars.ScalarType):
            # This is a scalar type

            self.schema, mt = t.material_type(self.schema)
            type_id = mt.id
            if type_id in self.uuid_to_pos:
                # already described
                return type_id

            base_type = mt.get_topmost_concrete_base(self.schema)
            enum_values = mt.get_enum_values(self.schema)

            if enum_values:
                buf.append(CTYPE_ENUM)
                buf.append(type_id.bytes)
                buf.append(_uint16_packer(len(enum_values)))
                for enum_val in enum_values:
                    enum_val_bytes = enum_val.encode('utf-8')
                    buf.append(_uint32_packer(len(enum_val_bytes)))
                    buf.append(enum_val_bytes)

                if self.inline_typenames:
                    self._add_annotation(mt)

            elif mt == base_type:
                buf.append(CTYPE_BASE_SCALAR)
                buf.append(type_id.bytes)

            else:
                bt_id = self._describe_type(
                    base_type, view_shapes, view_shapes_metadata)

                buf.append(CTYPE_SCALAR)
                buf.append(type_id.bytes)
                buf.append(_uint16_packer(self.uuid_to_pos[bt_id]))

                if self.inline_typenames:
                    self._add_annotation(mt)

            self._register_type_id(type_id)
            return type_id

        else:
            raise errors.InternalServerError(
                f'cannot describe type {t.get_name(self.schema)}')

    def _add_annotation(self, t: s_types.Type):
        self.anno_buffer.append(CTYPE_ANNO_TYPENAME)

        self.anno_buffer.append(t.id.bytes)

        tn = t.get_displayname(self.schema)

        tn_bytes = tn.encode('utf-8')
        self.anno_buffer.append(_uint32_packer(len(tn_bytes)))
        self.anno_buffer.append(tn_bytes)

    @classmethod
    def describe(
        cls, schema, typ, view_shapes, view_shapes_metadata,
        *,
        follow_links: bool = True,
        inline_typenames: bool = False,
    ) -> bytes:
        builder = cls(
            schema,
            inline_typenames=inline_typenames,
        )
        type_id = builder._describe_type(
            typ, view_shapes, view_shapes_metadata,
            follow_links=follow_links)
        out = b''.join(builder.buffer) + b''.join(builder.anno_buffer)
        return out, type_id

    @classmethod
    def describe_json(cls) -> bytes:
        if cls._JSON_DESC is not None:
            return cls._JSON_DESC

        cls._JSON_DESC = cls._describe_json()
        return cls._JSON_DESC

    @classmethod
    def _describe_json(cls) -> bytes:
        json_id = s_obj.get_known_type_id('std::str')

        buf = []
        buf.append(b'\x02')
        buf.append(json_id.bytes)

        return b''.join(buf), json_id

    @classmethod
    def _parse(
        cls,
        desc: binwrapper.BinWrapper,
        codecs_list: typing.List[TypeDesc]
    ) -> typing.Optional[TypeDesc]:
        t = desc.read_bytes(1)
        tid = uuidgen.from_bytes(desc.read_bytes(16))

        if t == CTYPE_SET:
            pos = desc.read_ui16()
            return SetDesc(tid=tid, subtype=codecs_list[pos])

        elif t == CTYPE_SHAPE:
            els = desc.read_ui16()
            fields = {}
            flags = {}
            for _ in range(els):
                flag = desc.read_bytes(1)[0]
                name = desc.read_len32_prefixed_bytes().decode()
                pos = desc.read_ui16()
                fields[name] = codecs_list[pos]
                flags[name] = flag
            return ShapeDesc(tid=tid, flags=flags, fields=fields)

        elif t == CTYPE_BASE_SCALAR:
            return BaseScalarDesc(tid=tid)

        elif t == CTYPE_SCALAR:
            pos = desc.read_ui16()
            return ScalarDesc(tid=tid, subtype=codecs_list[pos])

        elif t == CTYPE_TUPLE:
            els = desc.read_ui16()
            fields = []
            for _ in range(els):
                pos = desc.read_ui16()
                fields.append(codecs_list[pos])
            return TupleDesc(tid=tid, fields=fields)

        elif t == CTYPE_NAMEDTUPLE:
            els = desc.read_ui16()
            fields = {}
            for _ in range(els):
                name = desc.read_len32_prefixed_bytes().decode()
                pos = desc.read_ui16()
                fields[name] = codecs_list[pos]
            return NamedTupleDesc(tid=tid, fields=fields)

        elif t == CTYPE_ENUM:
            els = desc.read_ui16()
            names = []
            for _ in range(els):
                name = desc.read_len32_prefixed_bytes().decode()
                names.append(name)
            return EnumDesc(tid=tid, names=names)

        elif t == CTYPE_ARRAY:
            pos = desc.read_ui16()
            els = desc.read_ui16()
            if els != 1:
                raise NotImplementedError(
                    'cannot handle arrays with more than one dimension')
            dim_len = desc.read_i32()
            return ArrayDesc(
                tid=tid, dim_len=dim_len, subtype=codecs_list[pos])

        elif (t >= 0x7f and t <= 0xff):
            # Ignore all type annotations.
            desc.read_len32_prefixed_bytes()

        else:
            raise NotImplementedError(
                f'no codec implementation for EdgeDB data class {t}')

    @classmethod
    def parse(cls, typedesc: bytes) -> TypeDesc:
        buf = io.BytesIO(typedesc)
        wrapped = binwrapper.BinWrapper(buf)
        codecs_list = []
        while buf.tell() < len(typedesc):
            desc = cls._parse(wrapped, codecs_list)
            if desc is not None:
                codecs_list.append(desc)
        return desc


@dataclasses.dataclass(frozen=True)
class TypeDesc:
    tid: uuid.UUID


@dataclasses.dataclass(frozen=True)
class SetDesc(TypeDesc):
    subtype: TypeDesc


@dataclasses.dataclass(frozen=True)
class ShapeDesc(TypeDesc):
    fields: typing.Dict[TypeDesc]
    flags: typing.Dict[int]


@dataclasses.dataclass(frozen=True)
class ScalarDesc(TypeDesc):
    subtype: TypeDesc


@dataclasses.dataclass(frozen=True)
class BaseScalarDesc(TypeDesc):
    pass


@dataclasses.dataclass(frozen=True)
class NamedTupleDesc(TypeDesc):
    fields: typing.Dict[str, TypeDesc]


@dataclasses.dataclass(frozen=True)
class TupleDesc(TypeDesc):
    fields: typing.List[TypeDesc]


@dataclasses.dataclass(frozen=True)
class EnumDesc(TypeDesc):
    names: typing.List[str]


@dataclasses.dataclass(frozen=True)
class ArrayDesc(TypeDesc):
    dim_len: int
    subtype: TypeDesc
