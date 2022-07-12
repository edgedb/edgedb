# mypy: ignore-errors

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

import immutables

from edb import errors
from edb.common import binwrapper
from edb.common import uuidgen

from edb.protocol import enums as p_enums
from edb.server import config

from edb.edgeql import qltypes
from edb.schema import globals as s_globals
from edb.schema import links as s_links
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import scalars as s_scalars
from edb.schema import schema as s_schema
from edb.schema import types as s_types
from edb.ir import statypes

from . import enums


_int32_packer = struct.Struct('!l').pack
_uint32_packer = struct.Struct('!L').pack
_uint16_packer = struct.Struct('!H').pack
_uint8_packer = struct.Struct('!B').pack
_int64_struct = struct.Struct('!q')


EMPTY_TUPLE_ID = s_obj.get_known_type_id('empty-tuple')
EMPTY_TUPLE_DESC = b'\x04' + EMPTY_TUPLE_ID.bytes + b'\x00\x00'

UUID_TYPE_ID = s_obj.get_known_type_id('std::uuid')
STR_TYPE_ID = s_obj.get_known_type_id('std::str')

NULL_TYPE_ID = uuidgen.UUID(b'\x00' * 16)
NULL_TYPE_DESC = b''

CTYPE_SET = b'\x00'
CTYPE_SHAPE = b'\x01'
CTYPE_BASE_SCALAR = b'\x02'
CTYPE_SCALAR = b'\x03'
CTYPE_TUPLE = b'\x04'
CTYPE_NAMEDTUPLE = b'\x05'
CTYPE_ARRAY = b'\x06'
CTYPE_ENUM = b'\x07'
CTYPE_INPUT_SHAPE = b'\x08'
CTYPE_RANGE = b'\x09'
CTYPE_ANNO_TYPENAME = b'\xff'

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
    return _int64_struct.unpack(data)[0]


def cardinality_from_ptr(ptr, schema) -> enums.Cardinality:
    required = ptr.get_required(schema)
    is_multi = ptr.get_cardinality(schema).is_multi()

    if not required and not is_multi:
        return enums.Cardinality.AT_MOST_ONE
    if required and not is_multi:
        return enums.Cardinality.ONE
    if not required and is_multi:
        return enums.Cardinality.MANY
    if required and is_multi:
        return enums.Cardinality.AT_LEAST_ONE

    raise RuntimeError("unreachable")


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

    @classmethod
    def _get_set_type_id(cls, basetype_id):
        return uuidgen.uuid5(s_types.TYPE_ID_NAMESPACE,
                             'set-of::' + str(basetype_id))

    def _register_type_id(self, type_id):
        if type_id not in self.uuid_to_pos:
            self.uuid_to_pos[type_id] = len(self.uuid_to_pos)

    def _describe_set(self, t, view_shapes, view_shapes_metadata,
                      protocol_version):
        type_id = self._describe_type(t, view_shapes, view_shapes_metadata,
                                      protocol_version)
        set_id = self._get_set_type_id(type_id)
        if set_id in self.uuid_to_pos:
            return set_id

        self.buffer.append(CTYPE_SET)
        self.buffer.append(set_id.bytes)
        self.buffer.append(_uint16_packer(self.uuid_to_pos[type_id]))

        self._register_type_id(set_id)
        return set_id

    def _describe_type(self, t, view_shapes, view_shapes_metadata,
                       protocol_version,
                       follow_links: bool = True,
                       name_filter: str = ""):
        # The encoding format is documented in edb/api/types.txt.

        buf = self.buffer

        if isinstance(t, s_types.Tuple):
            subtypes = [self._describe_type(st, view_shapes,
                                            view_shapes_metadata,
                                            protocol_version)
                        for st in t.get_subtypes(self.schema)]

            if t.is_named(self.schema):
                element_names = list(t.get_element_names(self.schema))
                assert len(element_names) == len(subtypes)

                type_id = self._get_collection_type_id(
                    t.get_schema_name(), subtypes, element_names)

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
                type_id = self._get_collection_type_id(
                    t.get_schema_name(), subtypes)

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
                                            view_shapes_metadata,
                                            protocol_version)
                        for st in t.get_subtypes(self.schema)]

            assert len(subtypes) == 1
            type_id = self._get_collection_type_id(
                t.get_schema_name(), subtypes)

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

        elif isinstance(t, s_types.Range):
            subtypes = [self._describe_type(st, view_shapes,
                                            view_shapes_metadata,
                                            protocol_version)
                        for st in t.get_subtypes(self.schema)]

            assert len(subtypes) == 1
            type_id = self._get_collection_type_id(
                t.get_schema_name(), subtypes)

            if type_id in self.uuid_to_pos:
                return type_id

            buf.append(CTYPE_RANGE)
            buf.append(type_id.bytes)
            buf.append(_uint16_packer(self.uuid_to_pos[subtypes[0]]))

            self._register_type_id(type_id)
            return type_id

        elif isinstance(t, s_types.Collection):
            raise errors.SchemaError(f'unsupported collection type {t!r}')

        elif isinstance(t, s_objtypes.ObjectType):
            # This is a view
            self.schema, mt = t.material_type(self.schema)
            base_type_id = mt.id

            subtypes = []
            element_names = []
            link_props = []
            links = []
            cardinalities = []

            metadata = view_shapes_metadata.get(t)
            implicit_id = metadata is not None and metadata.has_implicit_id

            for ptr in view_shapes.get(t, ()):
                name = ptr.get_shortname(self.schema).name
                if not name.startswith(name_filter):
                    continue
                name = name.removeprefix(name_filter)
                if ptr.singular(self.schema):
                    if isinstance(ptr, s_links.Link) and not follow_links:
                        subtype_id = self._describe_type(
                            self.schema.get('std::uuid'), view_shapes,
                            view_shapes_metadata, protocol_version,
                        )
                    else:
                        subtype_id = self._describe_type(
                            ptr.get_target(self.schema), view_shapes,
                            view_shapes_metadata, protocol_version)
                else:
                    if isinstance(ptr, s_links.Link) and not follow_links:
                        raise errors.InternalServerError(
                            'cannot describe multi links when '
                            'follow_links=False'
                        )
                    else:
                        subtype_id = self._describe_set(
                            ptr.get_target(self.schema), view_shapes,
                            view_shapes_metadata, protocol_version)
                subtypes.append(subtype_id)
                element_names.append(name)
                link_props.append(False)
                links.append(not ptr.is_property(self.schema))
                cardinalities.append(
                    cardinality_from_ptr(ptr, self.schema).value)

            t_rptr = t.get_rptr(self.schema)
            if t_rptr is not None and (rptr_ptrs := view_shapes.get(t_rptr)):
                # There are link properties in the mix
                for ptr in rptr_ptrs:
                    if ptr.singular(self.schema):
                        subtype_id = self._describe_type(
                            ptr.get_target(self.schema), view_shapes,
                            view_shapes_metadata, protocol_version)
                    else:
                        subtype_id = self._describe_set(
                            ptr.get_target(self.schema), view_shapes,
                            view_shapes_metadata, protocol_version)
                    subtypes.append(subtype_id)
                    element_names.append(
                        ptr.get_shortname(self.schema).name)
                    link_props.append(True)
                    links.append(False)
                    cardinalities.append(
                        cardinality_from_ptr(ptr, self.schema).value)

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

            zipped_parts = list(zip(element_names, subtypes, link_props, links,
                                    cardinalities))
            for el_name, el_type, el_lp, el_l, el_c in zipped_parts:
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

                if protocol_version >= (0, 11):
                    buf.append(_uint32_packer(flags))
                    buf.append(_uint8_packer(el_c))
                else:
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
                    base_type, view_shapes, view_shapes_metadata,
                    protocol_version)

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

    def describe_input_shape(
        self, t, input_shapes, protocol_version,
        prepare_state: bool = False,
    ):
        if t in input_shapes:
            element_names = []
            subtypes = []
            cardinalities = []
            for name, subtype, cardinality in input_shapes[t]:
                if (
                    cardinality == enums.Cardinality.MANY or
                    cardinality == enums.Cardinality.AT_LEAST_ONE
                ):
                    subtype_id = self._describe_set(
                        subtype, {}, {}, protocol_version
                    )
                else:
                    subtype_id = self.describe_input_shape(
                        subtype, input_shapes, protocol_version
                    )
                element_names.append(name)
                subtypes.append(subtype_id)
                cardinalities.append(cardinality.value)

            if prepare_state:
                return

            self.schema, mt = t.material_type(self.schema)
            base_type_id = mt.id

            type_id = self._get_object_type_id(
                base_type_id, subtypes, element_names
            )

            if type_id in self.uuid_to_pos:
                return type_id

            buf = self.buffer
            buf.append(CTYPE_INPUT_SHAPE)
            buf.append(type_id.bytes)

            assert len(subtypes) == len(element_names)
            buf.append(_uint16_packer(len(subtypes)))

            zipped_parts = zip(element_names, subtypes, cardinalities)
            for el_name, el_type, el_c in zipped_parts:
                buf.append(_uint32_packer(0))  # flags
                buf.append(_uint8_packer(el_c))
                el_name_bytes = el_name.encode('utf-8')
                buf.append(_uint32_packer(len(el_name_bytes)))
                buf.append(el_name_bytes)
                buf.append(_uint16_packer(self.uuid_to_pos[el_type]))

            self._register_type_id(type_id)
            return type_id
        else:
            return self._describe_type(t, {}, {}, protocol_version)

    def _add_annotation(self, t: s_types.Type):
        self.anno_buffer.append(CTYPE_ANNO_TYPENAME)

        self.anno_buffer.append(t.id.bytes)

        tn = t.get_displayname(self.schema)

        tn_bytes = tn.encode('utf-8')
        self.anno_buffer.append(_uint32_packer(len(tn_bytes)))
        self.anno_buffer.append(tn_bytes)

    @classmethod
    def describe_params(
        cls,
        *,
        schema: s_schema.Schema,
        params: list[tuple[str, s_obj.Object, bool]],
        protocol_version: tuple[int, int],
    ) -> tuple[bytes, uuidgen.UUID]:
        assert protocol_version >= (0, 12)

        if not params:
            return NULL_TYPE_DESC, NULL_TYPE_ID

        builder = cls(schema)
        params_buf: list[bytes] = []

        for param_name, param_type, param_req in params:
            param_type_id = builder._describe_type(
                param_type,
                {},
                {},
                protocol_version,
            )

            params_buf.append(_uint32_packer(0))  # flags
            params_buf.append(_uint8_packer(
                p_enums.Cardinality.ONE.value if param_req else
                p_enums.Cardinality.AT_MOST_ONE.value
            ))

            param_name_bytes = param_name.encode('utf-8')
            params_buf.append(_uint32_packer(len(param_name_bytes)))
            params_buf.append(param_name_bytes)
            params_buf.append(_uint16_packer(
                builder.uuid_to_pos[param_type_id]
            ))

        buffer_encoded = b''.join(builder.buffer)

        full_params = EMPTY_BYTEARRAY.join([
            buffer_encoded,

            CTYPE_SHAPE,
            NULL_TYPE_ID.bytes,  # will be replaced with `params_id` later
            _uint16_packer(len(params)),
            *params_buf,

            *builder.anno_buffer,
        ])

        params_id = uuidgen.uuid5_bytes(
            s_types.TYPE_ID_NAMESPACE,
            full_params
        )

        id_pos = len(buffer_encoded) + 1
        full_params[id_pos : id_pos + 16] = params_id.bytes

        return bytes(full_params), params_id

    @classmethod
    def describe(
        cls, schema, typ, view_shapes, view_shapes_metadata,
        *,
        protocol_version,
        follow_links: bool = True,
        inline_typenames: bool = False,
        name_filter: str = "",
    ) -> typing.Tuple[bytes, uuidgen.UUID]:
        builder = cls(
            schema,
            inline_typenames=inline_typenames,
        )
        type_id = builder._describe_type(
            typ, view_shapes, view_shapes_metadata,
            protocol_version, follow_links=follow_links,
            name_filter=name_filter)
        out = b''.join(builder.buffer) + b''.join(builder.anno_buffer)
        return out, type_id

    @classmethod
    def describe_json(cls) -> bytes:
        if cls._JSON_DESC is not None:
            return cls._JSON_DESC

        cls._JSON_DESC = cls._describe_json()
        return cls._JSON_DESC

    @classmethod
    def _describe_json(cls) -> typing.Tuple[bytes, uuidgen.UUID]:
        json_id = s_obj.get_known_type_id('std::str')

        buf = []
        buf.append(b'\x02')
        buf.append(json_id.bytes)

        return b''.join(buf), json_id

    @classmethod
    def _parse(
        cls,
        desc: binwrapper.BinWrapper,
        codecs_list: typing.List[TypeDesc],
        protocol_version: tuple,
    ) -> typing.Optional[TypeDesc]:
        t = desc.read_bytes(1)
        tid = uuidgen.from_bytes(desc.read_bytes(16))

        if t == CTYPE_SET:
            pos = desc.read_ui16()
            return SetDesc(tid=tid, subtype=codecs_list[pos])

        elif t == CTYPE_SHAPE or t == CTYPE_INPUT_SHAPE:
            els = desc.read_ui16()
            fields = {}
            flags = {}
            cardinalities = {}
            fields_list = []
            for idx in range(els):
                if protocol_version >= (0, 11):
                    flag = desc.read_ui32()
                    cardinality = enums.Cardinality(desc.read_bytes(1)[0])
                else:
                    flag = desc.read_bytes(1)[0]
                    cardinality = None
                name = desc.read_len32_prefixed_bytes().decode()
                pos = desc.read_ui16()
                codec = codecs_list[pos]
                if t == CTYPE_INPUT_SHAPE:
                    fields_list.append((name, codec))
                    fields[name] = idx, codec
                else:
                    fields[name] = codec
                flags[name] = flag
                if cardinality:
                    cardinalities[name] = cardinality
            args = dict(
                tid=tid,
                flags=flags,
                fields=fields,
                cardinalities=cardinalities,
            )
            if t == CTYPE_SHAPE:
                return ShapeDesc(**args)
            else:
                return InputShapeDesc(fields_list=fields_list, **args)

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

        elif t == CTYPE_RANGE:
            pos = desc.read_ui16()
            return RangeDesc(tid=tid, inner=codecs_list[pos])

        elif (t[0] >= 0x80 and t[0] <= 0xff):
            # Ignore all type annotations.
            desc.read_len32_prefixed_bytes()
            return None

        else:
            raise NotImplementedError(
                f'no codec implementation for EdgeDB data class {t}')

    @classmethod
    def parse(cls, typedesc: bytes, protocol_version: tuple) -> TypeDesc:
        buf = io.BytesIO(typedesc)
        wrapped = binwrapper.BinWrapper(buf)
        codecs_list = []
        while buf.tell() < len(typedesc):
            desc = cls._parse(wrapped, codecs_list, protocol_version)
            if desc is not None:
                codecs_list.append(desc)
        if not codecs_list:
            raise errors.InternalServerError('could not parse type descriptor')
        return codecs_list[-1]

    def derive(self, schema) -> TypeSerializer:
        rv = type(self)(schema, inline_typenames=self.inline_typenames)
        rv.buffer = self.buffer.copy()
        rv.anno_buffer = self.anno_buffer.copy()
        rv.uuid_to_pos = self.uuid_to_pos.copy()
        return rv


class StateSerializerFactory:
    def __init__(self, std_schema: s_schema.FlatSchema):
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
        str_type = schema.get('std::str')
        schema, self._state_type = simple_derive_type(
            schema, 'std::FreeObject', 'state_type'
        )

        # aliases := { ('alias1', 'mod::type'), ... }
        schema, alias_tuple = s_types.Tuple.from_subtypes(
            schema, [str_type, str_type])
        schema, aliases_array = s_types.Array.from_subtypes(
            schema, [alias_tuple])

        # config := cfg::Config { session_cfg1, session_cfg2, ... }
        schema, config_type = simple_derive_type(
            schema, 'cfg::Config', 'state_config'
        )
        config_shape = []
        for setting in config.get_settings().values():
            if not setting.system:
                config_shape.append(
                    (
                        setting.name,
                        setting.s_type,
                        enums.Cardinality.MANY if setting.set_of else
                        enums.Cardinality.AT_MOST_ONE,
                    )
                )

        self._input_shapes = immutables.Map({
            config_type: tuple(sorted(config_shape)),
            self._state_type: (
                ("module", str_type, enums.Cardinality.AT_MOST_ONE),
                ("aliases", aliases_array, enums.Cardinality.AT_MOST_ONE),
                ("config", config_type, enums.Cardinality.AT_MOST_ONE),
            )
        })
        self._schema = schema
        self._builders = {}

    def make(
        self, user_schema, global_schema, protocol_version
    ) -> StateSerializer:
        if protocol_version in self._builders:
            builder = self._builders[protocol_version]
        else:
            builder = self._builders[protocol_version] = TypeSerializer(
                self._schema
            )
            builder.describe_input_shape(
                self._state_type,
                self._input_shapes,
                protocol_version,
                prepare_state=True,
            )
        schema = builder.schema
        schema, globals_type = simple_derive_type(
            schema, 'std::FreeObject', 'state_globals'
        )
        schema = s_schema.ChainedSchema(schema, user_schema, global_schema)
        globals_shape = []
        array_type_ids = {}
        for g in schema.get_objects(type=s_globals.Global):
            if g.is_computable(schema):
                continue
            name = str(g.get_name(schema))
            s_type = g.get_target(schema)
            if s_type.is_array():
                array_type_ids[name] = s_type.get_element_type(schema).id
            globals_shape.append(
                (
                    name,
                    s_type,
                    enums.Cardinality.AT_MOST_ONE if
                    g.get_cardinality(schema) == qltypes.SchemaCardinality.One
                    else enums.Cardinality.MANY,
                )
            )

        builder = builder.derive(schema)
        type_id = builder.describe_input_shape(
            self._state_type,
            self._input_shapes.update({
                globals_type: tuple(sorted(globals_shape)),
                self._state_type: self._input_shapes[self._state_type] + (
                    ("globals", globals_type, enums.Cardinality.AT_MOST_ONE),
                )
            }),
            protocol_version,
        )
        type_data = b''.join(builder.buffer)
        codec = TypeSerializer.parse(type_data, protocol_version)
        codec.fields['globals'][1].__dict__['data_raw'] = True

        return StateSerializer(type_id, type_data, codec, array_type_ids)


class StateSerializer:
    def __init__(
        self,
        type_id: uuidgen.UUID,
        type_data: bytes,
        codec: TypeDesc,
        globals_array_type_ids: typing.Dict[str, uuidgen.UUID],
    ):
        self._type_id = type_id
        self._type_data = type_data
        self._codec = codec
        self._globals_array_type_ids = globals_array_type_ids

    @property
    def type_id(self):
        return self._type_id

    def describe(self) -> typing.Tuple[uuidgen.UUID, bytes]:
        return self._type_id, self._type_data

    def encode(self, state) -> bytes:
        return self._codec.encode(state)

    def decode(self, state: bytes):
        return self._codec.decode(state)

    def get_global_array_type_id(self, global_name):
        return self._globals_array_type_ids.get(global_name)


def simple_derive_type(schema, parent, qualifier):
    s_type = schema.get(parent)
    return s_type.derive_subtype(
        schema,
        name=s_obj.derive_name(
            schema,
            qualifier,
            module='__derived__',
            parent=s_type,
        ),
        mark_derived=True,
        transient=True,
        inheritance_refdicts={'pointers'},
    )


@dataclasses.dataclass(frozen=True)
class TypeDesc:
    tid: uuidgen.UUID

    def encode(self, data) -> bytes:
        raise NotImplementedError

    def decode(self, data: bytes):
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class SetDesc(TypeDesc):
    subtype: TypeDesc
    impl: typing.ClassVar[type] = frozenset

    def encode(self, data) -> bytes:
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

    def decode(self, data: bytes):
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


@dataclasses.dataclass(frozen=True)
class ShapeDesc(TypeDesc):
    fields: typing.Dict[str, TypeDesc]
    flags: typing.Dict[str, int]
    cardinalities: typing.Dict[str, enums.Cardinality]


@dataclasses.dataclass(frozen=True)
class ScalarDesc(TypeDesc):
    subtype: TypeDesc


@dataclasses.dataclass(frozen=True)
class BaseScalarDesc(TypeDesc):
    codecs = {
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

    def encode(self, data) -> bytes:
        if codecs := self.codecs.get(self.tid):
            return codecs[0](data)
        raise NotImplementedError

    def decode(self, data: bytes):
        if codecs := self.codecs.get(self.tid):
            return codecs[1](data)
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class NamedTupleDesc(TypeDesc):
    fields: typing.Dict[str, TypeDesc]


@dataclasses.dataclass(frozen=True)
class TupleDesc(TypeDesc):
    fields: typing.List[TypeDesc]

    def encode(self, data) -> bytes:
        bufs = [_uint32_packer(len(self.fields))]
        for idx, desc in enumerate(self.fields):
            bufs.append(_uint32_packer(0))
            item = desc.encode(data[idx])
            bufs.append(_uint32_packer(len(item)))
            bufs.append(item)
        return b''.join(bufs)

    def decode(self, data: bytes):
        buf = io.BytesIO(data)
        wrapped = binwrapper.BinWrapper(buf)
        assert wrapped.read_ui32() == len(self.fields)
        rv = []
        for desc in self.fields:
            wrapped.read_ui32()
            rv.append(desc.decode(wrapped.read_len32_prefixed_bytes()))
        return tuple(rv)


@dataclasses.dataclass(frozen=True)
class EnumDesc(TypeDesc):
    names: typing.List[str]

    def encode(self, data) -> bytes:
        return _encode_str(data)

    def decode(self, data: bytes):
        return _decode_str(data)


@dataclasses.dataclass(frozen=True)
class ArrayDesc(SetDesc):
    dim_len: int
    impl: typing.ClassVar[type] = list


@dataclasses.dataclass(frozen=True)
class RangeDesc(TypeDesc):
    inner: TypeDesc


@dataclasses.dataclass(frozen=True)
class InputShapeDesc(ShapeDesc):
    fields: typing.Dict[str, typing.Tuple[int, TypeDesc]]
    fields_list: typing.List[typing.Tuple[str, TypeDesc]]
    data_raw: bool = False

    def encode(self, data) -> bytes:
        bufs = [b'']
        count = 0
        for key, desc_tuple in self.fields.items():
            if key not in data:
                continue
            value = data[key]

            desc_tuple = self.fields.get(key)
            if not desc_tuple:
                raise NotImplementedError
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

    def decode(self, data: bytes):
        rv = {}
        buf = io.BytesIO(data)
        wrapped = binwrapper.BinWrapper(buf)
        for _ in range(wrapped.read_ui32()):
            idx = wrapped.read_ui32()
            name, desc = self.fields_list[idx]
            data = wrapped.read_nullable_len32_prefixed_bytes()
            if self.data_raw or data is None:
                rv[name] = data
            else:
                rv[name] = desc.decode(data)
        return rv
