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


import struct
import uuid

from edb import errors

from edb.server.pgsql import types as pg_types

from edb.lang.schema import objects as s_obj
from edb.lang.schema import types as s_types


_uint16_packer = struct.Struct('!H').pack
_uint8_packer = struct.Struct('!B').pack


class TypeSerializer:

    EDGE_POINTER_IS_IMPLICIT = 1 << 0
    EDGE_POINTER_IS_LINKPROP = 1 << 1

    _JSON_DESC = None

    def __init__(self, schema):
        self.schema = schema
        self.buffer = []
        self.uuid_to_pos = {}

    def _get_collection_type_id(self, coll_type, subtypes, element_names=None):
        if coll_type == 'tuple' and not subtypes:
            return s_obj.get_known_type_id('empty-tuple')

        subtypes = (f"{st}" for st in subtypes)
        string_id = f'{coll_type}\x00{":".join(subtypes)}'
        if element_names:
            string_id += f'\x00{":".join(element_names)}'
        return uuid.uuid5(pg_types.TYPE_ID_NAMESPACE, string_id)

    def _get_union_type_id(self, union_type):
        base_type_id = ','.join(
            str(c.id) for c in union_type.children(self.schema))

        return uuid.uuid5(pg_types.TYPE_ID_NAMESPACE, base_type_id)

    @classmethod
    def _get_set_type_id(cls, basetype_id):
        return uuid.uuid5(pg_types.TYPE_ID_NAMESPACE,
                          'set-of::' + str(basetype_id))

    def _register_type_id(self, type_id):
        if type_id not in self.uuid_to_pos:
            self.uuid_to_pos[type_id] = len(self.uuid_to_pos)

    def _describe_set(self, t, view_shapes):
        type_id = self._describe_type(t, view_shapes)
        set_id = self._get_set_type_id(type_id)
        if set_id in self.uuid_to_pos:
            return set_id

        self.buffer.append(b'\x00')
        self.buffer.append(set_id.bytes)
        self.buffer.append(_uint16_packer(self.uuid_to_pos[type_id]))

        self._register_type_id(set_id)
        return set_id

    def _describe_type(self, t, view_shapes):
        # The encoding format is documented in edb/api/types.txt.

        buf = self.buffer

        if isinstance(t, s_types.Tuple):
            subtypes = [self._describe_type(st, view_shapes)
                        for st in t.get_subtypes()]

            if t.named:
                element_names = list(t.element_types)
                assert len(element_names) == len(subtypes)

                type_id = self._get_collection_type_id(
                    t.schema_name, subtypes, element_names)

                if type_id in self.uuid_to_pos:
                    return type_id

                buf.append(b'\x05')
                buf.append(type_id.bytes)
                buf.append(_uint16_packer(len(subtypes)))
                for el_name, el_type in zip(element_names, subtypes):
                    el_name_bytes = el_name.encode('utf-8')
                    buf.append(_uint16_packer(len(el_name_bytes)))
                    buf.append(el_name_bytes)
                    buf.append(_uint16_packer(self.uuid_to_pos[el_type]))

            else:
                type_id = self._get_collection_type_id(t.schema_name, subtypes)

                if type_id in self.uuid_to_pos:
                    return type_id

                buf.append(b'\x04')
                buf.append(type_id.bytes)
                buf.append(_uint16_packer(len(subtypes)))
                for el_type in subtypes:
                    buf.append(_uint16_packer(self.uuid_to_pos[el_type]))

            self._register_type_id(type_id)
            return type_id

        elif isinstance(t, s_types.Array):
            subtypes = [self._describe_type(st, view_shapes)
                        for st in t.get_subtypes()]

            assert len(subtypes) == 1
            type_id = self._get_collection_type_id(t.schema_name, subtypes)

            if type_id in self.uuid_to_pos:
                return type_id

            buf.append(b'\x06')
            buf.append(type_id.bytes)
            buf.append(_uint16_packer(self.uuid_to_pos[subtypes[0]]))

            self._register_type_id(type_id)
            return type_id

        elif isinstance(t, s_types.Collection):
            raise errors.SchemaError(f'unsupported collection type {t!r}')

        elif view_shapes.get(t):
            # This is a view
            mt = t.material_type(self.schema)
            if mt.get_is_virtual(self.schema):
                base_type_id = self._get_union_type_id(mt)
            else:
                base_type_id = mt.id

            subtypes = []
            element_names = []
            link_props = []

            for ptr in view_shapes[t]:
                if ptr.singular(self.schema):
                    subtype_id = self._describe_type(
                        ptr.get_target(self.schema), view_shapes)
                else:
                    subtype_id = self._describe_set(
                        ptr.get_target(self.schema), view_shapes)
                subtypes.append(subtype_id)
                element_names.append(ptr.get_shortname(self.schema).name)
                link_props.append(False)

            t_rptr = t.get_rptr(self.schema)
            if t_rptr is not None:
                # There are link properties in the mix
                for ptr in view_shapes[t_rptr]:
                    if ptr.singular(self.schema):
                        subtype_id = self._describe_type(
                            ptr.get_target(self.schema), view_shapes)
                    else:
                        subtype_id = self._describe_set(
                            ptr.get_target(self.schema), view_shapes)
                    subtypes.append(subtype_id)
                    element_names.append(
                        ptr.get_shortname(self.schema).name)
                    link_props.append(True)

            type_id = self._get_collection_type_id(
                base_type_id, subtypes, element_names)

            if type_id in self.uuid_to_pos:
                return type_id

            buf.append(b'\x01')
            buf.append(type_id.bytes)

            assert len(subtypes) == len(element_names)
            buf.append(_uint16_packer(len(subtypes)))

            for el_name, el_type, el_lp in zip(element_names,
                                               subtypes, link_props):
                flags = 0
                if el_lp:
                    flags |= self.EDGE_POINTER_IS_LINKPROP
                buf.append(_uint8_packer(flags))

                el_name_bytes = el_name.encode('utf-8')
                buf.append(_uint16_packer(len(el_name_bytes)))
                buf.append(el_name_bytes)
                buf.append(_uint16_packer(self.uuid_to_pos[el_type]))

            self._register_type_id(type_id)
            return type_id

        else:
            # This is a scalar type

            mt = t.material_type(self.schema)
            base_type = mt.get_topmost_concrete_base(self.schema)

            if mt is base_type:
                type_id = mt.id
                if type_id in self.uuid_to_pos:
                    # already described
                    return type_id

                buf.append(b'\x02')
                buf.append(type_id.bytes)

                self._register_type_id(type_id)
                return type_id

            else:
                bt_id = self._describe_type(base_type, view_shapes)
                type_id = mt.id

                if type_id in self.uuid_to_pos:
                    return type_id

                buf.append(b'\x03')
                buf.append(type_id.bytes)
                buf.append(_uint16_packer(self.uuid_to_pos[bt_id]))

                self._register_type_id(type_id)
                return type_id

    @classmethod
    def describe(cls, schema, typ, view_shapes):
        builder = cls(schema)
        type_id = builder._describe_type(typ, view_shapes)
        return b''.join(builder.buffer), type_id

    @classmethod
    def describe_json(cls):
        if cls._JSON_DESC is not None:
            return cls._JSON_DESC

        cls._JSON_DESC = cls._describe_json()
        return cls._JSON_DESC

    @classmethod
    def _describe_json(cls):
        json_id = s_obj.get_known_type_id('std::json')

        buf = []
        buf.append(b'\x02')
        buf.append(json_id.bytes)

        return b''.join(buf), json_id
