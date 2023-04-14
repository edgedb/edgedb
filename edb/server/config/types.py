#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


from edb import errors
from edb.common import typeutils
from edb.common import typing_inspect
from edb.schema import objects as s_obj
from edb.schema import name as s_name


class ConfigType:

    @classmethod
    def from_pyvalue(cls, v, *, spec, allow_missing=False):
        """Subclasses override this to allow creation from Python scalars."""
        raise NotImplementedError

    @classmethod
    def from_json_value(cls, v, *, spec):
        raise NotImplementedError

    def to_json_value(self):
        raise NotImplementedError

    @classmethod
    def get_edgeql_typeid(cls):
        raise NotImplementedError


class CompositeConfigType(ConfigType):

    @classmethod
    def from_pyvalue(cls, data, *, spec, allow_missing=False):
        if not isinstance(data, dict):
            raise cls._err(f'expected a dict value, got {type(data)!r}')

        data = dict(data)
        tname = data.pop('_tname', None)
        if tname is not None:
            if '::' in tname:
                tname = s_name.QualName.from_string(tname).name
            cls = spec.get_type_by_name(tname)

        fields = {f.name: f for f in dataclasses.fields(cls)}

        items = {}
        inv_keys = []
        for fieldname, value in data.items():
            field = fields.get(fieldname)
            if field is None:
                if value is None:
                    # This may happen when data is produced by
                    # a polymorphic config query.
                    pass
                else:
                    inv_keys.append(fieldname)

                continue

            f_type = field.type

            if value is None:
                # Config queries return empty pointer values as None.
                continue

            if typing_inspect.is_generic_type(f_type):
                container = typing_inspect.get_origin(f_type)
                if container not in (frozenset, list):
                    raise RuntimeError(
                        f'invalid type annotation on '
                        f'{cls.__name__}.{fieldname}: '
                        f'{f_type!r} is not supported')

                eltype = typing_inspect.get_args(f_type, evaluate=True)[0]
                if isinstance(value, eltype):
                    value = container((value,))
                elif (typeutils.is_container(value)
                        and all(isinstance(v, eltype) for v in value)):
                    value = container(value)
                else:
                    raise cls._err(
                        f'invalid {fieldname!r} field value: expecting '
                        f'{eltype.__name__} or a list thereof, but got '
                        f'{type(value).__name__}'
                    )

            elif (issubclass(f_type, CompositeConfigType)
                    and isinstance(value, dict)):

                tname = value.get('_tname', None)
                if tname is not None:
                    if '::' in tname:
                        tname = s_name.QualName.from_string(tname).name
                    actual_f_type = spec.get_type_by_name(tname)
                else:
                    actual_f_type = f_type
                    value['_tname'] = f_type.__name__

                value = actual_f_type.from_pyvalue(value, spec=spec)

            elif not isinstance(value, f_type):
                raise cls._err(
                    f'invalid {fieldname!r} field value: expecting '
                    f'{f_type.__name__}, but got {type(value).__name__}'
                )

            items[fieldname] = value

        if inv_keys:
            inv_keys = ', '.join(repr(r) for r in inv_keys)
            raise cls._err(f'unknown fields: {inv_keys}')

        for fieldname, field in fields.items():
            if fieldname not in items and field.default is dataclasses.MISSING:
                if allow_missing:
                    items[fieldname] = None
                else:
                    raise cls._err(f'missing required field: {fieldname!r}')

        try:
            return cls(**items)
        except (TypeError, ValueError) as ex:
            raise cls._err(str(ex))

    @classmethod
    def get_edgeql_typeid(cls):
        return s_obj.get_known_type_id('std::json')

    @classmethod
    def from_json_value(cls, s, *, spec):
        return cls.from_pyvalue(s, spec=spec)

    def to_json_value(self):
        dct = {}
        dct['_tname'] = self.__class__.__name__

        for f in dataclasses.fields(self):
            f_type = f.type
            value = getattr(self, f.name)
            if (isinstance(f_type, type)
                    and issubclass(f_type, CompositeConfigType)
                    and value is not None):
                value = value.to_json_value()
            elif typing_inspect.is_generic_type(f_type):
                value = list(value) if value is not None else []

            dct[f.name] = value

        return dct

    @classmethod
    def _err(cls, msg):
        return errors.ConfigurationError(
            f'invalid {cls.__name__.lower()!r} value: {msg}')
