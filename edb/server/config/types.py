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


import dataclasses
import json

from edb import errors
from edb.common import typeutils
from edb.common.vendor import typing_inspect
from edb.schema import objects as s_obj


class ConfigType:

    @classmethod
    def from_pyvalue(cls, v, *, allow_missing=False):
        """Subclasses override this to allow creation from Python scalars."""
        raise NotImplementedError

    @classmethod
    def from_json(cls, v):
        raise NotImplementedError

    def to_json(self):
        raise NotImplementedError

    def to_edgeql(self):
        raise NotImplementedError

    @classmethod
    def get_edgeql_typeid(cls):
        raise NotImplementedError


class CompositeConfigType(ConfigType):

    @classmethod
    def from_pyvalue(cls, data, *, allow_missing=False):
        if not isinstance(data, dict):
            raise cls._err(f'expected a dict value, got {type(data)!r}')

        fields = {f.name: f for f in dataclasses.fields(cls)}

        if data.keys() - fields.keys():
            inv_keys = ', '.join(repr(r) for r in data.keys() - fields.keys())
            raise cls._err(f'unknown fields: {inv_keys}')

        items = {}
        for fieldname, value in data.items():
            field = fields[fieldname]
            f_type = field.type

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
            else:
                if not isinstance(value, f_type):
                    raise cls._err(
                        f'invalid {fieldname!r} field value: expecting '
                        f'{f_type.__name__}, but got {type(value).__name__}'
                    )

            items[fieldname] = value

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
    def from_json(cls, s):
        return cls.from_pyvalue(json.loads(s))

    def to_json(self):
        dct = dataclasses.asdict(self)
        fields = {f.name: f for f in dataclasses.fields(self)}
        for fieldname, value in dct.items():
            f_type = fields[fieldname].type
            if typing_inspect.is_generic_type(f_type):
                dct[fieldname] = list(value)
        return json.dumps(dct)

    def to_edgeql(self):
        return repr(self.to_json())

    @classmethod
    def _err(cls, msg):
        return errors.ConfigurationError(
            f'invalid {cls.__name__.lower()!r} value: {msg}')
