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
import typing

from edb import errors
from edb.schema import objects as s_obj


class ConfigType:

    @classmethod
    def from_pyvalue(cls, v):
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


@dataclasses.dataclass(frozen=True, eq=True)
class Port(ConfigType):

    protocol: str
    database: str
    port: int
    concurrency: int
    user: str
    address: typing.FrozenSet[str] = frozenset({'localhost'})

    @classmethod
    def get_edgeql_typeid(cls):
        return s_obj.get_known_type_id('std::json')

    @classmethod
    def from_json(cls, s):
        return cls.from_pyvalue(s)

    def to_json(self):
        dct = dataclasses.asdict(self)
        if 'address' in dct and isinstance(dct['address'], frozenset):
            dct['address'] = list(dct['address'])
        return json.dumps(dct)

    def to_edgeql(self):
        return repr(self.to_json())

    @classmethod
    def from_pyvalue(cls, s):
        if not isinstance(s, str):
            raise errors.ConfigurationError(
                'invalid "ports" config value: a std::str expected')

        try:
            data = json.loads(s)
        except Exception:
            raise errors.ConfigurationError(
                'invalid "ports" config value: not a valid JSON string')

        if not isinstance(data, dict):
            raise errors.ConfigurationError(
                'invalid "ports" config value: a JSON object expected')

        fields = {f.name: f for f in dataclasses.fields(cls)}

        if data.keys() - fields.keys():
            inv_keys = ', '.join(repr(r) for r in data.keys() - fields.keys())
            raise errors.ConfigurationError(
                f'invalid "ports" config value: unknown fields: {inv_keys}')

        items = {}
        for fieldname, value in data.items():
            if fieldname == 'address':
                is_valid = (
                    isinstance(value, str) or
                    isinstance(value, list) and
                    all(isinstance(el, str) for el in value)
                )
                if not is_valid:
                    raise errors.ConfigurationError(
                        'invalid "ports" config value: '
                        '"address" field must be a string or '
                        'an array of strings')
                if isinstance(value, list):
                    value = frozenset(value)
                else:
                    value = frozenset({value})
            else:
                fieldtype = fields[fieldname].type
                if not isinstance(value, fieldtype):
                    raise errors.ConfigurationError(
                        f'invalid "ports" config value: '
                        f'"{fieldname}" field must be a {fieldtype.__name__}, '
                        f'got {type(value).__name__} instead')

            items[fieldname] = value

        for fieldname, field in fields.items():
            if fieldname not in items and field.default is dataclasses.MISSING:
                raise errors.ConfigurationError(
                    f'invalid "ports" config value: '
                    f'"{fieldname}" field is required')

        try:
            return cls(**items)
        except (TypeError, ValueError) as ex:
            raise errors.ConfigurationError(
                f'invalid "ports" config value: {ex}')
