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

from edb import errors


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


@dataclasses.dataclass(frozen=True, eq=True)
class Port(ConfigType):

    protocol: str
    database: str
    port: int
    concurrency: int

    @classmethod
    def from_json(cls, s):
        return cls.from_pyvalue(s)

    def to_json(self):
        return ';'.join(
            f'{field}={str(getattr(self, field))}'
            for field in type(self).__dataclass_fields__
        )

    def to_edgeql(self):
        return repr(self.to_json())

    @classmethod
    def from_pyvalue(cls, s):
        if not isinstance(s, str):
            raise errors.ConfigurationError(
                '"ports" config requires std::str values')

        items = {}

        parts = s.split(';')
        for part in parts:
            left, _, right = part.partition('=')
            if not left or not right:
                raise errors.ConfigurationError(
                    'invalid key/value item for the "ports" config')

            if left not in cls.__dataclass_fields__:
                raise errors.ConfigurationError(
                    f'{left!r} is not a valid config key for the '
                    f'"ports" config')

            try:
                right = cls.__dataclass_fields__[left].type(right)
            except (TypeError, ValueError):
                raise errors.ConfigurationError(
                    f'{left}={right!r} is not a valid config value for the '
                    f'"ports" config')

            items[left] = right

        try:
            return cls(**items)
        except (TypeError, ValueError):
            raise errors.ConfigurationError(
                f'invalid "ports" config {s!r}')
