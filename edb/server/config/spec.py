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


import collections.abc
import dataclasses


from . import types


@dataclasses.dataclass(frozen=True, eq=True)
class Setting:

    name: str
    type: type
    default: object
    set_of: bool = False
    system: bool = False
    internal: bool = False

    def __post_init__(self):
        if (self.type not in {str, int, bool} and
                not issubclass(self.type, types.ConfigType)):
            raise ValueError(
                f'invalid config setting {self.name!r}: '
                f'type is expected to be either one of {{str, int, bool}} '
                f'or an edb.server.config.types.ConfigType subclass')

        if self.internal and self.system:
            raise ValueError(
                f'invalid config setting {self.name!r}: cannot be both '
                f'"system" and "internal"')

        if self.set_of:
            if not isinstance(self.default, frozenset):
                raise ValueError(
                    f'invalid config setting {self.name!r}: "SET OF" settings '
                    f'must have frozenset() as a default value, got '
                    f'{self.default!r}')

            if self.default:
                # SET OF settings shouldn't have non-empty defaults,
                # as otherwise there are multiple semantical ambiguities:
                # * Can a user add a new element to the set?
                # * What happens of a user discards all elements from the set?
                #   Does the set become non-empty because the default would
                #   propagate?
                # * etc.
                raise ValueError(
                    f'invalid config setting {self.name!r}: "SET OF" settings '
                    f'should not have defaults')

        else:
            if not isinstance(self.default, self.type):
                raise ValueError(
                    f'invalid config setting {self.name!r}: '
                    f'the default {self.default!r} '
                    f'is not instance of {self.type}')


class Spec(collections.abc.Mapping):

    def __init__(self, *settings: Setting):
        self._settings = tuple(settings)
        self._by_name = {s.name: s for s in self._settings}

    def __iter__(self):
        return iter(self._by_name)

    def __getitem__(self, name: str) -> Setting:
        return self._by_name[name]

    def __contains__(self, name: str):
        return name in self._by_name

    def __len__(self):
        return len(self._settings)
