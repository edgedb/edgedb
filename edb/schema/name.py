#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

import functools

from edb import errors


NameT = TypeVar("NameT", bound="SchemaName")


class SchemaName(str):
    __slots__ = ('module', 'name')

    module: str
    name: str

    def __new__(
        cls: Type[NameT],
        name: Union[SchemaName, str],
        module: Optional[str] = None,
    ) -> NameT:
        if not name:
            raise NameError('name must not be empty')

        if isinstance(name, SchemaName):
            _name = name.name
            _module = name.module
        elif module is not None:
            _name = name
            _module = module
        else:
            _module, _, _name = name.rpartition('::')

            if not _module:
                if not module:
                    err = 'improperly formed name: ' \
                          'module is not specified: {}'.format(name)
                    raise errors.InvalidReferenceError(err)
                else:
                    _module = module

        # Ignore below since Mypy doesn't believe you can pass `_fullname` to
        # object.__new__.
        _fullname = f"{_module}::{_name}"
        result = super().__new__(cls, _fullname)  # type: ignore
        result.name = _name
        result.module = _module

        return cast(NameT, result)

    def as_tuple(self) -> Tuple[str, str]:
        return (self.module, self.name)

    @staticmethod
    def is_qualified(name: Union[SchemaName, str]) -> bool:
        return isinstance(name, SchemaName) or '::' in name


class UnqualifiedName(SchemaName):

    def __new__(cls, name: str) -> UnqualifiedName:
        # Ignore below since Mypy doesn't believe you can pass `name` to
        # object.__new__.
        result = str.__new__(cls, name)  # type: ignore
        result.name = name
        result.module = ''

        return cast(UnqualifiedName, result)


Name = SchemaName


def split_name(
    name: Union[SchemaName, str]
) -> Tuple[Union[SchemaName, str], Optional[str], str]:
    module: Optional[str]
    nqname: str

    if isinstance(name, SchemaName):
        module = name.module
        nqname = name.name
    elif isinstance(name, tuple):
        module = name[0]
        nqname = name[1]
        name = module + '::' + nqname if module else nqname
    elif SchemaName.is_qualified(name):
        name = SchemaName(name)
        module = name.module
        nqname = name.name
    else:
        module = None
        nqname = name

    return name, module, nqname


def mangle_name(name: Union[SchemaName, str]) -> str:
    return name.replace('::', '|')


def unmangle_name(name: str) -> str:
    return name.replace('|', '::')


@functools.lru_cache(4096)
def shortname_from_fullname(fullname: SchemaName) -> SchemaName:
    parts = str(fullname.name).split('@@', 1)
    if len(parts) == 2:
        return SchemaName(unmangle_name(parts[0]))
    else:
        return SchemaName(fullname)


@functools.lru_cache(4096)
def quals_from_fullname(fullname: SchemaName) -> List[str]:
    _, _, mangled_quals = fullname.name.partition('@@')
    return [unmangle_name(p) for p in mangled_quals.split('@')]


def get_specialized_name(
    basename: Union[SchemaName, str], *qualifiers: str
) -> str:
    return (mangle_name(basename) +
            '@@' +
            '@'.join(mangle_name(qualifier)
                     for qualifier in qualifiers if qualifier))


def is_fullname(name: str) -> bool:
    return (
        SchemaName.is_qualified(name)
        and '@@' in name
    )
