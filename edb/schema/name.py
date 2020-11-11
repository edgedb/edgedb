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
import re

from edb import errors


NameT = TypeVar("NameT", bound="QualifiedName")


class QualifiedName(str):
    __slots__ = ('module', 'name')

    module: str
    name: str

    def __new__(
        cls: Type[NameT],
        name: Union[QualifiedName, str],
        module: Optional[str] = None,
    ) -> NameT:
        if not name:
            raise NameError('name must not be empty')

        if isinstance(name, QualifiedName):
            _name = name.name
            _module = name.module
        elif module is not None:
            _name = name
            _module = module
        else:
            _module, _, _name = name.rpartition('::')

            if not _module:
                if not module:
                    err = (
                        f'improperly formed name {name!r}: '
                        f'module is not specified'
                    )
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
    def is_qualified(name: Union[QualifiedName, str]) -> bool:
        return isinstance(name, QualifiedName) or '::' in name


class UnqualifiedName(QualifiedName):

    def __new__(cls, name: str) -> UnqualifiedName:
        # Ignore below since Mypy doesn't believe you can pass `name` to
        # object.__new__.
        result = str.__new__(cls, name)  # type: ignore
        result.name = name
        result.module = ''

        return cast(UnqualifiedName, result)


def split_name(
    name: Union[QualifiedName, str]
) -> Tuple[Union[QualifiedName, str], Optional[str], str]:
    module: Optional[str]
    nqname: str

    if isinstance(name, QualifiedName):
        module = name.module
        nqname = name.name
    elif isinstance(name, tuple):
        module = name[0]
        nqname = name[1]
        name = module + '::' + nqname if module else nqname
    elif QualifiedName.is_qualified(name):
        name = QualifiedName(name)
        module = name.module
        nqname = name.name
    else:
        module = None
        nqname = name

    return name, module, nqname


def mangle_name(name: Union[QualifiedName, str]) -> str:
    return (
        name
        .replace('|', '||')
        .replace('&', '&&')
        .replace('::', '|')
        .replace('@', '&')
    )


def unmangle_name(name: str) -> str:
    name = re.sub(r'(?<![|])\|(?![|])', '::', name)
    name = re.sub(r'(?<![&])&(?![&])', '@', name)
    return name.replace('||', '|').replace('&&', '&')


@functools.lru_cache(4096)
def shortname_str_from_fullname(fullname: str) -> str:
    if isinstance(fullname, QualifiedName):
        name = fullname.name
    else:
        # `name` is a str
        name = fullname

    parts = str(name).split('@', 1)
    if len(parts) == 2:
        return unmangle_name(parts[0])
    else:
        return fullname


@functools.lru_cache(4096)
def shortname_from_fullname(fullname: QualifiedName) -> QualifiedName:
    return QualifiedName(shortname_str_from_fullname(fullname))


@functools.lru_cache(4096)
def quals_from_fullname(fullname: QualifiedName) -> List[str]:
    _, _, mangled_quals = fullname.name.partition('@')
    return [unmangle_name(p) for p in mangled_quals.split('@')]


def get_specialized_name(
    basename: Union[QualifiedName, str], *qualifiers: str
) -> str:
    mangled_quals = '@'.join(mangle_name(qual) for qual in qualifiers if qual)
    return f'{mangle_name(basename)}@{mangled_quals}'


def is_fullname(name: str) -> bool:
    return QualifiedName.is_qualified(name) and '@' in name


def compat_get_specialized_name(
    basename: Union[QualifiedName, str], *qualifiers: str
) -> str:
    mangled_quals = '@'.join(
        compat_mangle_name(qual) for qual in qualifiers if qual
    )
    return f'{compat_mangle_name(basename)}@@{mangled_quals}'


def compat_mangle_name(name: Union[QualifiedName, str]) -> str:
    return name.replace('::', '|')


def compat_name_remangle(name: str) -> str:
    if is_fullname(name):
        qname = QualifiedName(name)
        sn = shortname_str_from_fullname(qname)
        quals = list(quals_from_fullname(qname))
        if quals and is_fullname(quals[0]):
            quals[0] = compat_name_remangle(quals[0])
        compat_sn = compat_get_specialized_name(sn, *quals)
        return QualifiedName(name=compat_sn, module=qname.module)
    else:
        return name
