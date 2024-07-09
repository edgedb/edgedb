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
from typing import Any, Type, TypeVar, List, NamedTuple, TYPE_CHECKING

import abc
import functools
import re

from edb import errors
from edb.common import markup


NameT = TypeVar("NameT", bound="Name")
QualNameT = TypeVar("QualNameT", bound="QualName")
UnqualNameT = TypeVar("UnqualNameT", bound="UnqualName")


# Unfortunately, there is no way to convince mypy that QualName
# and UnqualName are implementations of the Name ABC:
# NamedTuple doesn't support multiple inheritance, and ABCMeta.register
# is not supported either.  And so, we must resort to stubbing.
if TYPE_CHECKING:

    class Name:

        name: str

        @classmethod
        def from_string(cls: Type[NameT], name: str) -> NameT:
            ...

        def get_local_name(self) -> UnqualName:
            ...

        def get_root_module_name(self) -> UnqualName:
            ...

        def __lt__(self, other: Any) -> bool:
            ...

        def __le__(self, other: Any) -> bool:
            ...

        def __gt__(self, other: Any) -> bool:
            ...

        def __ge__(self, other: Any) -> bool:
            ...

        def __str__(self) -> str:
            ...

        def __repr__(self) -> str:
            ...

        def __hash__(self) -> int:
            ...

    class QualName(Name):

        module: str
        name: str

        @classmethod
        def from_string(
            cls: Type[QualNameT],
            name: str,
        ) -> QualNameT:
            ...

        def __init__(self, module: str, name: str) -> None:
            ...

        def get_local_name(self) -> UnqualName:
            ...

        def get_module_name(self) -> Name:
            ...

    class UnqualName(Name):

        __slots__ = ('name',)

        name: str

        @classmethod
        def from_string(
            cls: Type[UnqualNameT],
            name: str,
        ) -> UnqualNameT:
            ...

        def __init__(self, name: str) -> None:
            ...

        def get_local_name(self) -> UnqualName:
            ...

else:

    class Name(abc.ABC):  # noqa: B024
        pass

    class QualName(NamedTuple):

        module: str
        name: str

        @classmethod
        def from_string(
            cls: Type[QualNameT],
            name: str,
        ) -> QualNameT:

            module, _, nqname = name.rpartition('::')

            if not module:
                err = (
                    f'improperly formed name {name!r}: '
                    f'module is not specified'
                )
                raise errors.InvalidReferenceError(err)

            return cls(
                module=module,
                name=nqname,
            )

        def get_local_name(self) -> UnqualName:
            return UnqualName(self.name)

        def get_module_name(self) -> Name:
            return UnqualName(self.module)

        def get_root_module_name(self) -> UnqualName:
            return UnqualName(self.module.partition('::')[0])

        def __str__(self) -> str:
            return f'{self.module}::{self.name}'

        def __repr__(self) -> str:
            return f'<QualName {self}>'

    class UnqualName(NamedTuple):

        name: str

        @classmethod
        def from_string(
            cls: Type[UnqualNameT],
            name: str,
        ) -> UnqualNameT:
            return cls(name)

        def get_local_name(self) -> UnqualName:
            return self

        def get_root_module_name(self) -> UnqualName:
            return UnqualName(self.name.partition('::')[0])

        def __str__(self) -> str:
            return self.name

        def __repr__(self) -> str:
            return f'<UnqualName {self.name}>'

    Name.register(QualName)
    Name.register(UnqualName)


def is_qualified(name: str) -> bool:
    return '::' in name


def name_from_string(name: str) -> Name:
    if is_qualified(name):
        return QualName.from_string(name)
    else:
        return UnqualName.from_string(name)


def mangle_name(name: str) -> str:
    return (
        name
        .replace('|', '||')
        .replace('&', '&&')
        .replace('::', '|')
        .replace('@', '&')
    )


mangle_re_1 = re.compile(r'(?<![|])\|(?![|])')
mangle_re_2 = re.compile(r'(?<![&])&(?![&])')


def unmangle_name(name: str) -> str:
    name = mangle_re_1.sub('::', name)
    name = mangle_re_2.sub('@', name)
    return name.replace('||', '|').replace('&&', '&')


@functools.lru_cache(10240)
def shortname_from_fullname(fullname: Name) -> Name:
    name = fullname.name
    parts = name.split('@', 1)
    if len(parts) == 2:
        return name_from_string(unmangle_name(parts[0]))
    else:
        return fullname


@functools.lru_cache(4096)
def quals_from_fullname(fullname: QualName) -> List[str]:
    _, _, mangled_quals = fullname.name.partition('@')
    return (
        [unmangle_name(p) for p in mangled_quals.split('@')]
        if mangled_quals else []
    )


def get_specialized_name(basename: Name, *qualifiers: str) -> str:
    mangled_quals = '@'.join(mangle_name(qual) for qual in qualifiers if qual)
    return f'{mangle_name(str(basename))}@{mangled_quals}'


def is_fullname(name: str) -> bool:
    return is_qualified(name) and '@' in name


def compat_get_specialized_name(basename: str, *qualifiers: str) -> str:
    mangled_quals = '@'.join(
        compat_mangle_name(qual) for qual in qualifiers if qual
    )
    return f'{compat_mangle_name(basename)}@@{mangled_quals}'


def compat_mangle_name(name: str) -> str:
    return name.replace('::', '|')


def compat_name_remangle(name: str) -> Name:
    if is_fullname(name):
        qname = QualName.from_string(name)
        sn = shortname_from_fullname(qname)
        quals = list(quals_from_fullname(qname))
        if quals and is_fullname(quals[0]):
            quals[0] = str(compat_name_remangle(quals[0]))
        compat_sn = compat_get_specialized_name(str(sn), *quals)
        return QualName(name=compat_sn, module=qname.module)
    else:
        return name_from_string(name)


@markup.serializer.no_ref_detect
@markup.serializer.serializer.register(Name)
def _serialize_to_markup(obj: Name, *, ctx: markup.Context) -> markup.Markup:
    return markup.elements.lang.Object(
        id=id(obj), class_module=type(obj).__module__,
        classname=type(obj).__name__, repr=str(obj))
