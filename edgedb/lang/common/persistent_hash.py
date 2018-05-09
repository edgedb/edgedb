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


"""Persistent hash implementation for builtin types."""


__all__ = ('persistent_hash', 'PersistentlyHashable')


import abc
import decimal
import functools
import struct

from hashlib import md5
from uuid import UUID


def _any_type(value):
    try:
        meth = value.persistent_hash
    except AttributeError:
        raise TypeError(
            f"un(persistently-)hashable type: {type(value)!r}") from None

    return meth()


persistent_hash = functools.singledispatch(_any_type)


@persistent_hash.register(type(None))
def _none(value):
    return _str('__edgedb__NONE__')


@persistent_hash.register(str)
def _str(value):
    return int(md5(value.encode()).hexdigest(), 16)


@persistent_hash.register(bytes)
def _bytes(value):
    return int(md5(value).hexdigest(), 16)


@persistent_hash.register(bool)
def _bool(value):
    return _str('__edgedb__TRUE__' if value else '__edgedb__FALSE__')


@persistent_hash.register(int)
def _int(value):
    return value


_floatpack = struct.Struct('>d').pack
_floatunpack = struct.Struct('>q').unpack


@persistent_hash.register(float)
def _float(value):
    return _floatunpack(_floatpack(value))[0]


@persistent_hash.register(decimal.Decimal)
def _decimal(value):
    return _str(str(value))


@persistent_hash.register(UUID)
def _uuid(value):
    return _tuple(('__stdlib_UUID__', value.hex))


@persistent_hash.register(tuple)
def _tuple(value):
    """Compute a persistent hash for a tuple."""
    # This algorithm is borrowed from CPython implementation.

    multiplier = 1000003

    result = 0x345678

    length = len(value)

    for i, item in enumerate(value):
        hash = persistent_hash(item)
        result = ((result ^ hash) * multiplier) & ((1 << 128) - 1)
        multiplier += 82520 + (length - i) * 2
    result += 97531

    return result


@persistent_hash.register(frozenset)
def _frozenset(value):
    """Compute a persistent hash for a frozenset."""
    # This algorithm is borrowed from CPython implementation.

    result = 1927868237

    for hash in sorted(persistent_hash(item) for item in value):
        result ^= (hash ^ (hash << 16) ^ 89869747) * 3644798167

    result = result * 69069 + 907133923

    return result


class PersistentlyHashable(metaclass=abc.ABCMeta):
    __slots__ = ()

    @abc.abstractmethod
    def persistent_hash(self):
        return 0

    @classmethod
    def __subclasshook__(cls, C):
        if cls is PersistentlyHashable:
            if persistent_hash.dispatch(C) is not _any_type:
                return True

            for B in C.__mro__:
                if 'persistent_hash' in B.__dict__:
                    if B.__dict__['persistent_hash']:
                        return True
                    break
        return NotImplemented
