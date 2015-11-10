##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Persistent hash implementation for builtin types."""


import abc
import contextlib
import decimal
from hashlib import md5
from uuid import UUID


class persistent_hash(int):
    magic_method = 'persistent_hash'

    _hash_memory = []

    def __new__(cls, value):
        """Compute a persistent hash for a given value.

        The hash is guaranteed to be universally stable.
        """

        hash = None

        if value is None:
            hash = cls.str_hash('__metamagic__NONE__')
        elif isinstance(value, str):
            hash = cls.str_hash(value)
        elif isinstance(value, bool):
            hash = cls.str_hash('__metamagic__TRUE__' if value else '__metamagic__FALSE__')
        elif isinstance(value, int):
            hash = cls.int_hash(value)
        elif isinstance(value, float):
            hash = cls.float_hash(value)
        elif isinstance(value, decimal.Decimal):
            hash = cls.decimal_hash(value)
        elif isinstance(value, tuple):
            hash = cls.tuple_hash(value)
        elif isinstance(value, frozenset):
            hash = cls.frozenset_hash(value)
        elif isinstance(value, UUID):
            hash = cls.tuple_hash(('__stdlib_UUID__', value.hex))
        else:
            for bucket in reversed(cls._hash_memory):
                hash = bucket.get(id(value))
                if hash is not None:
                    break
            else:
                phm = getattr(value, cls.magic_method, None)
                if phm:
                    hash = phm()

        if hash is None:
            raise TypeError("un(persistently-)hashable type: '%s'" % type(value).__name__)

        return super().__new__(cls, hash)

    @classmethod
    def str_hash(cls, value):
        """Compute a persistent hash for a string value"""
        return int(md5(value.encode()).hexdigest(), 16)

    @classmethod
    def int_hash(cls, value):
        """Compute a persistent hash for an integer value"""
        return value

    @classmethod
    def float_hash(cls, value):
        """Compute a persistent hash for a float value"""
        # XXX: Check if this is really persistent cross-arch
        return hash(value)

    @classmethod
    def decimal_hash(cls, value):
        """Compute a persistent hash for a Decimal value"""
        # XXX: Check if this is really persistent cross-arch
        return hash(value)

    @classmethod
    def tuple_hash(cls, value):
        """Compute a persistent hash for a tuple"""

        # This algorithm is borrowed from CPython implementation.

        multiplier = 1000003

        result = 0x345678

        length = len(value)

        for i, item in enumerate(value):
            hash = cls(item)
            result = ((result ^ hash) * multiplier) & ((1 << 128) - 1)
            multiplier += 82520 + (length - i) * 2
        result += 97531

        return result

    @classmethod
    def frozenset_hash(cls, value):
        """Compute a persistent hash for a frozenset"""

        # This algorithm is borrowed from CPython implementation.

        result = 1927868237

        for hash in sorted(cls(item) for item in value):
            result ^= (hash ^ (hash << 16) ^ 89869747)  * 3644798167

        result = result * 69069 + 907133923

        return result

    @classmethod
    @contextlib.contextmanager
    def memory(cls, *pairs):
        try:
            bucket = {id(obj): hash for obj, hash in pairs}
            cls._hash_memory.append(bucket)
            yield bucket
        finally:
            cls._hash_memory.pop()


_NoneType = type(None)
_hashables = (_NoneType, str, bool, int, float, decimal.Decimal, tuple,
              frozenset, UUID)


class PersistentlyHashable(metaclass=abc.ABCMeta):
    __slots__ = ()

    @abc.abstractmethod
    def persistent_hash(self):
        return 0

    @classmethod
    def __subclasshook__(cls, C):
        if cls is PersistentlyHashable:
            for B in C.__mro__:
                if issubclass(B, _hashables):
                    return True

                if 'persistent_hash' in B.__dict__:
                    if B.__dict__['persistent_hash']:
                        return True
                    break
        return NotImplemented
