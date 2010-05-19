##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Persistent hash implementation for builtin types."""


import decimal
from hashlib import md5


def persistent_hash(value):
    """Compute a persistent hash for a given value.

    The hash is guaranteed to be universally stable.
    """

    if value is None:
        return str_hash('__semantix__NONE__')
    elif isinstance(value, str):
        return str_hash(value)
    elif isinstance(value, bool):
        return str_hash('__semantix__TRUE__' if value else '__semantix__FALSE__')
    elif isinstance(value, int):
        return int_hash(value)
    elif isinstance(value, float):
        return float_hash(value)
    elif isinstance(value, decimal.Decimal):
        return decimal_hash(value)
    elif isinstance(value, tuple):
        return tuple_hash(value)
    elif isinstance(value, frozenset):
        return frozenset_hash(value)
    else:
        phm = getattr(value, 'persistent_hash', None)
        if phm:
            return phm()
    raise TypeError("un(persistently-)hashable type: '%s'" % type(value).__name__)


def str_hash(value):
    """Compute a persistent hash for a string value"""
    return int(md5(value.encode()).hexdigest(), 16)


def int_hash(value):
    """Compute a persistent hash for an integer value"""
    return value


def float_hash(value):
    """Compute a persistent hash for a float value"""
    # XXX: Check if this is really persistent cross-arch
    return hash(value)


def decimal_hash(value):
    """Compute a persistent hash for a Decimal value"""
    # XXX: Check if this is really persistent cross-arch
    return hash(value)


def tuple_hash(value):
    """Compute a persistent hash for a tuple"""

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


def frozenset_hash(value):
    """Compute a persistent hash for a frozenset"""

    # This algorithm is borrowed from CPython implementation.

    result = 1927868237

    for hash in sorted(persistent_hash(item) for item in value):
        result ^= (hash ^ (hash << 16) ^ 89869747)  * 3644798167

    result = result * 69069 + 907133923

    return result
