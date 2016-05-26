##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos import error

from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping


class Int(int):
    def __new__(cls, value=0):
        try:
            value = super().__new__(cls, value)
        except ValueError as e:
            raise error.AtomValueError(e.args[0]) from e

        if not -0x7FFFFFFFFFFFFFFF <= value <= 0x7FFFFFFFFFFFFFFF:
            raise error.AtomValueError('value is out of Int range')
        return value

_add_impl('metamagic.caos.builtins.int', Int)
_add_map(Int, 'metamagic.caos.builtins.int')
_add_map(int, 'metamagic.caos.builtins.int')


class IntTypeInfo(s_types.TypeInfo, type=Int):
    def intop(self, other: int) -> 'metamagic.caos.builtins.int':
        pass

    def float_result(self, other: int) -> 'metamagic.caos.builtins.float':
        pass

    __add__ = intop
    __radd__ = intop
    __sub__ = intop
    __rsub__ = intop
    __mul__ = intop
    __rmul__ = intop

    __truediv__ = float_result
    __rtruediv__ = float_result

    __floordiv__ = intop
    __rfloordiv__ = intop

    __mod__ = intop
    __rmod__ = intop

    __pow__ = intop
    __rpow__ = intop

    __neg__ = intop
    __pos__ = intop

    __abs__ = intop
    __invert__ = intop
