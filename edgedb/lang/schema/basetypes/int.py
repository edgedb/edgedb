##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import exceptions as edgedb_error

from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping


class Int(int):
    def __new__(cls, value=0):
        try:
            value = super().__new__(cls, value)
        except ValueError as e:
            raise edgedb_error.ScalarTypeValueError(e.args[0]) from e

        if not -0x7FFFFFFFFFFFFFFF <= value <= 0x7FFFFFFFFFFFFFFF:
            raise edgedb_error.ScalarTypeValueError(
                'value is out of Int range')
        return value


_add_impl('std::int', Int)
_add_map(Int, 'std::int')
_add_map(int, 'std::int')


class StdInt(s_types.SchemaObject, name='std::int'):
    pass


class StdFloat(s_types.SchemaObject, name='std::float'):
    pass


class IntTypeInfo(s_types.TypeInfo, type=Int):
    def intop(self, other: int) -> StdInt:
        pass

    def unary_intop(self) -> StdInt:
        pass

    def float_result(self, other: int) -> StdFloat:
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

    __neg__ = unary_intop
    __pos__ = unary_intop

    __abs__ = intop
    __invert__ = intop
