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


from . import base as s_types
from . import numeric as s_numeric


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping


class Int64(int):
    pass


class Int32(int):
    pass


class Int16(int):
    pass


_add_impl('std::int64', Int64)
_add_impl('std::int32', Int32)
_add_impl('std::int16', Int16)
_add_map(Int64, 'std::int64')
_add_map(Int32, 'std::int32')
_add_map(Int16, 'std::int16')
_add_map(int, 'std::int64')


class StdInt64(s_types.SchemaObject, name='std::int64'):
    pass


class StdInt32(s_types.SchemaObject, name='std::int32'):
    pass


class StdInt16(s_types.SchemaObject, name='std::int16'):
    pass


class Int64TypeInfo(s_types.TypeInfo, type=Int64):
    def intop(self, other: Int64) -> StdInt64:
        pass

    def unary_intop(self) -> StdInt64:
        pass

    def float_result(self, other: Int64) -> s_numeric.StdFloat64:
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


class Int32TypeInfo(s_types.TypeInfo, type=Int32):
    def intop(self, other: Int32) -> StdInt32:
        pass

    def unary_intop(self) -> StdInt32:
        pass

    def float_result(self, other: Int32) -> s_numeric.StdFloat64:
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


class Int16TypeInfo(s_types.TypeInfo, type=Int16):
    def intop(self, other: Int16) -> StdInt16:
        pass

    def unary_intop(self) -> StdInt16:
        pass

    def float_result(self, other: Int16) -> s_numeric.StdFloat64:
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
