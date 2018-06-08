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


import decimal
import functools

from edb.lang.common import exceptions as edgedb_error
from edb.lang.common import fpdecimal

from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping
_void = object()


class DecimalMetadata:
    def __init__(self, context=None, *, decimal_scale=None,
                 quantize_exponent=None):
        if context is None:
            context = decimal.Context()

        self.decimal_context = context
        self.decimal_scale = decimal_scale
        self.quantize_exponent = quantize_exponent


class DecimalMeta(type):
    def __new__(metacls, name, bases, dct):
        result = super().__new__(metacls, name, bases, dct)
        if result.__module__ == __name__ and result.__name__ == 'Decimal':
            metacls.init_methods(result)
            result.__sx_decimal_metadata__ = DecimalMetadata()
        return result

    _rounding_map = {
        'ceiling': decimal.ROUND_CEILING,
        'down': decimal.ROUND_DOWN,
        'floor': decimal.ROUND_FLOOR,
        'half-down': decimal.ROUND_HALF_DOWN,
        'half-even': decimal.ROUND_HALF_EVEN,
        'half-up': decimal.ROUND_HALF_UP,
        'up': decimal.ROUND_UP,
        '05up': decimal.ROUND_05UP
    }

    @classmethod
    def new(metacls, cls, schema_obj):
        ctxargs = {}
        precision = schema_obj.get_attribute('std::precision')
        if precision:
            precision = precision.value
            ctxargs['prec'] = precision[0]

            try:
                decimal_scale = precision[1]
            except IndexError:
                decimal_scale = 0

            if decimal_scale:
                quantize_exponent = decimal.Decimal(10) ** -decimal_scale
            else:
                quantize_exponent = None

            ctxargs['Emax'] = ctxargs['prec'] - decimal_scale - 1
        else:
            decimal_scale = None
            quantize_exponent = None

        rounding = schema_obj.get_attribute('std::rounding')
        if rounding:
            ctxargs['rounding'] = metacls._rounding_map[rounding.value]

        context = decimal.Context(**ctxargs)
        cls.__sx_decimal_metadata__ = \
            DecimalMetadata(context, decimal_scale=decimal_scale,
                            quantize_exponent=quantize_exponent)

    @classmethod
    def init_methods(metacls, cls):
        for category in metacls.ops.values():
            for op in category['ops']:
                orig_fn = getattr(cls, op)
                wrapper = cls.get_wrapper(op, category.get('coerce'),
                                          category.get('quantize'))
                functools.update_wrapper(wrapper, orig_fn)
                setattr(cls, op, wrapper)
                setattr(cls, op + '__orig', orig_fn)

    def get_wrapper(cls, method, coerce=False, quantize=False):
        def wrapper(self, *args, **kwargs):
            result = getattr(cls, method + '__orig')(self, *args, **kwargs)
            if coerce:
                result = self.__class__(result)
            metadata = self.__class__.__sx_decimal_metadata__
            if quantize and metadata.decimal_scale is not None:
                result = result.quantize()
            return result
        return wrapper

    ops = {
        'comparison': {
            'ops': ('__lt__', '__le__', '__gt__', '__ge__', 'compare'),
            'quantize': False,
            'coerce': False
        },

        'unary_arith': {
            'ops': ('__neg__', '__pos__'),
            'coerce': True
        },

        'binary_arith': {
            'ops': ('__add__', '__radd__', '__sub__', '__rsub__', '__mul__',
                    '__rmul__', '__truediv__', '__rtruediv__', '__mod__',
                    '__rmod__', 'remainder_near', '__floordiv__',
                    '__rfloordiv__'),
            'quantize': False,
            'coerce': False
        },

        'ternary_arith': {
            'ops': ('fma', '__pow__', '__rpow__'),
            'coerce': False
        }
    }


class Decimal(fpdecimal.FPDecimal, metaclass=DecimalMeta):
    def __new__(cls, value=_void):
        cumulative, last_increment = fpdecimal.CascadedContext.get()
        context = fpdecimal.CascadedContext.apply(
            cls.__sx_decimal_metadata__.decimal_context)

        if value is _void:
            value = cls.default()

        if value is None:
            value = 0

        if cumulative.scale is not None:
            fpcontext = cumulative
        else:
            scale = cls.__sx_decimal_metadata__.decimal_scale
            quantize_exponent = cls.__sx_decimal_metadata__.quantize_exponent
            fpcontext = fpdecimal.CascadedContext(
                scale=scale, quantize_exponent=quantize_exponent)

        try:
            with decimal.localcontext(context), fpcontext:
                result = fpdecimal.FPDecimal.__new__(cls, value)
        except (ValueError, decimal.InvalidOperation) as e:
            raise edgedb_error.ScalarTypeValueError(e.args[0]) from e

        return result

    def normalize(self, context=None):
        if context is None:
            context = self.__class__.__sx_decimal_metadata__.decimal_context
        return super().normalize(context=context)

    def quantize(self, exp=None, rounding=None, context=None):
        if context is None:
            context = self.__class__.__sx_decimal_metadata__.decimal_context
        if exp is None:
            exp = self.__class__.__sx_decimal_metadata__.decimal_scale
        kwargs = {}
        if rounding is not None:
            kwargs['rounding'] = rounding
        return self.__class__(super().quantize(exp, context=context, **kwargs))


class StdDecimal(s_types.SchemaObject, name='std::decimal'):
    pass


class DecimalTypeInfo(s_types.TypeInfo, type=Decimal):
    def op(self, other: (decimal.Decimal, int)) -> StdDecimal:
        pass

    def unary_op(self) -> StdDecimal:
        pass

    __add__ = op
    __radd__ = op
    __sub__ = op
    __rsub__ = op
    __mul__ = op
    __rmul__ = op

    __truediv__ = op
    __rtruediv__ = op

    __floordiv__ = op
    __rfloordiv__ = op

    __mod__ = op
    __rmod__ = op

    __pow__ = op
    __rpow__ = op

    __neg__ = unary_op
    __pos__ = unary_op

    __abs__ = unary_op
    __invert__ = unary_op


_add_impl('std::decimal', Decimal)
_add_map(Decimal, 'std::decimal')
_add_map(fpdecimal.FPDecimal, 'std::decimal')
_add_map(decimal.Decimal, 'std::decimal')


class Float64(float):
    pass


class Float32(float):
    pass


class StdFloat64(s_types.SchemaObject, name='std::float64'):
    pass


class StdFloat32(s_types.SchemaObject, name='std::float32'):
    pass


_add_impl('std::float64', Float64)
_add_impl('std::float32', Float32)
_add_map(Float64, 'std::float64')
_add_map(Float32, 'std::float32')
_add_map(float, 'std::float64')


class Float64TypeInfo(s_types.TypeInfo, type=Float64):
    def op(self, other: (int, float)) -> StdFloat64:
        pass

    def unary_op(self) -> StdFloat64:
        pass

    __add__ = op
    __radd__ = op
    __sub__ = op
    __rsub__ = op
    __mul__ = op
    __rmul__ = op

    __truediv__ = op
    __rtruediv__ = op

    __floordiv__ = op
    __rfloordiv__ = op

    __mod__ = op
    __rmod__ = op

    __pow__ = op
    __rpow__ = op

    __neg__ = unary_op
    __pos__ = unary_op

    __abs__ = unary_op
    __invert__ = unary_op


class Float32TypeInfo(s_types.TypeInfo, type=Float32):
    def op(self, other: (int, float)) -> StdFloat32:
        pass

    def unary_op(self) -> StdFloat32:
        pass

    __add__ = op
    __radd__ = op
    __sub__ = op
    __rsub__ = op
    __mul__ = op
    __rmul__ = op

    __truediv__ = op
    __rtruediv__ = op

    __floordiv__ = op
    __rfloordiv__ = op

    __mod__ = op
    __rmod__ = op

    __pow__ = op
    __rpow__ = op

    __neg__ = unary_op
    __pos__ = unary_op

    __abs__ = unary_op
    __invert__ = unary_op
