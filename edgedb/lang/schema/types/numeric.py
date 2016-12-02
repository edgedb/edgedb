##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import decimal
import functools

from edgedb.lang.common import exceptions as edgedb_error
from edgedb.lang.common import fpdecimal

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
            raise edgedb_error.AtomValueError(e.args[0]) from e

        return result

    def __str__(self):
        value = self
        return super(Decimal, value).__str__()

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


class DecimalTypeInfo(s_types.TypeInfo, type=Decimal):
    def op(self, other: (decimal.Decimal, int)) -> 'std::decimal':
        pass

    def unary_op(self) -> 'std::decimal':
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


class Float(float):
    pass


class FloatTypeInfo(s_types.TypeInfo, type=Float):
    def op(self, other: (int, float)) -> 'std::float':
        pass

    def unary_op(self) -> 'std::float':
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


_add_impl('std::float', Float)
_add_map(Float, 'std::float')
_add_map(float, 'std::float')
