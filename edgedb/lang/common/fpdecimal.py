##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Fixed-exponent decimal and cascaded decimal context implementation"""


import decimal
import threading

local = threading.local()

if hasattr(local, '__sx_decimal_cascaded_contexts__'):
    del local.__sx_decimal_cascaded_contexts__


class DecimalContext(decimal.Context):
    zero_flags = dict.fromkeys(decimal._signals, 0)

    def __init__(self, prec=None, rounding=None,
                 traps=None, flags=None,
                 Emin=None, Emax=None,
                 capitals=None, _clamp=0,
                 _ignored_flags=None):

        if _ignored_flags is None:
            _ignored_flags = []

        if flags is None:
            flags = self.zero_flags.copy()
        elif not isinstance(flags, dict):
            flags = {s: int(s in flags) for s in decimal._signals}

        if traps is None:
            traps = decimal.DefaultContext.traps.copy()
        elif not isinstance(traps, dict):
            traps = {s: int(s in traps) for s in decimal._signals}

        self.flags = flags
        self._ignored_flags = _ignored_flags
        self.traps = traps
        self.prec = prec if prec is not None else decimal.DefaultContext.prec
        self.rounding = rounding if rounding is not None else decimal.DefaultContext.rounding
        self.Emin = Emin if Emin is not None else decimal.DefaultContext.Emin
        self.Emax = Emax if Emax is not None else decimal.DefaultContext.Emax
        self.capitals = capitals if capitals is not None else decimal.DefaultContext.capitals
        self._clamp = _clamp


class CascadedContext:
    """Fixed-exponent cascaded decimal context

    Cascaded context, as its name implies, allows to perform and nest partial modifications
    to the decimal context.  In other words, cascaded contexts serve as a cumulative diff to
    whatever base context is used for an operation.

    One important distinction from standard decimal context is that CascadedContext forces
    fixed-exponent calculations: Emax is always set to prec - scale - 1, where scale is
    a minimum number of significant fractional digits.  In this regard, in CascadedContext
    decimals behave similarly to SQL standard's decimal type.
    """

    local = local

    def __init__(self, prec=None, rounding=None, traps=None, Emin=None, Emax=None, scale=None,
                                                                        quantize_exponent=None):
        self.prec = prec
        self.rounding = rounding
        if traps is None:
            self.traps = {}
        elif not isinstance(traps, dict):
            self.traps = dict.fromkeys(traps, 1)
        else:
            self.traps = traps.copy()

        self.Emin = Emin
        if prec is not None:
            if scale is not None and prec <= scale:
                raise ValueError('scale must be less than precision')
            emax = prec - (scale or 0) - 1
            if Emax is not None and emax != Emax:
                raise ValueError('scale conflicts with Emax value')
            Emax = emax
        self.Emax = Emax
        self.scale = scale

        if quantize_exponent is None:
            if scale is not None:
                quantize_exponent = decimal.Decimal(10)**-scale

        self.quantize_exponent = quantize_exponent

    def __enter__(self):
        cumulative = self.__class__.push(self)
        self.saved_context = decimal.getcontext()
        context = self.__class__.apply(self.saved_context, cumulative)
        decimal.setcontext(context)
        return context

    def __exit__(self, exc_type, exc_value, traceback):
        self.__class__.pop()
        decimal.setcontext(self.saved_context)

    def increment(self, increment):
        prec = increment.prec if increment.prec is not None else self.prec
        rounding = increment.rounding if increment.rounding is not None else self.rounding
        traps = self.traps.copy()
        traps.update(increment.traps)
        Emin = increment.Emin if increment.Emin is not None else self.Emin

        scale = increment.scale if increment.scale is not None else self.scale
        quantize_exponent = increment.quantize_exponent if increment.quantize_exponent is not None else self.quantize_exponent

        if increment.Emax is None and scale is not None and prec is not None:
            Emax = prec - scale - 1
        else:
            Emax = increment.Emax

        if scale is not None and prec is not None and (prec - scale - 1) < Emax:
            raise ValueError('requested precision is less than existing fixed-point scale')

        result = CascadedContext(prec=prec, rounding=rounding, traps=traps, Emin=Emin, Emax=Emax,
                                 scale=scale, quantize_exponent=quantize_exponent)

        return result

    @classmethod
    def apply(cls, context, cumulative=None):
        if cumulative is None:
            cumulative, last_increment = cls.get()

        traps = context.traps.copy()
        if cumulative.traps:
            traps.update(cumulative.traps)

        if cumulative.prec is not None:
            prec = cumulative.prec
        else:
            prec = context.prec or decimal.DefaultContext.prec

        Emax = prec - (cumulative.scale or 0) - 1 if cumulative.Emax is None else cumulative.Emax

        result = DecimalContext(
                    prec = prec,
                    rounding = cumulative.rounding if cumulative.rounding is not None \
                               else context.rounding,
                    traps = traps,
                    Emin = cumulative.Emin if cumulative.Emin is not None else context.Emin,
                    Emax = Emax
                  )

        return result

    @classmethod
    def get(cls):
        try:
            return cls.local.__sx_decimal_cascaded_contexts__[-1]
        except AttributeError:
            cumulative = CascadedContext()
            incremental = CascadedContext()
            cls.local.__sx_decimal_cascaded_contexts__ = [(cumulative, incremental)]
            return cumulative, incremental

    @classmethod
    def push(cls, increment):
        cumulative, last_increment = cls.get()
        cumulative = cumulative.increment(increment)
        cls.local.__sx_decimal_cascaded_contexts__.append((cumulative, increment))
        return cumulative

    @classmethod
    def pop(cls):
        contexts = getattr(cls.local, '__sx_decimal_cascaded_contexts__', None)
        if contexts and len(contexts) > 1:
            contexts.pop()


del threading, local


class FPDecimal(decimal.Decimal):
    """Fixed-point decimal

    Fixed-point decimals interpret prec as a number of significant digits around decimal point.
    The point is always fixed, E.g Emax is always set to the number of significant digits of
    integer part minus one. An optional scale variable in the context determines the number of
    significant fractional digits.  FPDecimal automatically quantizes its value when constructed.
    """

    def __new__(cls, value):
        result = decimal.Decimal.__new__(decimal.Decimal, value)
        # Check and enforce context requirements
        cumulative, last_increment = CascadedContext.get()
        context = CascadedContext.apply(decimal.getcontext())

        if cumulative.quantize_exponent is not None:
            try:
                result = result.quantize(cumulative.quantize_exponent, context=context)
            except decimal.InvalidOperation as e:
                raise decimal.Overflow from e
        else:
            result = result._fix(context)

        return super().__new__(cls, result)

    def __str__(self):
        return super().__str__()
