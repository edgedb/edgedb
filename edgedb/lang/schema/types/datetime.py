##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import datetime

from metamagic.utils import ast

from metamagic.caos import error as caos_exc
from metamagic.caos.lang.caosql import quote as caosql_quote

from metamagic.utils.datetime import DateTime, Date, Time, TimeDelta
from metamagic.utils.algos.persistent_hash import persistent_hash

from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping

_add_map(DateTime, 'metamagic.caos.builtins.datetime')


class DateTime(DateTime):
    def __new__(cls, value=None, format=None):
        try:
            return super().__new__(cls, value, format=format)
        except ValueError as e:
            raise caos_exc.AtomValueError(e.args[0]) from e

    def __sub__(self, other):
        if isinstance(other, datetime.datetime):
            return TimeDelta(dt1=datetime.datetime(self.year, self.month, self.day, self.hour,
                                                   self.minute, self.second, self.microsecond,
                                                   self.tzinfo),
                             dt2=datetime.datetime(other.year, other.month, other.day, other.hour,
                                                   other.minute, other.second, other.microsecond,
                                                   other.tzinfo))
        else:
            return NotImplemented

    def abssub(self, other):
        # get the precise delta instead of the fuzzy years/months/days relative delta
        dt1=datetime.datetime(self.year, self.month, self.day, self.hour,
                              self.minute, self.second, self.microsecond,
                              self.tzinfo)
        dt2=datetime.datetime(other.year, other.month, other.day, other.hour,
                              other.minute, other.second, other.microsecond,
                              other.tzinfo)
        return dt1 - dt2

    def __add__(self, other):
        result = super().__add__(other)

        if result is not NotImplemented:
            return DateTime(result)
        else:
            return result

    def __str__(self):
        # ISO 8601 makes the most sense as a default representation
        if self.microsecond:
            return self.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        else:
            return self.strftime('%Y-%m-%dT%H:%M:%S%z')

_add_map(DateTime, 'metamagic.caos.builtins.datetime')
_add_impl('metamagic.caos.builtins.datetime', DateTime)


class Date(Date):
    def __new__(cls, value=None, format=None):
        try:
            return super().__new__(cls, value, format=format)
        except ValueError as e:
            raise caos_exc.AtomValueError(e.args[0]) from e

    def __str__(self):
        # ISO 8601 makes the most sense as a default representation
        return self.strftime('%Y-%m-%d')

_add_impl('metamagic.caos.builtins.date', Date)
_add_map(Date, 'metamagic.caos.builtins.date')


_add_map(Time, 'metamagic.caos.builtins.time')


class Time(Time):
    def __new__(cls, value=None, *, format=None):
        try:
            return super().__new__(cls, value, format=format)
        except ValueError as e:
            raise caos_exc.AtomValueError(e.args[0]) from e

_add_impl('metamagic.caos.builtins.time', Time)
_add_map(Time, 'metamagic.caos.builtins.time')


_add_map(TimeDelta, 'metamagic.caos.builtins.timedelta')


class TimeDelta(TimeDelta):
    def __new__(cls, value=None, *, dt1=None, dt2=None, years=0, months=0, days=0, leapdays=0,
                      weeks=0, hours=0, minutes=0, seconds=0, microseconds=0, year=None,
                      month=None, day=None, weekday=None, yearday=None, nlyearday=None, hour=None,
                      minute=None, second=None, microsecond=None):
        try:
            return super().__new__(cls, value, dt1=dt1, dt2=dt2, years=years,
                                   months=months, days=days,
                                   hours=hours, minutes=minutes,
                                   seconds=seconds, microseconds=microseconds,
                                   leapdays=leapdays, year=year, weeks=weeks,
                                   month=month, day=day, weekday=weekday,
                                   hour=hour, minute=minute, second=second,
                                   microsecond=microsecond)
        except ValueError as e:
            raise caos_exc.AtomValueError(e.args[0]) from e

    def __mm_caosql__(self):
        return 'cast ({} as [metamagic.caos.builtins.timedelta])'\
                    .format(caosql_quote.quote_literal(str(self)))

    def persistent_hash(self):
        return persistent_hash((self.weekday, self.years, self.months, self.days, self.hours,
                                self.minutes, self.seconds, self.microseconds,
                                self.leapdays, self.year, self.month, self.day, self.hour,
                                self.minute, self.second, self.microsecond))

    def format_value(self, value, template):
        if value:
            return '%d %s%s' % (value, template, 's' if value % 10 != 1 else '')
        else:
            return ''

    def format_MMDDH(self, m, d, h):
        MM = self.format_value(m, 'month')
        DD = self.format_value(d, 'day')
        H = self.format_value(h, 'hour')

        return ' '.join(filter(None, (MM, DD, H)))

    def format_atom(self, format):
        m, d, s, us = self.to_months_days_seconds_microseconds()

        items = {
            'y': self.years,
            'Y': self.format_value(self.years, 'year'),
            'm': self.months,
            'M': self.format_value(self.months, 'month'),
            'mm': m,
            'MM': self.format_value(m, 'month'),
            'd': self.days,
            'D': self.format_value(self.days, 'day'),
            'dd': d,
            'DD': self.format_value(d, 'day'),
            'h': self.hours,
            'H': self.format_value(self.hours, 'hour'),
            'i': self.minutes,
            'I': self.format_value(self.minutes, 'minute'),
            's': self.seconds,
            'S': self.format_value(self.seconds, 'second'),
            'c': self.microseconds,
            'C': self.format_value(self.microseconds, 'microsecond'),
            'MMDDH': self.format_MMDDH(m, d, self.hours)
        }

        return format.format(**items)

_add_map(TimeDelta, 'metamagic.caos.builtins.timedelta')
_add_impl('metamagic.caos.builtins.timedelta', TimeDelta)

s_types.TypeRules.add_rule(ast.ops.ADD, (DateTime, DateTime), 'metamagic.caos.builtins.datetime')
s_types.TypeRules.add_rule(ast.ops.ADD, (DateTime, Time), 'metamagic.caos.builtins.datetime')
s_types.TypeRules.add_rule(ast.ops.ADD, (Time, DateTime), 'metamagic.caos.builtins.datetime')

s_types.TypeRules.add_rule(ast.ops.SUB, (DateTime, DateTime), 'metamagic.caos.builtins.timedelta')
