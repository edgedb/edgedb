##
# Copyright (c) 2008-2010, 2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import decimal
import math
import re
import time

import datetime
import dateutil.parser
import dateutil.relativedelta
import dateutil.tz


class DateTimeConfig:
    local_timezone = None


class DateTime(datetime.datetime):
    local_tz = None

    def __new__(cls, value=None, format=None):
        tzinfo = None

        if isinstance(value, int):
            # Unitx timestamp (UTC)
            value = datetime.datetime.utcfromtimestamp(value)
            value = value.replace(tzinfo=datetime.timezone.utc)

        if isinstance(value, datetime.datetime):
            args = [value.year, value.month, value.day, value.hour, value.minute, value.second,
                    value.microsecond]
            tzinfo = value.tzinfo
        elif isinstance(value, time.struct_time):
            args = [value.tm_year, value.tm_mon, value.tm_mday, value.tm_hour, value.tm_min,
                    value.tm_sec, 0]
        elif isinstance(value, datetime.date):
            args = [value.year, value.month, value.day, 0, 0, 0, 0]
        elif isinstance(value, str):
            dt = None

            if format is not None:
                if isinstance(format, str):
                    format = (format,)

                for f in format:
                    try:
                        dt = datetime.datetime.strptime(value, f)
                    except ValueError:
                        pass
                    else:
                        break
                else:
                    raise ValueError("invalid value for DateTime object: %s" % value)
            else:
                try:
                    dt = dateutil.parser.parse(value, tzinfos=cls.get_tz)
                except ValueError as e:
                    raise ValueError("invalid value for DateTime object: %s" % value) from e

            args = [dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond]
            tzinfo = dt.tzinfo
        else:
            raise ValueError("invalid value for DateTime object: %s" % value)

        if tzinfo is None:
            tzinfo = cls.get_tz()

        args.append(tzinfo)
        return super().__new__(cls, *args)

    @classmethod
    def get_tz(cls, name=None, offset=None):
        if name is None and offset is None:
            if cls.local_tz is None:
                cls.local_tz = dateutil.tz.gettz(name=DateTimeConfig.local_timezone)
            return cls.local_tz
        elif offset is not None:
            return dateutil.tz.tzoffset(name, offset)
        else:
            return dateutil.tz.gettz(name)

    @classmethod
    def now(cls, tz=None):
        if not tz:
            tz = cls.get_tz()
        return cls(datetime.datetime.now(tz=tz))

    def astimezone(self, tz=None):
        return self.__class__(super().astimezone(tz))

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

    def __add__(self, other):
        result = super().__add__(other)

        if result is not NotImplemented:
            return DateTime(result)
        else:
            return result

    def _truncate_to_milliseconds(self):
        return self.replace(microsecond=self.microsecond // 1000 * 1000)

    def _truncate_to_week(self):
        def _iso_calendar_to_date(iso_year, iso_week, iso_day):
            jan4 = datetime.date(iso_year, 1, 4)
            delta = datetime.timedelta(jan4.isoweekday() - 1)
            return jan4 - delta + datetime.timedelta(days=iso_day - 1, weeks=iso_week-1)

        year, week, weekday = self.isocalendar()

        d = _iso_calendar_to_date(year, week, 1)

        return self.replace(year=d.year, month=d.month, day=d.day)

    def _truncate_to_quarter(self):
        return self.replace(month=((self.month - 1) // 3) * 3 + 1)

    def _truncate_to_decade(self):
        return self.replace(year=self.year // 10 * 10)

    def _truncate_to_century(self):
        return self.replace(year=self.year // 100 * 100 + 1)

    def _truncate_to_millennium(self):
        return self.replace(year=self.year // 1000 * 1000 + 1)

    _field_base = {
        'microsecond': 0,
        'second': 0,
        'minute': 0,
        'hour': 0,
        'day': 1,
        'month': 1,
        'year': 1
    }

    _trunc_map = {
        'microsecond': {
        },
        'millisecond': {
            'fields': [],
            'postproc': _truncate_to_milliseconds
        },
        'second': {
            'fields': ['microsecond']
        },
        'minute': {
            'fields': ['microsecond', 'second']
        },
        'hour': {
            'fields': ['microsecond', 'second', 'minute']
        },
        'day': {
            'fields': ['microsecond', 'second', 'minute', 'hour']
        },
        'week': {
            'fields': ['microsecond', 'second', 'minute', 'hour'],
            'postproc': _truncate_to_week
        },
        'month': {
            'fields': ['microsecond', 'second', 'minute', 'hour', 'day']
        },
        'quarter': {
            'fields': ['microsecond', 'second', 'minute', 'hour', 'day'],
            'postproc': _truncate_to_quarter
        },
        'year': {
            'fields': ['microsecond', 'second', 'minute', 'hour', 'day', 'month']
        },
        'decade': {
            'fields': ['microsecond', 'second', 'minute', 'hour', 'day', 'month'],
            'postproc': _truncate_to_decade
        },
        'century': {
            'fields': ['microsecond', 'second', 'minute', 'hour', 'day', 'month'],
            'postproc': _truncate_to_century
        },
        'millennium': {
            'fields': ['microsecond', 'second', 'minute', 'hour', 'day', 'month'],
            'postproc': _truncate_to_millennium
        },
    }

    def truncate(self, precision):
        truncated = self.astimezone(datetime.timezone.utc)

        trunc_def = self._trunc_map.get(precision)
        if trunc_def is None:
            raise ValueError('DateTime.truncate: invalid precision: {}'.format(precision))

        fields = trunc_def.get('fields')
        postproc = trunc_def.get('postproc')

        if fields:
            replace_fields = {field: self._field_base[field] for field in fields}
            truncated = truncated.replace(**replace_fields)

        if postproc:
            truncated = postproc(truncated)

        if not isinstance(truncated, self.__class__):
            truncated = self.__class__(truncated)

        return truncated


class Date(datetime.date):
    def __new__(cls, value=None, format=None):
        if isinstance(value, datetime.date):
            args = [value.year, value.month, value.day]
        elif isinstance(value, time.struct_time):
            args = [value.tm_year, value.tm_mon, value.tm_mday]
        elif isinstance(value, str):
            dt = None

            if format is not None:
                if isinstance(format, str):
                    format = (format,)

                for f in format:
                    try:
                        dt = datetime.datetime.strptime(value, f)
                    except ValueError:
                        pass
                    else:
                        break
                else:
                    raise ValueError("invalid value for Date object: %s" % value)
            else:
                try:
                    dt = dateutil.parser.parse(value)
                except ValueError as e:
                    raise ValueError("invalid value for Date object: %s" % value) from e

            args = [dt.year, dt.month, dt.day]
        else:
            raise ValueError("invalid value for Date object: %s" % value)

        return super().__new__(cls, *args)

    @classmethod
    def now(cls, tz=None):
        now = DateTime.now(tz=tz)
        return cls(datetime.date(year=now.year, month=now.month, day=now.day))

    def truncate(self, field):
        # XXX
        raise NotImplementedError


class TimeDelta(dateutil.relativedelta.relativedelta):
    _verbose_units = {
        'microsecond',
        'millisecond',
        'second',
        'minute',
        'hour',
        'day',
        'week',
        'month',
        'year'
    }

    _verbose_units_plural = {u + 's' for u in _verbose_units}

    _verbose_units_map = dict(
        zip(_verbose_units_plural, _verbose_units_plural),
        **{u: u + 's' for u in _verbose_units})

    _verbose_units_re = \
        '(?:' + '|'.join('(' + u + ')' + '(?:s)?' for u in _verbose_units) + ')'

    _parse_re = re.compile(
        '^' +
            '(?:(?:P)' +
                # PnnYnnWnnMnnDTnnHnnMnnS
                '(' +
                    r'((?:(?:\d+(?:\.\d+)?)(?:[YMDW]))*)' +
                    r'(?:T((?:(?:\d+(?:\.\d+)?)(?:[HMS]))+))?' +
                ')' +
            ')' +
                '|' +
                # 1 year(s) 3 day(s) ... etc.
            '(' +
                r'(?:(-?\d+(?:\.\d+)?)\s*' + _verbose_units_re + ')' +
                r'(?:\s+(-?\d+(?:\.\d+)?)\s*' + _verbose_units_re + ')*' +
            ')' +
        '$',
        re.I
    )

    def __new__(cls, value=None, *, dt1=None, dt2=None, years=0, months=0,
                     days=0, leapdays=0, weeks=0, hours=0, minutes=0,
                     seconds=0, microseconds=0, year=None, month=None,
                     day=None, weekday=None, yearday=None, nlyearday=None,
                     hour=None, minute=None, second=None, microsecond=None):

        if value is None:
            result = super().__new__(cls)
            super().__init__(result,
                             dt1=dt1, dt2=dt2, years=years, months=months,
                             days=days, hours=hours, minutes=minutes,
                             seconds=seconds, microseconds=microseconds,
                             leapdays=leapdays, year=year, weeks=weeks,
                             month=month, day=day, weekday=weekday,
                             hour=hour, minute=minute, second=second,
                             microsecond=microsecond)

        elif isinstance(value, dateutil.relativedelta.relativedelta):
            result = super().__new__(cls)
            super().__init__(result,
                             years=value.years, months=value.months,
                             days=value.days, hours=value.hours,
                             minutes=value.minutes, seconds=value.seconds,
                             microseconds=value.microseconds,
                             leapdays=value.leapdays, year=value.year,
                             month=value.month, day=value.day,
                             weekday=value.weekday, hour=value.hour,
                             minute=value.minute, second=value.second,
                             microsecond=value.microsecond)

        elif isinstance(value, datetime.timedelta):
            result = super().__new__(cls)
            super().__init__(result,
                             days=value.days, seconds=value.seconds,
                             microseconds=value.microseconds)

        elif isinstance(value, str):
            match = cls._parse_re.match(value)

            if not match:
                msg = "invalid timedelta value: {!r}".format(value)
                raise ValueError(msg)

            quantities = {}
            try:
                if value[0] == 'P':
                    if match.group(2):
                        cls._parse_quantities(
                            match.group(2), quantities,
                            {'y': 'years', 'm': 'months',
                             'w': 'weeks', 'd': 'days'}
                        )

                    if match.group(3):
                        cls._parse_quantities(
                            match.group(3), quantities,
                            {'h': 'hours', 'm': 'minutes',
                             's': 'seconds'}
                        )
                else:
                    # Verbose format
                    cls._parse_quantities(match.group(4), quantities,
                                          cls._verbose_units_map, True)

            except ValueError:
                msg = "invalid timedelta value: {!r}".format(value)
                raise ValueError(msg) from None

            cls._fix_quantities(quantities)

            result = super().__new__(cls)
            super().__init__(result, **quantities)

        else:
            raise ValueError("invalid timedelta value: '%s'" % value)

        return result

    def __init__(self, *args, **kwargs):
        # We initialize in __new__, so nothing to do here
        pass

    def __str__(self):
        s = []
        for k in ('years', 'months', 'days', 'hours', 'minutes', 'seconds', 'microseconds'):
            v = getattr(self, k)
            if v:
                s.append('%s %s' % (v, k if v != 1 else k[:-1]))
        return ' '.join(s)

    def __hash__(self):
        return hash((self.weekday, self.years, self.months, self.days, self.hours,
                     self.minutes, self.seconds, self.leapdays, self.year, self.month,
                     self.day, self.hour, self.minute, self.second, self.microsecond))

    def __gt__(self, other):
        if not isinstance(other, TimeDelta):
            return NotImplemented

        if self.weekday or other.weekday:
            raise TypeError('TimeDeltas with non-empty weekday are not orderable')

        return self.years > other.years or self.years == other.years and \
               (self.months > other.months or self.months == other.months and \
               (self.days > other.days or self.days == other.days and \
               (self.hours > other.hours or self.hours == other.hours and \
               (self.minutes > other.minutes or self.minutes == other.minutes and \
               (self.seconds > other.seconds or self.seconds == other.seconds and \
               (self.microseconds > other.microseconds))))))

    def __ge__(self, other):
        return self == other or self > other

    def __lt__(self, other):
        return not self >= other

    def __le__(self, other):
        return not self > other

    def to_months_days_seconds_microseconds(self):
        months = self.months + self.years * 12
        days = self.days
        seconds = self.seconds + self.minutes * 60 + self.hours * 3600
        microseconds = self.microseconds
        return months, days, seconds, microseconds

    def _time_part(self):
        return self.hours * 3600 + self.minutes * 60 + self.seconds, self.microseconds

    def reduce(self, repr='days'):
        """
        Reduce timedelta to a single floating point number of a given denomination
        """

        seconds, us = self._time_part()
        day_fraction = seconds / 86400
        days = self.years * 365 + self.months * 30 + self.days + self.leapdays

        auto_repr = repr == 'auto'

        if auto_repr:
            if self.microseconds:
                repr = 'seconds'
            elif self.seconds:
                repr = 'seconds'
            elif self.minutes:
                repr = 'minutes'
            elif self.hours:
                repr = 'hours'
            elif self.days:
                repr = 'days'
            elif self.months:
                repr = 'months'
            else:
                repr = 'years'

        if repr == 'years':
            res = self.years + (self.months + (self.leapdays + self.days + day_fraction) / 30) / 12
        elif repr == 'months':
            res = self.years * 12 + self.months + ((self.leapdays + self.days + day_fraction) / 30)
        elif repr == 'days':
            res = days + day_fraction
        elif repr == 'hours':
            res = days * 24 + seconds / 3600
        elif repr == 'minutes':
            res = days * 1440 + seconds / 60
        elif repr == 'seconds':
            res = days * 1440 * 60 + seconds + us / (10 ** -6)
        else:
            raise ValueError('unsupported representation mode for reduce(): %s' % repr)

        if auto_repr:
            return res, repr

        return res

    def __mm_serialize__(self):
        return str(self)

    @classmethod
    def _fix_quantities(cls, quantities):
        seconds = quantities.get('seconds')
        if seconds:
            sec_fraction = seconds - math.floor(seconds)
            if sec_fraction:
                ms = quantities.get('microseconds') or 0
                quantities['microseconds'] = ms + sec_fraction * 1000000

        for unit, quantity in quantities.items():
            quantities[unit] = int(quantity)

    @classmethod
    def _parse_quantities(cls, value, quantities, units,
                               allow_whitespace=False):
        re_src = ''

        if allow_whitespace:
            re_src += r'\s*'
        re_src += r'(-?\d+(?:\.\d+)?)'
        if allow_whitespace:
            re_src += r'\s*'

        units_sorted = sorted(units, key=lambda u: -len(u))
        re_src += '(' + '|'.join(units_sorted) + ')'

        expr = re.compile(re_src, re.I)
        pos = 0

        while True:
            match = expr.match(value, pos)
            if not match:
                break

            quantity = decimal.Decimal(match.group(1))
            unit = match.group(2).lower()

            if unit not in units:
                raise ValueError('unexpected unit in interval spec: ' + unit)

            unit = units[unit]

            if unit in quantities:
                raise ValueError('repeated unit in interval spec: ' + unit)

            quantities[unit] = quantity
            pos += len(match.group(0))


class Time(datetime.time):
    def __new__(cls, value=None, *, format=None):
        if isinstance(value, (Time, datetime.time)):
            d = value
        elif isinstance(value, str):
            if format is not None:
                if isinstance(format, str):
                    format = (format,)

                for f in format:
                    try:
                        d = datetime.datetime.strptime(value, f)
                    except ValueError:
                        pass
                    else:
                        break
                else:
                    raise ValueError("invalid value for Time object: %s" % value)
            else:
                try:
                    d = dateutil.parser.parse(value)
                except ValueError as e:
                    raise ValueError("invalid value for Time object: %s" % value) from e
        else:
            raise ValueError("invalid value for Time object: %s" % value)

        return super().__new__(cls, hour=d.hour, minute=d.minute, second=d.second,
                                    microsecond=d.microsecond)


class IANATimeZone(datetime.tzinfo):
    _re = re.compile(r'^\w[\w-]*(/\w[\w-]*)?$')

    def __init__(self, name):
        if not self._re.match(name):
            raise ValueError("invalid timezone name: {!r}".format(name))

        self._name = name
        self._tz = dateutil.tz.gettz(name)

        if self._tz is None:
            raise ValueError("invalid timezone name: {!r}".format(name))

    @property
    def name(self):
        return self._name

    def utcoffset(self, dt):
        return self._tz.utcoffset(dt)

    def dst(self, dt):
        return self._tz.dst(dt)

    def tzname(self, dt):
        return self._tz.tzname(dt)

    def __eq__(self, other):
        if not isinstance(other, IANATimeZone):
            return False
        else:
            return self._name == other._name and self._tz == other._tz

    def __repr__(self):
        return '<{} {!r}>'.format(self.__class__.__name__, self.name)
