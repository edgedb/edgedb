##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import datetime
import dateutil.parser
import dateutil.relativedelta
import dateutil.tz
import re

from semantix.utils import config


@config.configurable
class DateTime(datetime.datetime):
    local_timezone = config.cvalue(type=str, default=None, doc='Default local time-zone')
    local_tz = None

    def __new__(cls, value=None):
        if isinstance(value, datetime.datetime):
            d = value
        elif isinstance(value, str):
            try:
                d = dateutil.parser.parse(value)
            except ValueError as e:
                raise ValueError("invalid value for DateTime object: %s" % value) from e
        else:
            raise ValueError("invalid value for DateTime object: %s" % value)

        tzinfo = d.tzinfo or cls.get_tz()

        return super().__new__(cls, d.year, d.month, d.day, d.hour, d.minute, d.second,
                                    d.microsecond, tzinfo)

    @classmethod
    def get_tz(cls):
        if cls.local_tz is None:
            cls.local_tz = dateutil.tz.gettz(name=cls.local_timezone)
        return cls.local_tz




class TimeDelta(dateutil.relativedelta.relativedelta):
    _interval_tokens = {'year': 'years', 'years': 'years',
                        'month': 'months', 'months': 'months',
                        'week': 'weeks', 'weeks': 'weeks',
                        'day': 'days', 'days': 'days',
                        'hour': 'hours', 'hours': 'hours',
                        'minute': 'minutes', 'minutes': 'minutes',
                        'second': 'seconds', 'seconds': 'seconds',
                        'microsecond': 'microseconds', 'microseconds': 'microseconds'}

    def __new__(cls, value=None, *, dt1=None, dt2=None, years=0, months=0, days=0, leapdays=0,
                      weeks=0, hours=0, minutes=0, seconds=0, microseconds=0, year=None,
                      month=None, day=None, weekday=None, yearday=None, nlyearday=None, hour=None,
                      minute=None, second=None, microsecond=None):

        if value is None:
            result = super().__new__(cls)
            super().__init__(result,
                             dt1=dt1, dt2=dt2, years=years, months=months, days=days,
                             hours=hours, minutes=minutes,
                             seconds=seconds, microseconds=microseconds,
                             leapdays=leapdays, year=year, weeks=weeks,
                             month=month, day=day, weekday=weekday,
                             hour=hour, minute=minute, second=second,
                             microsecond=microsecond)

        elif isinstance(value, dateutil.relativedelta.relativedelta):
            result = super().__new__(cls)
            super().__init__(result,
                             years=value.years, months=value.months, days=value.days,
                             hours=value.hours, minutes=value.minutes,
                             seconds=value.seconds, microseconds=value.microseconds,
                             leapdays=value.leapdays, year=value.year,
                             month=value.month, day=value.day, weekday=value.weekday,
                             hour=value.hour, minute=value.minute,
                             second=value.second, microsecond=value.microsecond)

        elif isinstance(value, datetime.timedelta):
            result = super().__new__(cls)
            super().__init__(result,
                             days=value.days, seconds=value.seconds,
                             microseconds=value.microseconds)

        elif isinstance(value, str):
            expecting_number = True

            intervals = {}
            interval_tokens = cls._interval_tokens

            try:
                number = None

                for token in re.split('\s+', value):
                    if expecting_number:
                        number = int(token)
                    else:
                        interval = interval_tokens[token]
                        if interval in intervals:
                            raise ValueError
                        intervals[interval] = number
                    expecting_number = not expecting_number
                if not expecting_number:
                    raise ValueError
            except (ValueError, KeyError):
                raise ValueError("invalid timedelta value: '%s'" % value)

            result = super().__new__(cls)
            super().__init__(result, **intervals)

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
        return hash((self.weekday, self.year, self.months, self.days, self.hours,
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


class Time(TimeDelta):
    def __new__(cls, value=None):
        if isinstance(value, datetime.time):
            d = value
        elif isinstance(value, str):
            try:
                d = dateutil.parser.parse(value)
            except ValueError as e:
                raise ValueError("invalid value for Time object: %s" % value) from e
        else:
            raise ValueError("invalid value for Time object: %s" % value)

        return super().__new__(cls, hour=d.hour, minute=d.minute, second=d.second,
                                    microsecond=d.microsecond)

    def __str__(self):
        return '%d:%d:%d.%d' % (self.hour, self.minute, self.second, self.microsecond)

    def __repr__(self):
        return "'%d:%d:%d.%d'" % (self.hour, self.minute, self.second, self.microsecond)
