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
import time

from metamagic.utils import config


class DateTimeConfig(metaclass=config.ConfigurableMeta):
    local_timezone = config.cvalue(type=str, default=None, doc='Default local time-zone')


class DateTime(datetime.datetime):
    local_tz = None

    def __new__(cls, value=None, format=None):
        tzinfo = None

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
        if name is None:
            if cls.local_tz is None:
                cls.local_tz = dateutil.tz.gettz(name=DateTimeConfig.local_timezone)
            return cls.local_tz
        else:
            return dateutil.tz.gettz(name)

    @classmethod
    def now(cls, tz=None):
        if not tz:
            tz = cls.get_tz()
        return cls(datetime.datetime.now(tz=tz))

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

    def truncate(self, field):
        # XXX
        raise NotImplementedError


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

    def truncate(self, field):
        # XXX
        raise NotImplementedError


class TimeDelta(dateutil.relativedelta.relativedelta):
    _interval_tokens = {'year': 'years', 'years': 'years',
                        'month': 'months', 'months': 'months',
                        'mon': 'months',
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

                for token in re.split('\s+', value.lower()):
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
            except (ValueError, KeyError) as e:
                raise ValueError("invalid timedelta value: '%s'" % value) from e

            result = super().__new__(cls)
            super().__init__(result, **intervals)

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

    def __sx_serialize__(self):
        return str(self)


class Time(TimeDelta):
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

    def __str__(self):
        return '%d:%d:%d.%d' % (self.hour, self.minute, self.second, self.microsecond)

    def __repr__(self):
        return "'%d:%d:%d.%d'" % (self.hour, self.minute, self.second, self.microsecond)
