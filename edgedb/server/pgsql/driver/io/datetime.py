##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import datetime

import postgresql.types
from postgresql.types.io import stdlib_datetime as pg_datetime_io
from postgresql.python.functools import Composition as compose

from metamagic.caos.objects.datetime import TimeDelta, DateTime, Time


def interval_pack(x):
    months, days, seconds, microseconds = x.to_months_days_seconds_microseconds()
    return (months, days, (seconds, microseconds))


def interval_unpack(mds):
    months, days, (seconds, microseconds) = mds
    return TimeDelta(months=months, days=days, seconds=seconds, microseconds=microseconds)


def timestamptz_pack(x,
                     seconds_in_day=pg_datetime_io.seconds_in_day,
                     pg_epoch_datetime_utc=pg_datetime_io.pg_epoch_datetime_utc,
                     UTC=pg_datetime_io.UTC):
    x = x.astimezone(UTC)
    x = datetime.datetime(x.year, x.month, x.day, x.hour, x.minute, x.second, x.microsecond,
                          x.tzinfo)

    x = x - pg_epoch_datetime_utc
    days, seconds, microseconds = x.days, x.seconds, x.microseconds
    return ((days * seconds_in_day) + seconds, microseconds)


def timestamptz_unpack(seconds):
    timestamp = pg_datetime_io.timestamptz_unpack(seconds)
    return timestamp.astimezone(DateTime.get_tz())


oid_to_io = {
    postgresql.types.INTERVALOID: (
        compose((interval_pack, postgresql.types.io.lib.interval64_pack)),
        compose((postgresql.types.io.lib.interval64_unpack, interval_unpack))
    ),

    postgresql.types.TIMESTAMPTZOID: (
        pg_datetime_io.proc_when_not_in(compose((timestamptz_pack,
                                                 postgresql.types.io.lib.time64_pack)),
                                        pg_datetime_io.time64_pack_constants),
        pg_datetime_io.proc_when_not_in(compose((postgresql.types.io.lib.time64_unpack,
                                                 timestamptz_unpack)),
                                        pg_datetime_io.time64_unpack_constants),
        datetime.datetime
    ),
}


oid_to_type = {
    postgresql.types.TIMESTAMPTZOID: DateTime,
    postgresql.types.TIMEOID: Time,
    postgresql.types.INTERVALOID: TimeDelta
}
