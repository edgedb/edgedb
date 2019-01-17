.. _ref_eql_functions_datetime:


Date and Time
=============

.. eql:function:: std::datetime_current() -> datetime

    Return the current server date and time.

    .. code-block:: edgeql-repl

        db> SELECT std::datetime_current();
        {'2018-05-14T20:07:11.755827+00:00'}

.. eql:function:: std::datetime_of_transaction() -> datetime

    Return the date and time of the start of the current transaction.

.. eql:function:: std::datetime_of_statement() -> datetime

    Return the date and time of the start of the current statement.

.. eql:function:: std::datetime_get(dt: datetime, el: str) -> float64
                  std::datetime_get(dt: naive_datetime, el: str) -> float64

    Extract a specific element of input datetime by name.

    The :eql:type:`datetime` scalar has the following elements
    available for extraction:

    - ``'century'`` - the century according to the Gregorian calendar
    - ``'day'`` - the day of the month (1-31)
    - ``'decade'`` - the decade (year divided by 10 and rounded down)
    - ``'dow'`` - the day of the week from Sunday (0) to Saturday (6)
    - ``'doy'`` - the day of the year (1-366)
    - ``'epoch'`` - the number of seconds since 1970-01-01 00:00:00
      UTC for :eql:type:`datetime` or local time for
      :eql:type:`naive_datetime`. It can be negative.
    - ``'hour'`` - the hour (0-23)
    - ``'isodow'`` - the ISO day of the week from Monday (1) to Sunday (7)
    - ``'isoyear'`` - the ISO 8601 week-numbering year that the date falls in.
      See the ``'week'`` element for more details.
    - ``'microseconds'`` - the seconds including fractional value expressed
      as microseconds
    - ``'millennium'`` - the millenium. The third millenium started
      on Jan 1, 2001.
    - ``'milliseconds'`` - the seconds including fractional value expressed
      as milliseconds
    - ``'minute'`` - the minutes (0-59)
    - ``'month'`` - the month of the year (1-12)
    - ``'quarter'`` - the quarter of the year (1-4)
    - ``'second'`` - the seconds, including fractional value from 0 up to and
      not including 60
    - ``'timezone'`` - the time zone offset from UTC, measured in seconds
    - ``'timezone_hour'`` - the hour component of time zone offset
    - ``'timezone_minute'`` - the minute component of time zone offset
    - ``'week'`` - the number of the ISO 8601 week-numbering week of
      the year. ISO weeks are defined to start on Mondays and the
      first week of a year must contain Jan 4 of that year.
    - ``'year'`` - the year

    For :eql:type:`naive_datetime` inputs the elements ``'timezone'``,
    ``'timezone_hour'``, and ``'timezone_minute'`` are invalid.

    .. code-block:: edgeql-repl

        db> SELECT datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'year');
        {2018}

        db> SELECT datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'quarter');
        {2}

        db> SELECT datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'doy');
        {127}

        db> SELECT datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'hour');
        {15}


.. eql:function:: std::time_get(dt: naive_time, el: str) -> float64

    Extract a specific element of input time by name.

    The :eql:type:`naive_time` scalar has the following elements
    available for extraction:

    - ``'epoch'``
    - ``'hour'``
    - ``'microseconds'``
    - ``'milliseconds'``
    - ``'minute'``
    - ``'second'``

    For full description of what these elements extract see
    :eql:func:`datetime_get`.

    .. code-block:: edgeql-repl

        db> SELECT time_get(
        ...     <naive_time>'15:01:22.306916', 'minute');
        {1}

        db> SELECT time_get(
        ...     <naive_time>'15:01:22.306916', 'milliseconds');
        {22306.916}

.. eql:function:: std::date_get(dt: naive_date, el: str) -> float64

    Extract a specific element of input date by name.

    Valid elements for :eql:type:`naive_date` are the same as for
    :eql:type:`naive_datetime` in :eql:func:`datetime_get`.

    .. code-block:: edgeql-repl

        db> SELECT date_get(
        ...     <naive_date>'2018-05-07T15:01:22.306916', 'century');
        {21}

        db> SELECT date_get(
        ...     <naive_date>'2018-05-07T15:01:22.306916', 'year');
        {2018}

        db> SELECT date_get(
        ...     <naive_date>'2018-05-07T15:01:22.306916', 'month');
        {5}

        db> SELECT date_get(
        ...     <naive_date>'2018-05-07T15:01:22.306916', 'doy');
        {127}

.. eql:function:: std::timedelta_get(dt: timedelta, el: str) -> float64

    Extract a specific element of input timedelta by name.

    The :eql:type:`timedelta` scalar has the following elements
    available for extraction:

    - ``'century'`` - the number of centuries, rounded towards 0
    - ``'day'`` - the number of days
    - ``'decade'`` - the number of decades, rounded towards 0
    - ``'epoch'`` - the total number of seconds in the timedelta
    - ``'hour'`` - the hour (0-23)
    - ``'microseconds'`` - the seconds including fractional value expressed
      as microseconds
    - ``'millennium'`` - the number of millennia, rounded towards 0
    - ``'milliseconds'`` - the seconds including fractional value expressed
      as milliseconds
    - ``'minute'`` - the minutes (0-59)
    - ``'month'`` - the number of months, modulo 12 (0-11)
    - ``'quarter'`` - the quarter of the year (1-4), based on months
    - ``'second'`` - the seconds, including fractional value from 0 up to and
      not including 60
    - ``'year'`` - the number of years

    Due to inherent ambiguity of counting days, months, and years the
    :eql:type:`timedelta` does not attempt to automatically convert
    between them. So ``<timedelta>'24 hours'`` is not necessarily
    the same as ``<timedelta>'1 day'``. So one must be careful
    when adding or subtracting :eql:type:`timedelta` values.

    .. code-block:: edgeql-repl

        db> SELECT timedelta_get(<timedelta>'24 hours', 'day');
        {0}

        db> SELECT timedelta_get(<timedelta>'24 hours', 'hour');
        {24}

        db> SELECT timedelta_get(<timedelta>'1 day', 'day');
        {1}

        db> SELECT timedelta_get(<timedelta>'1 day', 'hour');
        {0}

        db> SELECT timedelta_get(
        ...     <timedelta>'24 hours' - <timedelta>'1 day', 'hour');
        {24}

        db> SELECT timedelta_get(
        ...     <timedelta>'24 hours' - <timedelta>'1 day', 'day');
        {-1}

    However, ``'epoch'`` calculations assume that 1 day = 24 hours, 1
    month = 30 days and 1 year = 365.25 days or 12 months (depending
    on what is being converted).

    .. code-block:: edgeql-repl

        db> SELECT timedelta_get(
        ...     <timedelta>'24 hours' - <timedelta>'1d', 'epoch');
        {0}

        db> SELECT timedelta_get(<timedelta>'1 year', 'epoch');
        {31557600}

        db> SELECT timedelta_get(<timedelta>'365.25 days', 'epoch');
        {31557600}

        db> SELECT timedelta_get(
        ...     <timedelta>'365 days 6 hours', 'epoch');
        {31557600}
