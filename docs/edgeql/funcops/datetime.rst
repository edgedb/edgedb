.. _ref_eql_funcops_datetime:


=============
Date and Time
=============

:edb-alt-title: Date/Time Functions and Operators


.. list-table::
    :class: funcoptable

    * - :eql:op:`dt + dt <DTPLUS>`
      - :eql:op-desc:`DTPLUS`

    * - :eql:op:`dt - dt <DTMINUS>`
      - :eql:op-desc:`DTMINUS`

    * - :eql:op:`dt = dt <EQ>`, :eql:op:`dt \< dt <LT>`, ...
      - Comparison operators.

    * - :eql:func:`to_str`
      - Render a date/time value to a string.

    * - :eql:func:`to_datetime`
      - :eql:func-desc:`to_datetime`

    * - :eql:func:`to_local_datetime`
      - :eql:func-desc:`to_local_datetime`

    * - :eql:func:`to_local_date`
      - :eql:func-desc:`to_local_date`

    * - :eql:func:`to_local_time`
      - :eql:func-desc:`to_local_time`

    * - :eql:func:`to_duration`
      - :eql:func-desc:`to_duration`

    * - :eql:func:`datetime_get`
      - :eql:func-desc:`datetime_get`

    * - :eql:func:`time_get`
      - :eql:func-desc:`time_get`

    * - :eql:func:`date_get`
      - :eql:func-desc:`date_get`

    * - :eql:func:`duration_get`
      - :eql:func-desc:`duration_get`

    * - :eql:func:`datetime_trunc`
      - :eql:func-desc:`datetime_trunc`

    * - :eql:func:`duration_trunc`
      - :eql:func-desc:`duration_trunc`

    * - :eql:func:`datetime_current`
      - :eql:func-desc:`datetime_current`

    * - :eql:func:`datetime_of_transaction`
      - :eql:func-desc:`datetime_of_transaction`

    * - :eql:func:`datetime_of_statement`
      - :eql:func-desc:`datetime_of_statement`


----------


.. eql:operator:: DTPLUS: datetime + duration -> datetime
                          local_datetime + duration -> local_datetime
                          local_date + duration -> local_date
                          local_time + duration -> local_time
                          duration + duration -> duration

    Time interval addition.

    This operator is commutative.

    .. code-block:: edgeql-repl

        db> SELECT <local_time>'22:00' + <duration>'1 hour';
        {<local_time>'23:00:00'}
        db> SELECT <duration>'1 hour' + <local_time>'22:00';
        {<local_time>'23:00:00'}
        db> SELECT <duration>'1 hour' + <duration>'2 hours';
        {<duration>'3:00:00'}


----------


.. eql:operator:: DTMINUS: duration - duration -> duration
                           datetime - datetime -> duration
                           local_datetime - local_datetime -> duration
                           local_time - local_time -> duration
                           local_date - local_date -> duration
                           datetime - duration -> datetime
                           local_datetime - duration -> local_datetime
                           local_time - duration -> local_time
                           local_date - duration -> local_date

    Time interval and date/time subtraction.

    .. code-block:: edgeql-repl

        db> SELECT <datetime>'January 01 2019 UTC' -
        ...   <duration>'1 day';
        {<datetime>'2018-12-31T00:00:00+00:00'}
        db> SELECT <datetime>'January 01 2019 UTC' -
        ...   <datetime>'January 02 2019 UTC';
        {<duration>'-1 day, 0:00:00'}
        db> SELECT <duration>'1 hour' -
        ...   <duration>'2 hours';
        {<duration>'-1 day, 23:00:00'}

    It is an error to subtract a date/time object from a time interval:

    .. code-block:: edgeql-repl

        db> SELECT <duration>'1 day' -
        ...   <datetime>'January 01 2019 UTC';
        QueryError: operator '-' cannot be applied to operands ...

    It is also an error to subtract timezone-aware :eql:type:`std::datetime`
    to or from :eql:type:`std::local_datetime`:

    .. code-block:: edgeql-repl

        db> SELECT <datetime>'January 01 2019 UTC' -
        ...   <local_datetime>'January 02 2019';
        QueryError: operator '-' cannot be applied to operands ...


----------

.. eql:function:: std::datetime_current() -> datetime

    Return the current server date and time.

    .. code-block:: edgeql-repl

        db> SELECT datetime_current();
        {'2018-05-14T20:07:11.755827+00:00'}


----------


.. eql:function:: std::datetime_of_transaction() -> datetime

    Return the date and time of the start of the current transaction.


----------


.. eql:function:: std::datetime_of_statement() -> datetime

    Return the date and time of the start of the current statement.


----------


.. eql:function:: std::datetime_get(dt: datetime, el: str) -> float64
                  std::datetime_get(dt: local_datetime, el: str) -> float64

    Extract a specific element of input datetime by name.

    The :eql:type:`datetime` scalar has the following elements
    available for extraction:

    - ``'epoch'`` - the number of seconds since 1970-01-01 00:00:00
      UTC for :eql:type:`datetime` or local time for
      :eql:type:`local_datetime`. It can be negative.
    - ``'century'`` - the century according to the Gregorian calendar
    - ``'day'`` - the day of the month (1-31)
    - ``'decade'`` - the decade (year divided by 10 and rounded down)
    - ``'dow'`` - the day of the week from Sunday (0) to Saturday (6)
    - ``'doy'`` - the day of the year (1-366)
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
    - ``'week'`` - the number of the ISO 8601 week-numbering week of
      the year. ISO weeks are defined to start on Mondays and the
      first week of a year must contain Jan 4 of that year.
    - ``'year'`` - the year

    .. code-block:: edgeql-repl

        db> SELECT datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00',
        ...     'epoch');
        {1525705282.306916}

        db> SELECT datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00',
        ...     'year');
        {2018}

        db> SELECT datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00',
        ...     'quarter');
        {2}

        db> SELECT datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00',
        ...     'doy');
        {127}

        db> SELECT datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00',
        ...     'hour');
        {15}


----------


.. eql:function:: std::time_get(dt: local_time, el: str) -> float64

    Extract a specific element of input time by name.

    The :eql:type:`local_time` scalar has the following elements
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
        ...     <local_time>'15:01:22.306916', 'minute');
        {1}

        db> SELECT time_get(
        ...     <local_time>'15:01:22.306916', 'milliseconds');
        {22306.916}


----------


.. eql:function:: std::date_get(dt: local_date, el: str) -> float64

    Extract a specific element of input date by name.

    Valid elements for :eql:type:`local_date` are the same as for
    :eql:type:`local_datetime` in :eql:func:`datetime_get`.

    .. code-block:: edgeql-repl

        db> SELECT date_get(
        ...     <local_date>'2018-05-07T15:01:22.306916',
        ...     'century');
        {21}

        db> SELECT date_get(
        ...     <local_date>'2018-05-07T15:01:22.306916',
        ...     'year');
        {2018}

        db> SELECT date_get(
        ...     <local_date>'2018-05-07T15:01:22.306916',
        ...     'month');
        {5}

        db> SELECT date_get(
        ...     <local_date>'2018-05-07T15:01:22.306916',
        ...     'doy');
        {127}


----------


.. eql:function:: std::duration_get(dt: duration, el: str) -> float64

    Extract a specific element of input duration by name.

    The :eql:type:`duration` scalar has the following elements
    available for extraction:

    - ``'century'`` - the number of centuries, rounded towards 0
    - ``'day'`` - the number of days
    - ``'decade'`` - the number of decades, rounded towards 0
    - ``'epoch'`` - the total number of seconds in the duration
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
    :eql:type:`duration` does not attempt to automatically convert
    between them. So ``<duration>'24 hours'`` is not necessarily
    the same as ``<duration>'1 day'``. So one must be careful
    when adding or subtracting :eql:type:`duration` values.

    .. code-block:: edgeql-repl

        db> SELECT duration_get(<duration>'24 hours', 'day');
        {0}

        db> SELECT duration_get(<duration>'24 hours', 'hour');
        {24}

        db> SELECT duration_get(<duration>'1 day', 'day');
        {1}

        db> SELECT duration_get(<duration>'1 day', 'hour');
        {0}

        db> SELECT duration_get(
        ...     <duration>'24 hours' - <duration>'1 day', 'hour');
        {24}

        db> SELECT duration_get(
        ...     <duration>'24 hours' - <duration>'1 day', 'day');
        {-1}

    However, ``'epoch'`` calculations assume that 1 day = 24 hours, 1
    month = 30 days and 1 year = 365.25 days or 12 months (depending
    on what is being converted).

    .. code-block:: edgeql-repl

        db> SELECT duration_get(
        ...     <duration>'24 hours' - <duration>'1d', 'epoch');
        {0}

        db> SELECT duration_get(<duration>'1 year', 'epoch');
        {31557600}

        db> SELECT duration_get(<duration>'365.25 days', 'epoch');
        {31557600}

        db> SELECT duration_get(
        ...     <duration>'365 days 6 hours', 'epoch');
        {31557600}


----------


.. eql:function:: std::datetime_trunc(dt: datetime, unit: str) -> datetime

    Truncate the input datetime to a particular precision.

    The valid *unit* values in order or decreasing precision are:

    - ``'microseconds'``
    - ``'milliseconds'``
    - ``'second'``
    - ``'minute'``
    - ``'hour'``
    - ``'day'``
    - ``'week'``
    - ``'month'``
    - ``'quarter'``
    - ``'year'``
    - ``'decade'``
    - ``'century'``
    - ``'millennium'``

    .. code-block:: edgeql-repl

        db> SELECT datetime_trunc(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'year');
        {'2018-01-01T00:00:00+00:00'}

        db> SELECT datetime_trunc(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'quarter');
        {'2018-04-01T00:00:00+00:00'}

        db> SELECT datetime_trunc(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'day');
        {'2018-05-07T00:00:00+00:00'}

        db> SELECT datetime_trunc(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'hour');
        {'2018-05-07T15:00:00+00:00'}


----------


.. eql:function:: std::duration_trunc(dt: duration, unit: str) -> duration

    Truncate the input duration to a particular precision.

    The valid *unit* values are the same as for :eql:func:`datetime_trunc`.

    .. code-block:: edgeql-repl

        db> SELECT duration_trunc(
        ...     <duration>'3 days 15:01:22', 'day');
        {'3 days'}

        db> SELECT duration_trunc(
        ...     <duration>'15:01:22.306916', 'minute');
        {'15:01:00'}

    The usual caveat that :eql:type:`duration` doesn't automatically
    convert units applies to how truncation works.


----------


.. eql:function:: std::to_datetime(s: str, fmt: OPTIONAL str={}) -> datetime
                  std::to_datetime(local: local_datetime, zone: str) \
                    -> datetime
                  std::to_datetime(year: int64, month: int64, day: int64, \
                    hour: int64, min: int64, sec: float64, timezone: str) \
                    -> datetime

    :index: parse datetime

    Create a :eql:type:`datetime` value.

    The :eql:type:`datetime` value can be parsed from the input
    :eql:type:`str` *s*. By default, the input is expected to conform
    to ISO 8601 format. However, the optional argument *fmt* can
    be used to override the :ref:`input format
    <ref_eql_functions_converters_datetime_fmt>` to other forms.

    .. code-block:: edgeql-repl

        db> SELECT to_datetime('2018-05-07T15:01:22.306916+00');
        {<datetime>'2018-05-07T15:01:22.306916+00:00'}
        db> SELECT to_datetime('2018-05-07T15:01:22+00');
        {<datetime>'2018-05-07T15:01:22+00:00'}
        db> SELECT to_datetime('May 7th, 2018 15:01:22 +00',
        ...                    'Mon DDth, YYYY HH24:MI:SS TZM');
        {<datetime>'2018-05-07T15:01:22+00:00'}

    Alternatively, the :eql:type:`datetime` value can be constructed
    from a :eql:type:`std::local_datetime` value:

    .. code-block:: edgeql-repl

        db> SELECT to_datetime(
        ...   <local_datetime>'January 1, 2019 12:00AM', 'HKT');
        {<datetime>'2018-12-31T16:00:00+00:00'}

    Yet another way to construct a the :eql:type:`datetime` value
    is to specify it in terms of its component parts: *year*, *month*,
    *day*, *hour*, *min*, *sec*, and *timezone*

    .. code-block:: edgeql-repl

        db> SELECT to_datetime(
        ...     2018, 5, 7, 15, 1, 22.306916, 'UTC');
        {<datetime>'2018-05-07T15:01:22.306916+00:00'}


------------


.. eql:function:: std::to_local_datetime(s: str, fmt: OPTIONAL str={}) \
                    -> local_datetime
                  std::to_local_datetime(dt: datetime, zone: str) \
                    -> local_datetime
                  std::to_local_datetime(year: int64, month: int64, \
                    day: int64, hour: int64, min: int64, sec: float64) \
                    -> local_datetime

    :index: parse local_datetime

    Create a :eql:type:`local_datetime` value.

    Similar to :eql:func:`to_datetime`, the :eql:type:`local_datetime`
    value can be parsed from the input :eql:type:`str` *s* with an
    optional *fmt* argument or it can be given in terms of its
    component parts: *year*, *month*, *day*, *hour*, *min*, *sec*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_datetime_fmt>`.

    .. code-block:: edgeql-repl

        db> SELECT to_local_datetime('2018-05-07T15:01:22.306916');
        {<local_datetime>'2018-05-07T15:01:22.306916'}
        db> SELECT to_local_datetime('May 7th, 2018 15:01:22',
        ...                          'Mon DDth, YYYY HH24:MI:SS');
        {<local_datetime>'2018-05-07T15:01:22'}
        db> SELECT to_local_datetime(
        ...     2018, 5, 7, 15, 1, 22.306916);
        {<local_datetime>'2018-05-07T15:01:22.306916'}

    A timezone-aware :eql:type:`datetime` type can be converted
    to local datetime in the specified timezone:

    .. code-block:: edgeql-repl

        db> SELECT to_local_datetime(
        ...   <datetime>'December 31, 2018 10:00PM GMT+8',
        ...   'US/Central');
        {<local_datetime>'2019-01-01T00:00:00'}


------------


.. eql:function:: std::to_local_date(s: str, fmt: OPTIONAL str={}) \
                    -> local_date
                  std::to_local_date(dt: datetime, zone: str) -> local_date
                  std::to_local_date(year: int64, month: int64, \
                    day: int64) -> local_date

    :index: parse local_date

    Create a :eql:type:`local_date` value.

    Similar to :eql:func:`to_datetime`, the :eql:type:`local_date`
    value can be parsed from the input :eql:type:`str` *s* with an
    optional *fmt* argument or it can be given in terms of its
    component parts: *year*, *month*, *day*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_datetime_fmt>`.

    .. code-block:: edgeql-repl

        db> SELECT to_local_date('2018-05-07');
        {<local_date>'2018-05-07'}
        db> SELECT to_local_date('May 7th, 2018', 'Mon DDth, YYYY');
        {<local_date>'2018-05-07'}
        db> SELECT to_local_date(2018, 5, 7);
        {<local_date>'2018-05-07'}

    A timezone-aware :eql:type:`datetime` type can be converted
    to local date in the specified timezone:

    .. code-block:: edgeql-repl

        db> SELECT to_local_date(
        ...   <datetime>'December 31, 2018 10:00PM GMT+8',
        ...   'US/Central');
        {<local_date>'2019-01-01'}


------------


.. eql:function:: std::to_local_time(s: str, fmt: OPTIONAL str={}) \
                    -> local_time
                  std::to_local_time(dt: datetime, zone: str) \
                    -> local_time
                  std::to_local_time(hour: int64, min: int64, sec: float64) \
                    -> local_time

    :index: parse local_time

    Create a :eql:type:`local_time` value.

    Similar to :eql:func:`to_datetime`, the :eql:type:`local_time`
    value can be parsed from the input :eql:type:`str` *s* with an
    optional *fmt* argument or it can be given in terms of its
    component parts: *hour*, *min*, *sec*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_datetime_fmt>`.

    .. code-block:: edgeql-repl

        db> SELECT to_local_time('15:01:22.306916');
        {<local_time>'15:01:22.306916'}
        db> SELECT to_local_time('03:01:22pm', 'HH:MI:SSam');
        {<local_time>'15:01:22'}
        db> SELECT to_local_time(15, 1, 22.306916);
        {<local_time>'15:01:22.306916'}

    A timezone-aware :eql:type:`datetime` type can be converted
    to local date in the specified timezone:

    .. code-block:: edgeql-repl

        db> SELECT to_local_time(
        ...   <datetime>'December 31, 2018 10:00PM GMT+8',
        ...   'US/Pacific');
        {<local_date>'22:00:00'}


------------


.. eql:function:: std::to_duration( \
                    NAMED ONLY years: int64=0, \
                    NAMED ONLY months: int64=0, \
                    NAMED ONLY weeks: int64=0, \
                    NAMED ONLY days: int64=0, \
                    NAMED ONLY hours: int64=0, \
                    NAMED ONLY minutes: int64=0, \
                    NAMED ONLY seconds: float64=0 \
                  ) -> duration

    :index: duration

    Create a :eql:type:`duration` value.

    This function uses ``NAMED ONLY`` arguments  to create a
    :eql:type:`duration` value. The available duration fields are:
    *years*, *months*, *weeks*, *days*, *hours*, *minutes*, *seconds*.

    .. code-block:: edgeql-repl

        db> SELECT to_duration(hours := 1,
        ...                    minutes := 20,
        ...                    seconds := 45);
        {<duration>'1:20:45'}
        db> SELECT to_duration(seconds := 4845);
        {<duration>'1:20:45'}
