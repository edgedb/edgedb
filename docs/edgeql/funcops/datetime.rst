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

    * - :eql:func:`cal::to_local_datetime`
      - :eql:func-desc:`cal::to_local_datetime`

    * - :eql:func:`cal::to_local_date`
      - :eql:func-desc:`cal::to_local_date`

    * - :eql:func:`cal::to_local_time`
      - :eql:func-desc:`cal::to_local_time`

    * - :eql:func:`to_duration`
      - :eql:func-desc:`to_duration`

    * - :eql:func:`datetime_get`
      - :eql:func-desc:`datetime_get`

    * - :eql:func:`cal::time_get`
      - :eql:func-desc:`cal::time_get`

    * - :eql:func:`cal::date_get`
      - :eql:func-desc:`cal::date_get`

    * - :eql:func:`datetime_truncate`
      - :eql:func-desc:`datetime_truncate`

    * - :eql:func:`duration_truncate`
      - :eql:func-desc:`duration_truncate`

    * - :eql:func:`datetime_current`
      - :eql:func-desc:`datetime_current`

    * - :eql:func:`datetime_of_transaction`
      - :eql:func-desc:`datetime_of_transaction`

    * - :eql:func:`datetime_of_statement`
      - :eql:func-desc:`datetime_of_statement`


----------


.. eql:operator:: DTPLUS: datetime + duration -> datetime
                          cal::local_datetime + duration -> cal::local_datetime
                          cal::local_date + duration -> cal::local_date
                          cal::local_time + duration -> cal::local_time
                          duration + duration -> duration

    Time interval addition.

    This operator is commutative.

    .. code-block:: edgeql-repl

        db> SELECT <cal::local_time>'22:00' + <duration>'1 hour';
        {<cal::local_time>'23:00:00'}
        db> SELECT <duration>'1 hour' + <cal::local_time>'22:00';
        {<cal::local_time>'23:00:00'}
        db> SELECT <duration>'1 hour' + <duration>'2 hours';
        {10800s}


----------


.. eql:operator:: DTMINUS: duration - duration -> duration
                           datetime - datetime -> duration
                           cal::local_datetime - cal::local_datetime \
                                -> duration
                           local_time - local_time -> duration
                           local_date - local_date -> duration
                           datetime - duration -> datetime
                           cal::local_datetime - duration \
                                -> cal::local_datetime
                           local_time - duration -> local_time
                           local_date - duration -> local_date

    Time interval and date/time subtraction.

    .. code-block:: edgeql-repl

        db> SELECT <datetime>'2019-01-01T01:02:03+00' -
        ...   <duration>'24 hours';
        {<datetime>'2018-12-31T01:02:03Z'}
        db> SELECT <datetime>'2019-01-01T01:02:03+00' -
        ...   <datetime>'2019-02-01T01:02:03+00';
        {-2678400s}
        db> SELECT <duration>'1 hour' -
        ...   <duration>'2 hours';
        {-3600s}

    It is an error to subtract a date/time object from a time interval:

    .. code-block:: edgeql-repl

        db> SELECT <duration>'1 day' -
        ...   <datetime>'2019-01-01T01:02:03+00';
        QueryError: operator '-' cannot be applied to operands ...

    It is also an error to subtract timezone-aware :eql:type:`std::datetime`
    to or from :eql:type:`cal::local_datetime`:

    .. code-block:: edgeql-repl

        db> SELECT <datetime>'2019-01-01T01:02:03+00' -
        ...   <cal::local_datetime>'2019-02-01T01:02:03';
        QueryError: operator '-' cannot be applied to operands ...


----------

.. eql:function:: std::datetime_current() -> datetime

    Return the current server date and time.

    .. code-block:: edgeql-repl

        db> SELECT datetime_current();
        {<datetime>'2018-05-14T20:07:11.755827Z'}


----------


.. eql:function:: std::datetime_of_transaction() -> datetime

    Return the date and time of the start of the current transaction.


----------


.. eql:function:: std::datetime_of_statement() -> datetime

    Return the date and time of the start of the current statement.


----------


.. eql:function:: std::datetime_get(dt: datetime, el: str) -> float64
                  std::datetime_get(dt: cal::local_datetime, \
                                    el: str) -> float64

    Extract a specific element of input datetime by name.

    The :eql:type:`datetime` scalar has the following elements
    available for extraction:

    - ``'epochseconds'`` - the number of seconds since 1970-01-01 00:00:00
      UTC (Unix epoch) for :eql:type:`datetime` or local time for
      :eql:type:`cal::local_datetime`. It can be negative.
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
    - ``'millennium'`` - the millennium. The third millennium started
      on Jan 1, 2001.
    - ``'milliseconds'`` - the seconds including fractional value expressed
      as milliseconds
    - ``'minutes'`` - the minutes (0-59)
    - ``'month'`` - the month of the year (1-12)
    - ``'quarter'`` - the quarter of the year (1-4)
    - ``'seconds'`` - the seconds, including fractional value from 0 up to and
      not including 60
    - ``'week'`` - the number of the ISO 8601 week-numbering week of
      the year. ISO weeks are defined to start on Mondays and the
      first week of a year must contain Jan 4 of that year.
    - ``'year'`` - the year

    .. code-block:: edgeql-repl

        db> SELECT datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00',
        ...     'epochseconds');
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


.. eql:function:: cal::time_get(dt: cal::local_time, el: str) -> float64

    Extract a specific element of input time by name.

    The :eql:type:`cal::local_time` scalar has the following elements
    available for extraction:

    - ``'midnightseconds'``
    - ``'hour'``
    - ``'microseconds'``
    - ``'milliseconds'``
    - ``'minutes'``
    - ``'seconds'``

    For full description of what these elements extract see
    :eql:func:`datetime_get`.

    .. code-block:: edgeql-repl

        db> SELECT cal::time_get(
        ...     <cal::local_time>'15:01:22.306916', 'minutes');
        {1}

        db> SELECT cal::time_get(
        ...     <cal::local_time>'15:01:22.306916', 'milliseconds');
        {22306.916}


----------


.. eql:function:: cal::date_get(dt: local_date, el: str) -> float64

    Extract a specific element of input date by name.

    The :eql:type:`cal::local_date` scalar has the following elements
    available for extraction:

    - ``'century'`` - the century according to the Gregorian calendar
    - ``'day'`` - the day of the month (1-31)
    - ``'decade'`` - the decade (year divided by 10 and rounded down)
    - ``'dow'`` - the day of the week from Sunday (0) to Saturday (6)
    - ``'doy'`` - the day of the year (1-366)
    - ``'isodow'`` - the ISO day of the week from Monday (1) to Sunday (7)
    - ``'isoyear'`` - the ISO 8601 week-numbering year that the date falls in.
      See the ``'week'`` element for more details.
    - ``'millennium'`` - the millennium. The third millennium started
      on Jan 1, 2001.
    - ``'month'`` - the month of the year (1-12)
    - ``'quarter'`` - the quarter of the year (1-4)
      not including 60
    - ``'week'`` - the number of the ISO 8601 week-numbering week of
      the year. ISO weeks are defined to start on Mondays and the
      first week of a year must contain Jan 4 of that year.
    - ``'year'`` - the year

    .. code-block:: edgeql-repl

        db> SELECT cal::date_get(
        ...     <cal::local_date>'2018-05-07', 'century');
        {21}

        db> SELECT cal::date_get(
        ...     <cal::local_date>'2018-05-07', 'year');
        {2018}

        db> SELECT cal::date_get(
        ...     <cal::local_date>'2018-05-07', 'month');
        {5}

        db> SELECT cal::date_get(
        ...     <cal::local_date>'2018-05-07', 'doy');
        {127}


----------


.. eql:function:: std::datetime_truncate(dt: datetime, unit: str) -> datetime

    Truncate the input datetime to a particular precision.

    The valid *unit* values in order or decreasing precision are:

    - ``'microseconds'``
    - ``'milliseconds'``
    - ``'seconds'``
    - ``'minutes'``
    - ``'hours'``
    - ``'days'``
    - ``'weeks'``
    - ``'months'``
    - ``'quarters'``
    - ``'years'``
    - ``'decades'``
    - ``'centuries'``

    .. code-block:: edgeql-repl

        db> SELECT datetime_truncate(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'years');
        {<datetime>'2018-01-01T00:00:00Z'}

        db> SELECT datetime_truncate(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'quarters');
        {<datetime>'2018-04-01T00:00:00Z'}

        db> SELECT datetime_truncate(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'days');
        {<datetime>'2018-05-07T00:00:00Z'}

        db> SELECT datetime_truncate(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'hours');
        {<datetime>'2018-05-07T15:00:00Z'}


----------


.. eql:function:: std::duration_truncate(dt: duration, unit: str) -> duration

    Truncate the input duration to a particular precision.

    The valid *unit* values are:
    - ``'microseconds'``
    - ``'milliseconds'``
    - ``'seconds'``
    - ``'minutes'``
    - ``'hours'``

    .. code-block:: edgeql-repl

        db> SELECT duration_truncate(
        ...     <duration>'15:01:22', 'hours');
        {54000s}

        db> SELECT duration_truncate(
        ...     <duration>'15:01:22.306916', 'minutes');
        {54060s}


----------


.. eql:function:: std::to_datetime(s: str, fmt: OPTIONAL str={}) -> datetime
                  std::to_datetime(local: cal::local_datetime, zone: str) \
                    -> datetime
                  std::to_datetime(year: int64, month: int64, day: int64, \
                    hour: int64, min: int64, sec: float64, timezone: str) \
                    -> datetime
                  std::to_datetime(epochseconds: decimal) -> datetime
                  std::to_datetime(epochseconds: float64) -> datetime
                  std::to_datetime(epochseconds: int64) -> datetime

    :index: parse datetime

    Create a :eql:type:`datetime` value.

    The :eql:type:`datetime` value can be parsed from the input
    :eql:type:`str` *s*. By default, the input is expected to conform
    to ISO 8601 format. However, the optional argument *fmt* can
    be used to override the :ref:`input format
    <ref_eql_functions_converters_datetime_fmt>` to other forms.

    .. code-block:: edgeql-repl

        db> SELECT to_datetime('2018-05-07T15:01:22.306916+00');
        {<datetime>'2018-05-07T15:01:22.306916Z'}
        db> SELECT to_datetime('2018-05-07T15:01:22+00');
        {<datetime>'2018-05-07T15:01:22Z'}
        db> SELECT to_datetime('May 7th, 2018 15:01:22 +00',
        ...                    'Mon DDth, YYYY HH24:MI:SS TZH');
        {<datetime>'2018-05-07T15:01:22Z'}

    Alternatively, the :eql:type:`datetime` value can be constructed
    from a :eql:type:`cal::local_datetime` value:

    .. code-block:: edgeql-repl

        db> SELECT to_datetime(
        ...   <cal::local_datetime>'2019-01-01T01:02:03', 'HKT');
        {<datetime>'2018-12-31T17:02:03Z'}

    Another way to construct a the :eql:type:`datetime` value
    is to specify it in terms of its component parts: *year*, *month*,
    *day*, *hour*, *min*, *sec*, and *timezone*

    .. code-block:: edgeql-repl

        db> SELECT to_datetime(
        ...     2018, 5, 7, 15, 1, 22.306916, 'UTC');
        {<datetime>'2018-05-07T15:01:22.306916000Z'}

    Finally, it is also possible to convert a Unix timestamp to a
    :eql:type:`datetime`

    .. code-block:: edgeql-repl

        db> SELECT to_datetime(1590595184.584);
        {<datetime>'2020-05-27T15:59:44.584000000Z'}

------------


.. eql:function:: cal::to_local_datetime(s: str, fmt: OPTIONAL str={}) \
                    -> local_datetime
                  cal::to_local_datetime(dt: datetime, zone: str) \
                    -> local_datetime
                  cal::to_local_datetime(year: int64, month: int64, \
                    day: int64, hour: int64, min: int64, sec: float64) \
                    -> local_datetime

    :index: parse local_datetime

    Create a :eql:type:`cal::local_datetime` value.

    Similar to :eql:func:`to_datetime`, the :eql:type:`cal::local_datetime`
    value can be parsed from the input :eql:type:`str` *s* with an
    optional *fmt* argument or it can be given in terms of its
    component parts: *year*, *month*, *day*, *hour*, *min*, *sec*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_datetime_fmt>`.

    .. code-block:: edgeql-repl

        db> SELECT cal::to_local_datetime('2018-05-07T15:01:22.306916');
        {<cal::local_datetime>'2018-05-07T15:01:22.306916'}
        db> SELECT cal::to_local_datetime('May 7th, 2018 15:01:22',
        ...                          'Mon DDth, YYYY HH24:MI:SS');
        {<cal::local_datetime>'2018-05-07T15:01:22'}
        db> SELECT cal::to_local_datetime(
        ...     2018, 5, 7, 15, 1, 22.306916);
        {<cal::local_datetime>'2018-05-07T15:01:22.306916'}

    A timezone-aware :eql:type:`datetime` type can be converted
    to local datetime in the specified timezone:

    .. code-block:: edgeql-repl

        db> SELECT cal::to_local_datetime(
        ...   <datetime>'2018-12-31T22:00:00+08',
        ...   'US/Central');
        {<cal::local_datetime>'2018-12-31T08:00:00'}


------------


.. eql:function:: cal::to_local_date(s: str, fmt: OPTIONAL str={}) \
                    -> local_date
                  cal::to_local_date(dt: datetime, zone: str) -> local_date
                  cal::to_local_date(year: int64, month: int64, \
                    day: int64) -> local_date

    :index: parse local_date

    Create a :eql:type:`cal::local_date` value.

    Similar to :eql:func:`to_datetime`, the :eql:type:`cal::local_date`
    value can be parsed from the input :eql:type:`str` *s* with an
    optional *fmt* argument or it can be given in terms of its
    component parts: *year*, *month*, *day*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_datetime_fmt>`.

    .. code-block:: edgeql-repl

        db> SELECT cal::to_local_date('2018-05-07');
        {<cal::local_date>'2018-05-07'}
        db> SELECT cal::to_local_date('May 7th, 2018', 'Mon DDth, YYYY');
        {<cal::local_date>'2018-05-07'}
        db> SELECT cal::to_local_date(2018, 5, 7);
        {<cal::local_date>'2018-05-07'}

    A timezone-aware :eql:type:`datetime` type can be converted
    to local date in the specified timezone:

    .. code-block:: edgeql-repl

        db> SELECT cal::to_local_date(
        ...   <datetime>'2018-12-31T22:00:00+08',
        ...   'US/Central');
        {<cal::local_date>'2019-01-01'}


------------


.. eql:function:: cal::to_local_time(s: str, fmt: OPTIONAL str={}) \
                    -> local_time
                  cal::to_local_time(dt: datetime, zone: str) \
                    -> local_time
                  cal::to_local_time(hour: int64, min: int64, sec: float64) \
                    -> local_time

    :index: parse local_time

    Create a :eql:type:`cal::local_time` value.

    Similar to :eql:func:`to_datetime`, the :eql:type:`cal::local_time`
    value can be parsed from the input :eql:type:`str` *s* with an
    optional *fmt* argument or it can be given in terms of its
    component parts: *hour*, *min*, *sec*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_datetime_fmt>`.

    .. code-block:: edgeql-repl

        db> SELECT cal::to_local_time('15:01:22.306916');
        {<cal::local_time>'15:01:22.306916'}
        db> SELECT cal::to_local_time('03:01:22pm', 'HH:MI:SSam');
        {<cal::local_time>'15:01:22'}
        db> SELECT cal::to_local_time(15, 1, 22.306916);
        {<cal::local_time>'15:01:22.306916'}

    A timezone-aware :eql:type:`datetime` type can be converted
    to local date in the specified timezone:

    .. code-block:: edgeql-repl

        db> SELECT cal::to_local_time(
        ...   <datetime>'2018-12-31T22:00:00+08',
        ...   'US/Pacific');
        {<cal::local_time>'06:00:00'}


------------


.. eql:function:: std::to_duration( \
                    NAMED ONLY hours: int64=0, \
                    NAMED ONLY minutes: int64=0, \
                    NAMED ONLY seconds: float64=0, \
                    NAMED ONLY microseconds: int64=0 \
                  ) -> duration

    :index: duration

    Create a :eql:type:`duration` value.

    This function uses ``NAMED ONLY`` arguments  to create a
    :eql:type:`duration` value. The available duration fields are:
    *hours*, *minutes*, *seconds*, *microseconds*.

    .. code-block:: edgeql-repl

        db> SELECT to_duration(hours := 1,
        ...                    minutes := 20,
        ...                    seconds := 45);
        {4845s}
        db> SELECT to_duration(seconds := 4845);
        {4845s}


.. eql:function:: std::duration_to_seconds(cur: duration) -> decimal

    Return duration as total number of seconds in interval.

    .. code-block:: edgeql-repl

        db> SELECT duration_to_seconds(<duration>'1 hour');
        {3600.000000n}
        db> SELECT duration_to_seconds(<duration>'10 second 123 ms');
        {10.123000n}
