.. _ref_std_datetime:


========
Temporal
========

:edb-alt-title: Temporal Types, Functions, and Operators

.. list-table::
    :class: funcoptable

    * - :eql:type:`datetime`
      - Timezone-aware point in time

    * - :eql:type:`duration`
      - Absolute time span

    * - :eql:type:`cal::local_datetime`
      - Date and time w/o timezone

    * - :eql:type:`cal::local_date`
      - Date type

    * - :eql:type:`cal::local_time`
      - Time type

    * - :eql:type:`cal::relative_duration`
      - Relative time span

    * - :eql:op:`dt + dt <dtplus>`
      - :eql:op-desc:`dtplus`

    * - :eql:op:`dt - dt <dtminus>`
      - :eql:op-desc:`dtminus`

    * - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
        :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
        :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
      - Comparison operators

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

    * - :eql:func:`cal::to_relative_duration`
      - :eql:func-desc:`cal::to_relative_duration`

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

.. _ref_std_datetime_intro:

EdgeDB has two classes of date/time types:

* a timezone-aware :eql:type:`std::datetime` type;

* a set of "local" date/time objects, not attached to any particular
  timezone: :eql:type:`cal::local_datetime`, :eql:type:`cal::local_date`,
  and :eql:type:`cal::local_time`.

There are also two different ways of measuring duration:

* :eql:type:`duration` using absolute and unambiguous units;

* :eql:type:`cal::relative_duration` using fuzzy units like years,
  months and days in addition to the absolute units.

All date/time operators and functions and type casts are designed to
maintain a strict separation between timezone-aware and "local"
date/time values.

EdgeDB stores and outputs timezone-aware values in UTC.

.. note::

    All the date/time types are restricted to years between 1 and
    9999, including the end points.

    Although many systems support ISO 8601 date formatting in theory,
    in practice the formatting before year 1 and after 9999 tends to
    be inconsistent. As such dates outside that range are not reliably
    portable.


----------


.. eql:type:: std::datetime


    A timezone-aware type representing a moment in time.

    All dates must correspond to dates that exist in the proleptic Gregorian
    calendar.

    :eql:op:`Casting <cast>` is a simple way to obtain a
    :eql:type:`datetime` value in an expression:

    .. code-block:: edgeql

        select <datetime>'2018-05-07T15:01:22.306916+00';
        select <datetime>'2018-05-07T15:01:22+00';

    Note that when casting from strings, the string should be in ISO
    8601 format with timezone included:

    .. code-block:: edgeql-repl

        db> select <datetime>'January 01 2019 UTC';
        InvalidValueError: invalid input syntax for type
        std::datetime: 'January 01 2019 UTC'
        Hint: Please use ISO8601 format. Alternatively "to_datetime"
        function provides custom formatting options.

        db> select <datetime>'2019-01-01T15:01:22';
        InvalidValueError: invalid input syntax for type
        std::datetime: '2019-01-01T15:01:22'
        Hint: Please use ISO8601 format. Alternatively "to_datetime"
        function provides custom formatting options.

    All ``datetime`` values are restricted to the range from year 1 to 9999.

    See functions :eql:func:`datetime_get`, :eql:func:`to_datetime`,
    and :eql:func:`to_str` for more ways of working with
    :eql:type:`datetime`.




----------


.. eql:type:: cal::local_datetime

    A type representing date and time without time zone.

    :eql:op:`Casting <cast>` is a simple way to obtain a
    :eql:type:`cal::local_datetime` value in an expression:

    .. code-block:: edgeql

        select <cal::local_datetime>'2018-05-07T15:01:22.306916';
        select <cal::local_datetime>'2018-05-07T15:01:22';

    Note that when casting from strings, the string should be in ISO
    8601 format without timezone:

    .. code-block:: edgeql-repl

        db> select <cal::local_datetime>'2019-01-01T15:01:22+00';
        InvalidValueError: invalid input syntax for type
        cal::local_datetime: '2019-01-01T15:01:22+00'
        Hint: Please use ISO8601 format. Alternatively
        "cal::to_local_datetime" function provides custom formatting
        options.

        db> select <cal::local_datetime>'January 01 2019';
        InvalidValueError: invalid input syntax for type
        cal::local_datetime: 'January 01 2019'
        Hint: Please use ISO8601 format. Alternatively
        "cal::to_local_datetime" function provides custom formatting
        options.

    All ``datetime`` values are restricted to the range from year 1 to 9999.

    See functions :eql:func:`datetime_get`, :eql:func:`cal::to_local_datetime`,
    and :eql:func:`to_str` for more ways of working with
    :eql:type:`cal::local_datetime`.


----------


.. eql:type:: cal::local_date

    A type representing a date without a time zone.

    :eql:op:`Casting <cast>` is a simple way to obtain a
    :eql:type:`cal::local_date` value in an expression:

    .. code-block:: edgeql

        select <cal::local_date>'2018-05-07';

    Note that when casting from strings, the string should be in ISO
    8601 date format.

    See functions :eql:func:`cal::date_get`, :eql:func:`cal::to_local_date`,
    and :eql:func:`to_str` for more ways of working with
    :eql:type:`cal::local_date`.


----------


.. eql:type:: cal::local_time

    A type representing time without a time zone.

    :eql:op:`Casting <cast>` is a simple way to obtain a
    :eql:type:`cal::local_time` value in an expression:

    .. code-block:: edgeql

        select <cal::local_time>'15:01:22.306916';
        select <cal::local_time>'15:01:22';

    Note that when casting from strings, the string should be in ISO
    8601 time format.

    See functions :eql:func:`cal::time_get`, :eql:func:`cal::to_local_time`,
    and :eql:func:`to_str` for more ways of working with
    :eql:type:`cal::local_time`.


----------



.. _ref_datetime_duration:

.. eql:type:: std::duration

    A type representing a span of time.

    Valid units when converting from a string (and combinations of them):
    - ``'microseconds'``
    - ``'milliseconds'``
    - ``'seconds'``
    - ``'minutes'``
    - ``'hours'``

    .. code-block:: edgeql

        select <duration>'45.6 seconds';
        select <duration>'15 milliseconds';
        select <duration>'48 hours 45 minutes';
        select <duration>'-7 minutes';

    All date/time types support the ``+`` and ``-`` arithmetic operations
    with durations:

    .. code-block:: edgeql-repl

        db> select <datetime>'2019-01-01T00:00:00Z' - <duration>'24 hours';
        {<datetime>'2018-12-31T00:00:00+00:00'}
        db> select <cal::local_time>'22:00' + <duration>'1 hour';
        {<cal::local_time>'23:00:00'}

    Duration is a fixed number of seconds and microseconds and isn't
    adjusted by timezone, length of month or anything else in datetime
    calculations.

    See functions :eql:func:`to_duration`, and :eql:func:`to_str` and
    date/time :eql:op:`operators <dtminus>` for more ways of working with
    :eql:type:`duration`.


----------


.. eql:type:: cal::relative_duration

    A type representing a span of time.

    Unlike :eql:type:`std::duration` a ``relative_duration`` is not a precise
    measurment because it uses 3 different units under the hood: months, days
    and seconds. However not all months have the same number of days and not
    all days have the same number of seconds. For example 2019 was a leap year
    and had 366 days. Notice how the number of hours in each year below is
    different.

    .. code-block:: edgeql-repl

        db> with
        ...     first_day_of_2020 := <datetime>'2020-01-01T00:00:00Z',
        ...     one_year := <cal::relative_duration>'1 year',
        ...     first_day_of_next_year := first_day_of_2020 + one_year
        ... select first_day_of_next_year - first_day_of_2020;
        {<duration>'8784:00:00'}
        db> with
        ...     first_day_of_2019 := <datetime>'2019-01-01T00:00:00Z',
        ...     one_year := <cal::relative_duration>'1 year',
        ...     first_day_of_next_year := first_day_of_2019 + one_year
        ... select first_day_of_next_year - first_day_of_2019;
        {<duration>'8760:00:00'}

    Valid units when converting from a string (and combinations of them):
    - ``'microseconds'``
    - ``'milliseconds'``
    - ``'seconds'``
    - ``'minutes'``
    - ``'hours'``
    - ``'days'``
    - ``'weeks'``
    - ``'months'``
    - ``'years'``
    - ``'decades'``
    - ``'centuries'``
    - ``'millennia'``

    .. code-block:: edgeql

        select <cal::relative_duration>'45.6 seconds';
        select <cal::relative_duration>'15 milliseconds';
        select <cal::relative_duration>'3 weeks 45 minutes';
        select <cal::relative_duration>'-7 millennia';

    All date/time types support the ``+`` and ``-`` arithmetic operations
    with relative_durations:

    .. code-block:: edgeql-repl

        db> select <datetime>'2019-01-01T00:00:00Z' -
        ...        <cal::relative_duration>'3 years';
        {<datetime>'2016-01-01T00:00:00+00:00'}
        db> select <cal::local_time>'22:00' +
        ...        <cal::relative_duration>'1 hour';
        {<cal::local_time>'23:00:00'}

    If an arithmetic operation results in a day that doesn't exist in the given
    month, the last day of the month is used instead.

    .. code-block:: edgeql-repl

      db> select <cal::local_datetime>"2021-01-31T15:00:00" +
      ...        <cal::relative_duration>"1 month";
      {<cal::local_datetime>'2021-02-28T15:00:00'}


    During arithmetic operations involving a ``relative_duration`` consisting
    of multiple components (units), higher-order components are applied first,
    followed by lower-order elements.

    .. code-block:: edgeql-repl

      db> select <cal::local_datetime>"2021-04-30T15:00:00" +
      ...        <cal::relative_duration>"1 month 1 day";
      {<cal::local_datetime>'2021-05-31T15:00:00'}

    Compare this to adding up the same duration components separately
    with higher-order units first and then lower-order, which produces
    the same result as above:

    .. code-block:: edgeql-repl

      db> select <cal::local_datetime>"2021-04-30T15:00:00" +
      ...        <cal::relative_duration>"1 month" +
      ...        <cal::relative_duration>"1 day";
      {<cal::local_datetime>'2021-05-31T15:00:00'}

    When the order is reversed the result may actually be different
    for some corner cases:

    .. code-block:: edgeql-repl

      db> select <cal::local_datetime>"2021-04-30T15:00:00" +
      ...        <cal::relative_duration>"1 day" +
      ...        <cal::relative_duration>"1 month";
      {<cal::local_datetime>'2021-06-01T15:00:00'}

    **Gotchas**

    Due to the implementation of ``relative_duration`` logic, arithmetic
    operations may behave counterintuitively.

    Non-associative

    .. code-block:: edgeql-repl

      db> select <cal::local_datetime>'2021-01-31T00:00:00' +
      ...        <cal::relative_duration>'1 month' +
      ...        <cal::relative_duration>'1 month';
      {<cal::local_datetime>'2021-03-28T00:00:00'}
      db> select <cal::local_datetime>'2021-01-31T00:00:00' +
      ...       (<cal::relative_duration>'1 month' +
      ...        <cal::relative_duration>'1 month');
      {<cal::local_datetime>'2021-03-31T00:00:00'}
      db> select <cal::local_date>'2021-01-31' +
      ...        <cal::relative_duration>'12 hours' +
      ...        <cal::relative_duration>'12 hours';
      {<cal::local_date>'2021-01-31'}
      db> select <cal::local_date>'2021-01-31' +
      ...       (<cal::relative_duration>'12 hours' +
      ...        <cal::relative_duration>'12 hours');
      {<cal::local_date>'2021-02-01'}

    Lossy

    .. code-block:: edgeql-repl

      db> with m := <cal::relative_duration>'1 month'
      ... select <cal::local_date>'2021-01-31' + m
      ...        =
      ...        <cal::local_date>'2021-01-30' + m;
      {true}

    Asymmetric

    .. code-block:: edgeql-repl

      db> with m := <cal::relative_duration>'1 month'
      ... select <cal::local_date>'2021-01-31' + m - m;
      {<cal::local_date>'2021-01-28'}

    Non-monotonic

    .. code-block:: edgeql-repl

      db> with m := <cal::relative_duration>'1 month'
      ... select <cal::local_datetime>'2021-01-31T01:00:00' + m
      ...        <
      ...        <cal::local_datetime>'2021-01-30T23:00:00' + m;
      {true}
      db> with m := <cal::relative_duration>'2 month'
      ... select <cal::local_datetime>'2021-01-31T01:00:00' + m
      ...        <
      ...        <cal::local_datetime>'2021-01-30T23:00:00' + m;
      {false}

    See functions :eql:func:`cal::to_relative_duration`, and :eql:func:`to_str`
    and date/time :eql:op:`operators <dtminus>` for more ways of working with
    :eql:type:`cal::relative_duration`.


----------


.. eql:operator:: dtplus: datetime + duration -> datetime
                          cal::local_datetime + duration -> cal::local_datetime
                          cal::local_date + duration -> cal::local_date
                          cal::local_time + duration -> cal::local_time
                          duration + duration -> duration
                          datetime + cal::relative_duration \
                              -> cal::relative_duration
                          cal::local_dateiime + cal::relative_duration \
                              -> cal::relative_duration
                          cal::local_date + cal::relative_duration \
                              -> cal::relative_duration
                          cal::local_time + cal::relative_duration \
                              -> cal::relative_duration
                          duration + cal::relative_duration \
                              -> cal::relative_duration
                          cal::relative_duration + cal::relative_duration \
                              -> cal::relative_duration

    Time interval addition.

    This operator is commutative.

    .. code-block:: edgeql-repl

        db> select <cal::local_time>'22:00' + <duration>'1 hour';
        {<cal::local_time>'23:00:00'}
        db> select <duration>'1 hour' + <cal::local_time>'22:00';
        {<cal::local_time>'23:00:00'}
        db> select <duration>'1 hour' + <duration>'2 hours';
        {10800s}


----------


.. eql:operator:: dtminus: duration - duration -> duration
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
                           duration - cal::relative_duration \
                                -> cal::relative_duration
                           cal::relative_duration - duration\
                                -> cal::relative_duration
                           cal::relative_duration - cal::relative_duration \
                                -> cal::relative_duration
                           datetime - cal::relative_duration -> datetime
                           cal::local_datetime - cal::relative_duration \
                                -> cal::local_datetime
                           local_time - cal::relative_duration -> local_time
                           local_date - cal::relative_duration -> local_date

    Time interval and date/time subtraction.

    .. code-block:: edgeql-repl

        db> select <datetime>'2019-01-01T01:02:03+00' -
        ...   <duration>'24 hours';
        {<datetime>'2018-12-31T01:02:03Z'}
        db> select <datetime>'2019-01-01T01:02:03+00' -
        ...   <datetime>'2019-02-01T01:02:03+00';
        {-2678400s}
        db> select <duration>'1 hour' -
        ...   <duration>'2 hours';
        {-3600s}

    It is an error to subtract a date/time object from a time interval:

    .. code-block:: edgeql-repl

        db> select <duration>'1 day' -
        ...   <datetime>'2019-01-01T01:02:03+00';
        QueryError: operator '-' cannot be applied to operands ...

    It is also an error to subtract timezone-aware :eql:type:`std::datetime`
    to or from :eql:type:`cal::local_datetime`:

    .. code-block:: edgeql-repl

        db> select <datetime>'2019-01-01T01:02:03+00' -
        ...   <cal::local_datetime>'2019-02-01T01:02:03';
        QueryError: operator '-' cannot be applied to operands ...


----------

.. eql:function:: std::datetime_current() -> datetime

    Return the current server date and time.

    .. code-block:: edgeql-repl

        db> select datetime_current();
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

        db> select datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00',
        ...     'epochseconds');
        {1525705282.306916}

        db> select datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00',
        ...     'year');
        {2018}

        db> select datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00',
        ...     'quarter');
        {2}

        db> select datetime_get(
        ...     <datetime>'2018-05-07T15:01:22.306916+00',
        ...     'doy');
        {127}

        db> select datetime_get(
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

        db> select cal::time_get(
        ...     <cal::local_time>'15:01:22.306916', 'minutes');
        {1}

        db> select cal::time_get(
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

        db> select cal::date_get(
        ...     <cal::local_date>'2018-05-07', 'century');
        {21}

        db> select cal::date_get(
        ...     <cal::local_date>'2018-05-07', 'year');
        {2018}

        db> select cal::date_get(
        ...     <cal::local_date>'2018-05-07', 'month');
        {5}

        db> select cal::date_get(
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

        db> select datetime_truncate(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'years');
        {<datetime>'2018-01-01T00:00:00Z'}

        db> select datetime_truncate(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'quarters');
        {<datetime>'2018-04-01T00:00:00Z'}

        db> select datetime_truncate(
        ...     <datetime>'2018-05-07T15:01:22.306916+00', 'days');
        {<datetime>'2018-05-07T00:00:00Z'}

        db> select datetime_truncate(
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

        db> select duration_truncate(
        ...     <duration>'15:01:22', 'hours');
        {54000s}

        db> select duration_truncate(
        ...     <duration>'15:01:22.306916', 'minutes');
        {54060s}


----------


.. eql:function:: std::to_datetime(s: str, fmt: optional str={}) -> datetime
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
    <ref_std_converters_datetime_fmt>` to other forms.

    .. code-block:: edgeql-repl

        db> select to_datetime('2018-05-07T15:01:22.306916+00');
        {<datetime>'2018-05-07T15:01:22.306916Z'}
        db> select to_datetime('2018-05-07T15:01:22+00');
        {<datetime>'2018-05-07T15:01:22Z'}
        db> select to_datetime('May 7th, 2018 15:01:22 +00',
        ...                    'Mon DDth, YYYY HH24:MI:SS TZH');
        {<datetime>'2018-05-07T15:01:22Z'}

    Alternatively, the :eql:type:`datetime` value can be constructed
    from a :eql:type:`cal::local_datetime` value:

    .. code-block:: edgeql-repl

        db> select to_datetime(
        ...   <cal::local_datetime>'2019-01-01T01:02:03', 'HKT');
        {<datetime>'2018-12-31T17:02:03Z'}

    Another way to construct a the :eql:type:`datetime` value
    is to specify it in terms of its component parts: *year*, *month*,
    *day*, *hour*, *min*, *sec*, and *timezone*

    .. code-block:: edgeql-repl

        db> select to_datetime(
        ...     2018, 5, 7, 15, 1, 22.306916, 'UTC');
        {<datetime>'2018-05-07T15:01:22.306916000Z'}

    Finally, it is also possible to convert a Unix timestamp to a
    :eql:type:`datetime`

    .. code-block:: edgeql-repl

        db> select to_datetime(1590595184.584);
        {<datetime>'2020-05-27T15:59:44.584000000Z'}

------------


.. eql:function:: cal::to_local_datetime(s: str, fmt: optional str={}) \
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
    <ref_std_converters_datetime_fmt>`.

    .. code-block:: edgeql-repl

        db> select cal::to_local_datetime('2018-05-07T15:01:22.306916');
        {<cal::local_datetime>'2018-05-07T15:01:22.306916'}
        db> select cal::to_local_datetime('May 7th, 2018 15:01:22',
        ...                          'Mon DDth, YYYY HH24:MI:SS');
        {<cal::local_datetime>'2018-05-07T15:01:22'}
        db> select cal::to_local_datetime(
        ...     2018, 5, 7, 15, 1, 22.306916);
        {<cal::local_datetime>'2018-05-07T15:01:22.306916'}

    A timezone-aware :eql:type:`datetime` type can be converted
    to local datetime in the specified timezone:

    .. code-block:: edgeql-repl

        db> select cal::to_local_datetime(
        ...   <datetime>'2018-12-31T22:00:00+08',
        ...   'US/Central');
        {<cal::local_datetime>'2018-12-31T08:00:00'}


------------


.. eql:function:: cal::to_local_date(s: str, fmt: optional str={}) \
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
    <ref_std_converters_datetime_fmt>`.

    .. code-block:: edgeql-repl

        db> select cal::to_local_date('2018-05-07');
        {<cal::local_date>'2018-05-07'}
        db> select cal::to_local_date('May 7th, 2018', 'Mon DDth, YYYY');
        {<cal::local_date>'2018-05-07'}
        db> select cal::to_local_date(2018, 5, 7);
        {<cal::local_date>'2018-05-07'}

    A timezone-aware :eql:type:`datetime` type can be converted
    to local date in the specified timezone:

    .. code-block:: edgeql-repl

        db> select cal::to_local_date(
        ...   <datetime>'2018-12-31T22:00:00+08',
        ...   'US/Central');
        {<cal::local_date>'2019-01-01'}


------------


.. eql:function:: cal::to_local_time(s: str, fmt: optional str={}) \
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
    <ref_std_converters_datetime_fmt>`.

    .. code-block:: edgeql-repl

        db> select cal::to_local_time('15:01:22.306916');
        {<cal::local_time>'15:01:22.306916'}
        db> select cal::to_local_time('03:01:22pm', 'HH:MI:SSam');
        {<cal::local_time>'15:01:22'}
        db> select cal::to_local_time(15, 1, 22.306916);
        {<cal::local_time>'15:01:22.306916'}

    A timezone-aware :eql:type:`datetime` type can be converted
    to local date in the specified timezone:

    .. code-block:: edgeql-repl

        db> select cal::to_local_time(
        ...   <datetime>'2018-12-31T22:00:00+08',
        ...   'US/Pacific');
        {<cal::local_time>'06:00:00'}


------------


.. eql:function:: std::to_duration( \
                    named only hours: int64=0, \
                    named only minutes: int64=0, \
                    named only seconds: float64=0, \
                    named only microseconds: int64=0 \
                  ) -> duration

    :index: duration

    Create a :eql:type:`duration` value.

    This function uses ``named only`` arguments to create a
    :eql:type:`duration` value. The available duration fields are:
    *hours*, *minutes*, *seconds*, *microseconds*.

    .. code-block:: edgeql-repl

        db> select to_duration(hours := 1,
        ...                    minutes := 20,
        ...                    seconds := 45);
        {4845s}
        db> select to_duration(seconds := 4845);
        {4845s}


.. eql:function:: std::duration_to_seconds(cur: duration) -> decimal

    Return duration as total number of seconds in interval.

    .. code-block:: edgeql-repl

        db> select duration_to_seconds(<duration>'1 hour');
        {3600.000000n}
        db> select duration_to_seconds(<duration>'10 second 123 ms');
        {10.123000n}


------------


.. eql:function:: cal::to_relative_duration( \
                    named only years: int64=0, \
                    named only months: int64=0, \
                    named only days: int64=0, \
                    named only hours: int64=0, \
                    named only minutes: int64=0, \
                    named only seconds: float64=0, \
                    named only microseconds: int64=0 \
                  ) -> cal::relative_duration

    :index: parse relative_duration

    Create a :eql:type:`cal::relative_duration` value.

    This function uses ``named only`` arguments to create a
    :eql:type:`cal::relative_duration` value. The available duration fields
    are: *years*, *months*, *days*, *hours*, *minutes*, *seconds*,
    *microseconds*.

    .. code-block:: edgeql-repl

        db> select cal::to_relative_duration(years := 5, minutes := 1);
        {P5YT1S}
        db> select cal::to_relative_duration(months := 3, days := 27);
        {P3M27D}
