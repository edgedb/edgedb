.. _ref_std_datetime:


===============
Dates and Times
===============

:edb-alt-title: Types, Functions, and Operators for Dates and Times

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

    * - :eql:type:`cal::date_duration`
      - Relative time span in days

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

    * - :eql:func:`cal::to_date_duration`
      - :eql:func-desc:`cal::to_date_duration`

    * - :eql:func:`datetime_get`
      - :eql:func-desc:`datetime_get`

    * - :eql:func:`cal::time_get`
      - :eql:func-desc:`cal::time_get`

    * - :eql:func:`cal::date_get`
      - :eql:func-desc:`cal::date_get`

    * - :eql:func:`duration_get`
      - :eql:func-desc:`duration_get`

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

    * - :eql:func:`cal::duration_normalize_hours`
      - :eql:func-desc:`cal::duration_normalize_hours`

    * - :eql:func:`cal::duration_normalize_days`
      - :eql:func-desc:`cal::duration_normalize_days`

.. _ref_std_datetime_intro:

EdgeDB offers two ways of representing date/time values:

* a timezone-aware :eql:type:`std::datetime` type;

* a set of "local" date/time types, not attached to any particular
  timezone: :eql:type:`cal::local_datetime`, :eql:type:`cal::local_date`,
  and :eql:type:`cal::local_time`.

There are also two different ways of measuring duration:

* :eql:type:`duration` for using absolute and unambiguous units;

* :eql:type:`cal::relative_duration` for using fuzzy units like years,
  months and days in addition to the absolute units.

All related operators, functions, and type casts are designed to maintain a
strict separation between timezone-aware and "local" date/time values.

EdgeDB stores and outputs timezone-aware values in UTC format.

.. note::

    All date/time types are restricted to years between 1 and 9999, including
    the years 1 and 9999.

    Although many systems support ISO 8601 date/time formatting in theory,
    in practice the formatting before year 1 and after 9999 tends to
    be inconsistent. As such, dates outside this range are not reliably
    portable.

.. _ref_std_datetime_timezones:

Timezones
---------

For timezone string literals, you may specify timezones in one of two ways:

* IANA (Olson) timezone database name (e.g. ``America/New_York``)

* A time zone abbreviation (e.g. ``EDT`` for Eastern Daylight Time)

See the `relevant section from the PostgreSQL documentation
<https://www.postgresql.org/docs/current/datetime-timezones.html#TIMEZONE-TABLES>`_
for more detail about how time zones affect the behavior of date/time
functionality.

.. note::

  The IANA timezone database is maintained by Paul Eggert for the IANA. You can
  find a `GitHub repository with the latest timezone data here
  <https://github.com/eggert/tz>`_, and the `list of timezone names here
  <https://github.com/eggert/tz/blob/master/zone1970.tab>`_.


----------


.. eql:type:: std::datetime


    Represents a timezone-aware moment in time.

    All dates must correspond to dates that exist in the proleptic Gregorian
    calendar.

    :eql:op:`Casting <cast>` is a simple way to obtain a
    :eql:type:`datetime` value in an expression:

    .. code-block:: edgeql

        select <datetime>'2018-05-07T15:01:22.306916+00';
        select <datetime>'2018-05-07T15:01:22+00';

    When casting ``datetime`` from strings, the string must follow
    the ISO 8601 format with a timezone included.

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

    For more information regarding interacting with this type, see
    :eql:func:`datetime_get`, :eql:func:`to_datetime`, and :eql:func:`to_str`.


----------


.. eql:type:: cal::local_datetime

    A type for representing a date and time without a timezone.

    :eql:op:`Casting <cast>` is a simple way to obtain a
    :eql:type:`cal::local_datetime` value in an expression:

    .. code-block:: edgeql

        select <cal::local_datetime>'2018-05-07T15:01:22.306916';
        select <cal::local_datetime>'2018-05-07T15:01:22';

    When casting ``cal::local_datetime`` from strings, the string must follow
    the ISO 8601 format without timezone:

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

    For more information regarding interacting with this type, see
    :eql:func:`datetime_get`, :eql:func:`cal::to_local_datetime`, and
    :eql:func:`to_str`.


----------


.. eql:type:: cal::local_date

    A type for representing a date without a timezone.

    :eql:op:`Casting <cast>` is a simple way to obtain a
    :eql:type:`cal::local_date` value in an expression:

    .. code-block:: edgeql

        select <cal::local_date>'2018-05-07';

    When casting ``cal::local_date`` from strings, the string must follow the
    ISO 8601 date format.

    For more information regarding interacting with this type, see
    :eql:func:`cal::date_get`, :eql:func:`cal::to_local_date`, and
    :eql:func:`to_str`.


----------


.. eql:type:: cal::local_time

    A type for representing a time without a timezone.

    :eql:op:`Casting <cast>` is a simple way to obtain a
    :eql:type:`cal::local_time` value in an expression:

    .. code-block:: edgeql

        select <cal::local_time>'15:01:22.306916';
        select <cal::local_time>'15:01:22';

    When casting ``cal::local_time`` from strings, the string must follow the
    ISO 8601 time format.

    For more information regarding interacting with this type, see
    :eql:func:`cal::time_get`, :eql:func:`cal::to_local_time`, and
    :eql:func:`to_str`.


----------



.. _ref_datetime_duration:

.. eql:type:: std::duration

    A type for representing a span of time.

    A :eql:type:`duration` is a fixed number of seconds and microseconds and
    isn't adjusted by timezone, length of month, or anything else in datetime
    calculations.

    When converting from a string, only units of ``'microseconds'``,
    ``'milliseconds'``, ``'seconds'``, ``'minutes'``, and ``'hours'`` are
    valid:

    .. code-block:: edgeql-repl

        db> select <duration>'45.6 seconds';
        {<duration>'0:00:45.6'}
        db> select <duration>'15 milliseconds';
        {<duration>'0:00:00.015'}
        db> select <duration>'48 hours 45 minutes';
        {<duration>'48:45:00'}
        db> select <duration>'11 months';
        edgedb error: InvalidValueError: invalid input syntax for type
        std::duration: '11 months'
          Hint: Units bigger than hours cannot be used for std::duration.

    All date/time types support the ``+`` and ``-`` arithmetic operations
    with durations:

    .. code-block:: edgeql-repl

        db> select <datetime>'2019-01-01T00:00:00Z' - <duration>'24 hours';
        {<datetime>'2018-12-31T00:00:00+00:00'}
        db> select <cal::local_time>'22:00' + <duration>'1 hour';
        {<cal::local_time>'23:00:00'}

    For more information regarding interacting with this type, see
    :eql:func:`to_duration`, and :eql:func:`to_str` and date/time
    :eql:op:`operators <dtplus>`.


----------


.. eql:type:: cal::relative_duration

    A type for representing a relative span of time.

    Unlike :eql:type:`std::duration`, ``cal::relative_duration`` is an
    imprecise form of measurement. When months and days are used, the same
    relative duration could have a different absolute duration depending on
    the date you're measuring from.

    For example 2020 was a leap year and had 366 days. Notice how the number
    of hours in each year below is different:

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

    When converting from a string, only the following units are valid:

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

    Examples of units usage:

    .. code-block:: edgeql

        select <cal::relative_duration>'45.6 seconds';
        select <cal::relative_duration>'15 milliseconds';
        select <cal::relative_duration>'3 weeks 45 minutes';
        select <cal::relative_duration>'-7 millennia';

    All date/time types support the ``+`` and ``-`` arithmetic operations
    with ``relative_duration``:

    .. code-block:: edgeql-repl

        db> select <datetime>'2019-01-01T00:00:00Z' -
        ...        <cal::relative_duration>'3 years';
        {<datetime>'2016-01-01T00:00:00+00:00'}
        db> select <cal::local_time>'22:00' +
        ...        <cal::relative_duration>'1 hour';
        {<cal::local_time>'23:00:00'}

    If an arithmetic operation results in a day that doesn't exist in the
    given month, the last day of the month will be used instead:

    .. code-block:: edgeql-repl

      db> select <cal::local_datetime>"2021-01-31T15:00:00" +
      ...        <cal::relative_duration>"1 month";
      {<cal::local_datetime>'2021-02-28T15:00:00'}

    For arithmetic operations involving a ``cal::relative_duration``
    consisting of multiple components (units), higher-order components are
    applied first followed by lower-order components.

    .. code-block:: edgeql-repl

      db> select <cal::local_datetime>"2021-04-30T15:00:00" +
      ...        <cal::relative_duration>"1 month 1 day";
      {<cal::local_datetime>'2021-05-31T15:00:00'}

    If you add the same components split into separate durations, adding the
    higher-order units first followed by the lower-order units, the
    calculation produces the same result as in the previous example:

    .. code-block:: edgeql-repl

      db> select <cal::local_datetime>"2021-04-30T15:00:00" +
      ...        <cal::relative_duration>"1 month" +
      ...        <cal::relative_duration>"1 day";
      {<cal::local_datetime>'2021-05-31T15:00:00'}

    When the order of operations is reversed, the result may be different for
    some corner cases:

    .. code-block:: edgeql-repl

      db> select <cal::local_datetime>"2021-04-30T15:00:00" +
      ...        <cal::relative_duration>"1 day" +
      ...        <cal::relative_duration>"1 month";
      {<cal::local_datetime>'2021-06-01T15:00:00'}

    .. rubric:: Gotchas

    Due to the implementation of ``relative_duration`` logic, arithmetic
    operations may behave counterintuitively.

    **Non-associative**

    .. code-block:: edgeql-repl

      db> select <cal::local_datetime>'2021-01-31T00:00:00' +
      ...        <cal::relative_duration>'1 month' +
      ...        <cal::relative_duration>'1 month';
      {<cal::local_datetime>'2021-03-28T00:00:00'}
      db> select <cal::local_datetime>'2021-01-31T00:00:00' +
      ...       (<cal::relative_duration>'1 month' +
      ...        <cal::relative_duration>'1 month');
      {<cal::local_datetime>'2021-03-31T00:00:00'}

    **Lossy**

    .. code-block:: edgeql-repl

      db> with m := <cal::relative_duration>'1 month'
      ... select <cal::local_date>'2021-01-31' + m
      ...        =
      ...        <cal::local_date>'2021-01-30' + m;
      {true}

    **Asymmetric**

    .. code-block:: edgeql-repl

      db> with m := <cal::relative_duration>'1 month'
      ... select <cal::local_date>'2021-01-31' + m - m;
      {<cal::local_date>'2021-01-28'}

    **Non-monotonic**

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

    For more information regarding interacting with this type, see
    :eql:func:`cal::to_relative_duration`, and :eql:func:`to_str` and
    date/time :eql:op:`operators <dtplus>`.


----------


.. eql:type:: cal::date_duration

    .. versionadded:: 2.0

    A type for representing a span of time in days.

    This type is similar to :eql:type:`cal::relative_duration`, except it only
    uses 2 units: months and days. It is the result of subtracting one
    :eql:type:`cal::local_date` from another. The purpose of this type is to
    allow performing ``+`` and ``-`` operations on a
    :eql:type:`cal::local_date` and to produce a :eql:type:`cal::local_date`
    as the result:

    .. code-block:: edgeql-repl

      db> select <cal::local_date>'2022-06-30' -
      ...   <cal::local_date>'2022-06-25';
      {<cal::date_duration>'P5D'}
      db> select <cal::local_date>'2022-06-25' +
      ...   <cal::date_duration>'5 days';
      {<cal::local_date>'2022-06-30'}
      db> select <cal::local_date>'2022-06-25' -
      ...   <cal::date_duration>'5 days';
      {<cal::local_date>'2022-06-20'}


    When converting from a string, only the following units are valid:

    - ``'days'``,
    - ``'weeks'``,
    - ``'months'``,
    - ``'years'``,
    - ``'decades'``,
    - ``'centuries'``,
    - ``'millennia'``.

    .. code-block:: edgeql

        select <cal::date_duration>'45 days';
        select <cal::date_duration>'3 weeks 5 days';
        select <cal::date_duration>'-7 millennia';

    In most cases, ``date_duration`` is fully compatible with
    :eql:type:`cal::relative_duration` and shares the same general behavior
    and caveats. EdgeDB will apply type coercion in the event it expects a
    :eql:type:`cal::relative_duration` and finds a ``cal::date_duration``
    instead.

    For more information regarding interacting with this type, see
    :eql:func:`cal::to_date_duration` and date/time :eql:op:`operators
    <dtplus>`.


----------


.. eql:operator:: dtplus: datetime + duration -> datetime
                          datetime + cal::relative_duration \
                              -> cal::relative_duration
                          duration + duration -> duration
                          duration + cal::relative_duration \
                              -> cal::relative_duration
                          cal::relative_duration + cal::relative_duration \
                              -> cal::relative_duration
                          cal::local_datetime + cal::relative_duration \
                              -> cal::relative_duration
                          cal::local_datetime + duration \
                              -> cal::local_datetime
                          cal::local_time + cal::relative_duration \
                              -> cal::relative_duration
                          cal::local_time + duration -> cal::local_time
                          cal::local_date + cal::date_duration \
                              -> cal::local_date
                          cal::date_duration + cal::date_duration \
                              -> cal::date_duration
                          cal::local_date + cal::relative_duration \
                              -> cal::local_datetime
                          cal::local_date + duration -> cal::local_datetime

    Adds a duration and any other datetime value.

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
                           datetime - duration -> datetime
                           datetime - cal::relative_duration -> datetime
                           cal::relative_duration - cal::relative_duration \
                                -> cal::relative_duration
                           cal::local_datetime - cal::local_datetime \
                                -> cal::relative_duration
                           cal::local_datetime - cal::relative_duration \
                                -> cal::local_datetime
                           cal::local_datetime - duration \
                                -> cal::local_datetime
                           cal::local_time - cal::local_time \
                                -> cal::relative_duration
                           cal::local_time - cal::relative_duration \
                                -> cal::local_time
                           cal::local_time - duration -> cal::local_time
                           cal::date_duration - cal::date_duration \
                                -> cal::date_duration
                           cal::local_date - cal::local_date \
                                -> cal::date_duration
                           cal::local_date - cal::date_duration \
                                -> cal::local_date
                           cal::local_date - cal::relative_duration \
                                -> cal::local_datetime
                           cal::local_date - duration -> cal::local_datetime
                           duration - cal::relative_duration \
                                -> cal::relative_duration
                           cal::relative_duration - duration\
                                -> cal::relative_duration

    Subtracts two compatible datetime or duration values.

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

    When subtracting a :eql:type:`cal::local_date` type from another, the
    result is given as a whole number of days using the
    :eql:type:`cal::date_duration` type:

    .. code-block:: edgeql-repl

        db> select <cal::local_date>'2022-06-25' -
        ...   <cal::local_date>'2019-02-01';
        {<cal::date_duration>'P1240D'}

    .. note::

        Subtraction doesn't make sense for some type combinations. You
        couldn't subtract a point in time from a duration, so neither can
        EdgeDB (although the inverse — subtracting a duration from a point in
        time — is perfectly fine). You also couldn't subtract a timezone-aware
        datetime from a local one or vice versa. If you attempt any of these,
        EdgeDB will raise an exception as shown in these examples.

    When subtracting a date/time object from a time interval, an exception
    will be raised:

    .. code-block:: edgeql-repl

        db> select <duration>'1 day' -
        ...   <datetime>'2019-01-01T01:02:03+00';
        QueryError: operator '-' cannot be applied to operands ...

    An exception will also be raised when trying to subtract a timezone-aware
    :eql:type:`std::datetime` type from :eql:type:`cal::local_datetime` or
    vice versa:

    .. code-block:: edgeql-repl

        db> select <datetime>'2019-01-01T01:02:03+00' -
        ...   <cal::local_datetime>'2019-02-01T01:02:03';
        QueryError: operator '-' cannot be applied to operands...
        db> select <cal::local_datetime>'2019-02-01T01:02:03' -
        ...   <datetime>'2019-01-01T01:02:03+00';
        QueryError: operator '-' cannot be applied to operands...


----------

.. eql:function:: std::datetime_current() -> datetime

    :index: now

    Returns the server's current date and time.

    .. code-block:: edgeql-repl

        db> select datetime_current();
        {<datetime>'2018-05-14T20:07:11.755827Z'}

    This function is volatile since it always returns the current time when it
    is called. As a result, it cannot be used in :ref:`computed properties
    defined in schema <ref_datamodel_computed>`. This does *not* apply to
    computed properties outside of schema.

----------


.. eql:function:: std::datetime_of_transaction() -> datetime

    :index: now

    Returns the date and time of the start of the current transaction.

    This function is non-volatile since it returns the current time when the
    transaction is started, not when the function is called. As a result, it
    can be used in :ref:`computed properties <ref_datamodel_computed>` defined
    in schema.

----------


.. eql:function:: std::datetime_of_statement() -> datetime

    :index: now

    Returns the date and time of the start of the current statement.

    This function is non-volatile since it returns the current time when the
    statement is started, not when the function is called. As a result, it
    can be used in :ref:`computed properties <ref_datamodel_computed>` defined
    in schema.

----------


.. eql:function:: std::datetime_get(dt: datetime, el: str) -> float64
                  std::datetime_get(dt: cal::local_datetime, \
                                    el: str) -> float64

    Returns the element of a date/time given a unit name.

    You may pass any of these unit names for *el*:

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

    Returns the element of a time value given a unit name.

    You may pass any of these unit names for *el*:

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

    Returns the element of a date given a unit name.

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


.. eql:function:: std::duration_get(dt: duration, el: str) -> float64
                  std::duration_get(dt: cal::relative_duration, \
                                    el: str) -> float64
                  std::duration_get(dt: cal::date_duration, \
                                    el: str) -> float64

    .. versionadded:: 2.0

    Returns the element of a duration given a unit name.

    You may pass any of these unit names as ``el``:

    - ``'millennium'`` - number of 1000-year chunks rounded down
    - ``'century'`` - number of centuries rounded down
    - ``'decade'`` - number of decades rounded down
    - ``'year'`` - number of years rounded down
    - ``'quarter'``- remaining quarters after whole years are accounted for
    - ``'month'`` - number of months left over after whole years are
      accounted for
    - ``'day'`` - number of days recorded in the duration
    - ``'hour'`` - number of hours
    - ``'minutes'`` - remaining minutes after whole hours are accounted for
    - ``'seconds'`` - remaining seconds, including fractional value after whole
      minutes are accounted for
    - ``'milliseconds'`` - remaining seconds including fractional value
      expressed as milliseconds
    - ``'microseconds'`` - remaining seconds including fractional value
      expressed as microseconds

    .. note ::

      Only for units ``'month'`` or larger or for units ``'hour'`` or smaller
      will you receive a total across multiple units expressed in the original
      duration. See *Gotchas* below for details.

    Additionally, it's possible to convert a given duration into seconds:

    - ``'totalseconds'`` - the number of seconds represented by the duration.
      It will be approximate for :eql:type:`cal::relative_duration` and
      :eql:type:`cal::date_duration` for units ``'month'`` or larger because a
      month is assumed to be 30 days exactly.

    The :eql:type:`duration` scalar has only ``'hour'`` and smaller units
    available for extraction.

    The :eql:type:`cal::relative_duration` scalar has all of the units
    available for extraction.

    The :eql:type:`cal::date_duration` scalar only has ``'date'`` and larger
    units available for extraction.

    .. code-block:: edgeql-repl

        db> select duration_get(
        ...   <cal::relative_duration>'400 months', 'year');
        {33}
        db> select duration_get(
        ...   <cal::date_duration>'400 months', 'month');
        {4}
        db> select duration_get(
        ...   <cal::relative_duration>'1 month 20 days 30 hours',
        ...   'day');
        {20}
        db> select duration_get(
        ...   <cal::relative_duration>'30 hours', 'hour');
        {30}
        db> select duration_get(
        ...   <cal::relative_duration>'1 month 20 days 30 hours',
        ...   'hour');
        {30}
        db> select duration_get(<duration>'30 hours', 'hour');
        {30}
        db> select duration_get(
        ...   <cal::relative_duration>'1 month 20 days 30 hours',
        ...   'totalseconds');
        {4428000}
        db> select duration_get(
        ...   <duration>'30 hours', 'totalseconds');
        {108000}

    .. rubric:: Gotchas

    This function will provide you with a calculated total for the unit passed
    as ``el``, but only within the given "size class" of the unit. These size
    classes exist because they are logical breakpoints that we can't reliably
    convert values across. A month might be 30 days long, or it might be 28 or
    29 or 31. A day is generally 24 hours, but with daylight savings, it might
    be longer or shorter.

    As a result, it's impossible to convert across these lines in a way that
    works in every situation. For some use cases, assuming a 30 day month works
    fine. For others, it might not. The size classes are as follows:

    - ``'month'`` and larger
    - ``'day'``
    - ``'hour'`` and smaller

    For example, if you specify ``'day'`` as your ``el`` argument, the function
    will return only the number of days expressed as ``N days`` in your
    duration. It will not add another day to the returned count for every 24
    hours (defined as ``24 hours``) in the duration, nor will it consider the
    months' constituent day counts in the returned value. Specifying
    ``'decade'`` for ``el`` will total up all decades represented in units
    ``'month'`` and larger, but it will not add a decade's worth of days to the
    returned value as an additional decade.

    In this example, the duration represents more than a day's time, but since
    ``'day'`` and ``'hour'`` are in different size classes, the extra day
    stemming from the duration's hours is not added.

    .. code-block:: edgeql-repl

        db> select duration_get(
        ...   <cal::relative_duration>'1 day 36 hours', 'day');
        {1}

    In this counter example, both the decades and months are pooled together
    since they are in the same size class. The return value is 5: the 2
    ``'decades'`` and the 3 decades in ``'400 months'``.

    .. code-block:: edgeql-repl

        db> select duration_get(
        ...   <cal::relative_duration>'2 decades 400 months', 'decade');
        {5}

    If a unit from a smaller size class would contribute to your desired unit's
    total, it is not added.

    .. code-block:: edgeql-repl

        db> select duration_get(
        ...   <cal::relative_duration>'1 year 400 days', 'year');
        {1}

    When you request a unit in the smallest size class, it will be pooled with
    other durations in the same size class.

    .. code-block:: edgeql-repl

        db> select duration_get(
        ...   <cal::relative_duration>'20 hours 3600 seconds', 'hour');
        {21}

    Seconds and smaller units always return remaining time in that unit after
    accounting for the next larger unit.

    .. code-block:: edgeql-repl

        db> select duration_get(
        ...   <cal::relative_duration>'20 hours 3600 seconds', 'seconds');
        {0}
        db> select duration_get(
        ...   <cal::relative_duration>'20 hours 3630 seconds', 'seconds');
        {30}

    Normalization and truncation may help you deal with this. If your use case
    allows for making assumptions about the duration of a month or a day, you
    can make those conversions for yourself using the
    :eql:func:`cal::duration_normalize_hours` or
    :eql:func:`cal::duration_normalize_days` functions. If you got back a
    duration as a result of a datetime calculation and don't need the level of
    granularity you have, you can truncate the value with
    :eql:func:`duration_truncate`.

----------


.. eql:function:: std::datetime_truncate(dt: datetime, unit: str) -> datetime

    Truncates the input datetime to a particular precision.

    The valid units in order or decreasing precision are:

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
        ...   <datetime>'2018-05-07T15:01:22.306916+00', 'years');
        {<datetime>'2018-01-01T00:00:00Z'}

        db> select datetime_truncate(
        ...   <datetime>'2018-05-07T15:01:22.306916+00', 'quarters');
        {<datetime>'2018-04-01T00:00:00Z'}

        db> select datetime_truncate(
        ...   <datetime>'2018-05-07T15:01:22.306916+00', 'days');
        {<datetime>'2018-05-07T00:00:00Z'}

        db> select datetime_truncate(
        ...   <datetime>'2018-05-07T15:01:22.306916+00', 'hours');
        {<datetime>'2018-05-07T15:00:00Z'}


----------


.. eql:function:: std::duration_truncate(dt: duration, unit: str) -> duration
                  std::duration_truncate(dt: cal::relative_duration, \
                    unit: str) -> cal::relative_duration

    Truncates the input duration to a particular precision.

    The valid units for :eql:type:`duration` are:

    - ``'microseconds'``
    - ``'milliseconds'``
    - ``'seconds'``
    - ``'minutes'``
    - ``'hours'``

    In addition to the above the following are also valid for
    :eql:type:`cal::relative_duration`:

    - ``'days'``
    - ``'weeks'``
    - ``'months'``
    - ``'years'``
    - ``'decades'``
    - ``'centuries'``

    .. code-block:: edgeql-repl

        db> select duration_truncate(
        ...   <duration>'15:01:22', 'hours');
        {<duration>'15:00:00'}
        db> select duration_truncate(
        ...   <duration>'15:01:22.306916', 'minutes');
        {<duration>'15:01:00'}
        db> select duration_truncate(
        ...   <cal::relative_duration>'400 months', 'years');
        {<cal::relative_duration>'P33Y'}
        db> select duration_truncate(
        ...   <cal::relative_duration>'400 months', 'decades');
        {<cal::relative_duration>'P30Y'}


----------


.. eql:function:: std::to_datetime(s: str, fmt: optional str={}) -> datetime
                  std::to_datetime(local: cal::local_datetime, zone: str) \
                    -> datetime
                  std::to_datetime(year: int64, month: int64, day: int64, \
                    hour: int64, min: int64, sec: float64, zone: str) \
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

    Another way to construct a the :eql:type:`datetime` value is to specify it
    in terms of its component parts: year, month, day, hour, min, sec, and
    :ref:`zone <ref_std_datetime_timezones>`.

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

    A timezone-aware :eql:type:`datetime` type can be converted to local
    datetime in the specified :ref:`timezone <ref_std_datetime_timezones>`:

    .. code-block:: edgeql-repl

        db> select cal::to_local_datetime(
        ...   <datetime>'2018-12-31T22:00:00+08',
        ...   'America/Chicago');
        {<cal::local_datetime>'2018-12-31T08:00:00'}
        db> select cal::to_local_datetime(
        ...   <datetime>'2018-12-31T22:00:00+08',
        ...   'CST');
        {<cal::local_datetime>'2018-12-31T08:00:00'}


------------


.. eql:function:: cal::to_local_date(s: str, fmt: optional str={}) \
                    -> cal::local_date
                  cal::to_local_date(dt: datetime, zone: str) \
                    -> cal::local_date
                  cal::to_local_date(year: int64, month: int64, \
                    day: int64) -> cal::local_date

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

    A timezone-aware :eql:type:`datetime` type can be converted to local date
    in the specified :ref:`timezone <ref_std_datetime_timezones>`:

    .. code-block:: edgeql-repl

        db> select cal::to_local_date(
        ...   <datetime>'2018-12-31T22:00:00+08',
        ...   'America/Chicago');
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

    A timezone-aware :eql:type:`datetime` type can be converted to local date
    in the specified :ref:`timezone <ref_std_datetime_timezones>`:

    .. code-block:: edgeql-repl

        db> select cal::to_local_time(
        ...   <datetime>'2018-12-31T22:00:00+08',
        ...   'America/Los_Angeles');
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
        {<cal::relative_duration>'P5YT1S'}
        db> select cal::to_relative_duration(months := 3, days := 27);
        {<cal::relative_duration>'P3M27D'}


------------


.. eql:function:: cal::to_date_duration( \
                    named only years: int64=0, \
                    named only months: int64=0, \
                    named only days: int64=0 \
                  ) -> cal::date_duration

    :index: parse date_duration

    Create a :eql:type:`cal::date_duration` value.

    This function uses ``named only`` arguments to create a
    :eql:type:`cal::date_duration` value. The available duration fields
    are: *years*, *months*, *days*.

    .. code-block:: edgeql-repl

        db> select cal::to_date_duration(years := 1, days := 3);
        {<cal::date_duration>'P1Y3D'}
        db> select cal::to_date_duration(days := 12);
        {<cal::date_duration>'P12D'}


------------


.. eql:function:: cal::duration_normalize_hours( \
                    dur: cal::relative_duration \
                  ) -> cal::relative_duration

    :index: justify_hours

    .. versionadded:: 2.0

    Convert 24-hour chunks into days.

    This function converts all 24-hour chunks into day units. The resulting
    :eql:type:`cal::relative_duration` is guaranteed to have less than 24
    hours in total in the units smaler than days.

    .. code-block:: edgeql-repl

        db> select cal::duration_normalize_hours(
        ...   <cal::relative_duration>'1312 hours');
        {<cal::relative_duration>'P54DT16H'}

    This is a lossless operation because 24 hours are always equal to 1 day
    in :eql:type:`cal::relative_duration` units.

    This is sometimes used together with
    :eql:func:`cal::duration_normalize_days`.

------------


.. eql:function:: cal::duration_normalize_days( \
                    dur: cal::relative_duration \
                  ) -> cal::relative_duration
                  cal::duration_normalize_days( \
                    dur: cal::date_duration \
                  ) -> cal::date_duration

    :index: justify_days

    .. versionadded:: 2.0

    Convert 30-day chunks into months.

    This function converts all 30-day chunks into month units. The resulting
    :eql:type:`cal::relative_duration` or :eql:type:`cal::date_duration` is
    guaranteed to have less than 30 day units.

    .. code-block:: edgeql-repl

        db> select cal::duration_normalize_days(
        ...   <cal::relative_duration>'1312 days');
        {<cal::relative_duration>'P3Y7M22D'}

        db> select cal::duration_normalize_days(
        ...   <cal::date_duration>'1312 days');
        {<cal::date_duration>'P3Y7M22D'}

    This function is a form of approximation and does not preserve the exact
    duration.

    This is often used together with
    :eql:func:`cal::duration_normalize_hours`.
