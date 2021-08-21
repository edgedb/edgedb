.. _ref_datamodel_scalars_datetime:

=============
Date and Time
=============

:edb-alt-title: Date/Time Types


EdgeDB has two classes of date/time types:

* a timezone-aware :eql:type:`std::datetime` type;

* a set of "local" date/time objects, not attached to any particular
  timezone: :eql:type:`cal::local_datetime`, :eql:type:`cal::local_date`,
  and :eql:type:`cal::local_time`.

There are also two different ways of measuring duration:

* :eql:type:`duration` using absolute and unambiguous units;

* :eql:type:`cal::relative_duration` using fuzzy units like years,
  months and days in addition to the absolute units.

All date/time :ref:`operators <ref_std_datetime>` and
:ref:`functions <ref_std_datetime>` and type casts are designed to
maintain a strict separation between timezone-aware and "local"
date/time values.

EdgeDB stores and outputs timezone-aware values in UTC.


----------


.. eql:type:: std::datetime

    A timezone-aware type representing date and time.

    :eql:op:`Casting <CAST>` is a simple way to obtain a
    :eql:type:`datetime` value in an expression:

    .. code-block:: edgeql

        SELECT <datetime>'2018-05-07T15:01:22.306916+00';
        SELECT <datetime>'2018-05-07T15:01:22+00';

    Note that when casting from strings, the string should be in ISO
    8601 format with timezone included:

    .. code-block:: edgeql-repl

        db> SELECT <datetime>'January 01 2019 UTC';
        InvalidValueError: invalid input syntax for type
        std::datetime: 'January 01 2019 UTC'
        Hint: Please use ISO8601 format. Alternatively "to_datetime"
        function provides custom formatting options.

        db> SELECT <datetime>'2019-01-01T15:01:22';
        InvalidValueError: invalid input syntax for type
        std::datetime: '2019-01-01T15:01:22'
        Hint: Please use ISO8601 format. Alternatively "to_datetime"
        function provides custom formatting options.

    See functions :eql:func:`datetime_get`, :eql:func:`to_datetime`,
    and :eql:func:`to_str` for more ways of working with
    :eql:type:`datetime`.


----------


.. eql:type:: cal::local_datetime

    A type representing date and time without time zone.

    :eql:op:`Casting <CAST>` is a simple way to obtain a
    :eql:type:`cal::local_datetime` value in an expression:

    .. code-block:: edgeql

        SELECT <cal::local_datetime>'2018-05-07T15:01:22.306916';
        SELECT <cal::local_datetime>'2018-05-07T15:01:22';

    Note that when casting from strings, the string should be in ISO
    8601 format without timezone:

    .. code-block:: edgeql-repl

        db> SELECT <cal::local_datetime>'2019-01-01T15:01:22+00';
        InvalidValueError: invalid input syntax for type
        cal::local_datetime: '2019-01-01T15:01:22+00'
        Hint: Please use ISO8601 format. Alternatively
        "cal::to_local_datetime" function provides custom formatting
        options.

        db> SELECT <cal::local_datetime>'January 01 2019';
        InvalidValueError: invalid input syntax for type
        cal::local_datetime: 'January 01 2019'
        Hint: Please use ISO8601 format. Alternatively
        "cal::to_local_datetime" function provides custom formatting
        options.

    See functions :eql:func:`datetime_get`, :eql:func:`cal::to_local_datetime`,
    and :eql:func:`to_str` for more ways of working with
    :eql:type:`cal::local_datetime`.


----------


.. eql:type:: cal::local_date

    A type representing a date without a time zone.

    :eql:op:`Casting <CAST>` is a simple way to obtain a
    :eql:type:`cal::local_date` value in an expression:

    .. code-block:: edgeql

        SELECT <cal::local_date>'2018-05-07';

    Note that when casting from strings, the string should be in ISO
    8601 date format.

    See functions :eql:func:`cal::date_get`, :eql:func:`cal::to_local_date`,
    and :eql:func:`to_str` for more ways of working with
    :eql:type:`cal::local_date`.


----------


.. eql:type:: cal::local_time

    A type representing time without a time zone.

    :eql:op:`Casting <CAST>` is a simple way to obtain a
    :eql:type:`cal::local_time` value in an expression:

    .. code-block:: edgeql

        SELECT <cal::local_time>'15:01:22.306916';
        SELECT <cal::local_time>'15:01:22';

    Note that when casting from strings, the string should be in ISO
    8601 time format.

    See functions :eql:func:`cal::time_get`, :eql:func:`cal::to_local_time`,
    and :eql:func:`to_str` for more ways of working with
    :eql:type:`cal::local_time`.


----------


.. eql:type:: std::duration

    A type representing a span of time.

    Valid units when converting from a string (and combinations of them):
    - ``'microseconds'``
    - ``'milliseconds'``
    - ``'seconds'``
    - ``'minutes'``
    - ``'hours'``

    .. code-block:: edgeql

        SELECT <duration>'45.6 seconds';
        SELECT <duration>'15 milliseconds';
        SELECT <duration>'48 hours 45 minutes';
        SELECT <duration>'-7 minutes';

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
    date/time :eql:op:`operators <DTMINUS>` for more ways of working with
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

        db> WITH
        ...     first_day_of_2020 := <datetime>'2020-01-01T00:00:00Z',
        ...     one_year := <cal::relative_duration>'1 year',
        ...     first_day_of_next_year := first_day_of_2020 + one_year
        ... SELECT first_day_of_next_year - first_day_of_2020;
        {<duration>'8784:00:00'}
        db> WITH
        ...     first_day_of_2019 := <datetime>'2019-01-01T00:00:00Z',
        ...     one_year := <cal::relative_duration>'1 year',
        ...     first_day_of_next_year := first_day_of_2019 + one_year
        ... SELECT first_day_of_next_year - first_day_of_2019;
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
    - ``'millennium'``

    .. code-block:: edgeql

        SELECT <cal::relative_duration>'45.6 seconds';
        SELECT <cal::relative_duration>'15 milliseconds';
        SELECT <cal::relative_duration>'3 weeks 45 minutes';
        SELECT <cal::relative_duration>'-7 millennium';

    All date/time types support the ``+`` and ``-`` arithmetic operations
    with relative_durations:

    .. code-block:: edgeql-repl

        db> select <datetime>'2019-01-01T00:00:00Z' -
        ...        <cal::relative_duration>'3 years';
        {<datetime>'2016-01-01T00:00:00+00:00'}
        db> select <cal::local_time>'22:00' + <cal::relative_duration>'1 hour';
        {<cal::local_time>'23:00:00'}

    See functions :eql:func:`cal::to_relative_duration`, and :eql:func:`to_str`
    and date/time :eql:op:`operators <DTMINUS>` for more ways of working with
    :eql:type:`cal::relative_duration`.


See Also
--------

Scalar type
:ref:`SDL <ref_eql_sdl_scalars>`,
:ref:`DDL <ref_eql_ddl_scalars>`,
:ref:`introspection <ref_eql_introspection_scalar_types>`,
date/time :ref:`operators <ref_std_datetime>`.
and :ref:`functions <ref_std_datetime>`.
