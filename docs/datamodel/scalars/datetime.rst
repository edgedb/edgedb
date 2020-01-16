.. _ref_datamodel_scalars_datetime:

Date and Time
=============

:edb-alt-title: Date/Time Types


EdgeDB has two classes of date/time types:

* a timezone-aware :eql:type:`std::datetime` type;

* a set of "local" date/time objects, not attached to any particular
  timezone: :eql:type:`cal::local_datetime`, :eql:type:`cal::local_date`,
  and :eql:type:`cal::local_time`.

All date/time :ref:`functions and operators <ref_eql_funcops_datetime>`
and type casts are designed to maintain a strict separation between
timezone-aware and "local" date/time values.

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

    A type representing date without time zone.

    :eql:op:`Casting <CAST>` is a simple way to obtain a
    :eql:type:`cal::local_date` value in an expression:

    .. code-block:: edgeql

        SELECT <cal::local_date>'2018-05-07';

    Note that when casting from strings, the string should be in ISO
    8601 date format.

    See functions :eql:func:`date_get`, :eql:func:`cal::to_local_date`,
    and :eql:func:`to_str` for more ways of working with
    :eql:type:`cal::local_date`.


----------


.. eql:type:: cal::local_time

    A type representing time without time zone.

    :eql:op:`Casting <CAST>` is a simple way to obtain a
    :eql:type:`cal::local_time` value in an expression:

    .. code-block:: edgeql

        SELECT <cal::local_time>'15:01:22.306916';
        SELECT <cal::local_time>'15:01:22';

    Note that when casting from strings, the string should be in ISO
    8601 time format.

    See functions :eql:func:`time_get`, :eql:func:`cal::to_local_time`,
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


See Also
--------

Scalar type
:ref:`SDL <ref_eql_sdl_scalars>`,
:ref:`DDL <ref_eql_ddl_scalars>`,
:ref:`introspection <ref_eql_introspection_scalar_types>`,
and :ref:`date/time functions and operators <ref_eql_funcops_datetime>`.
