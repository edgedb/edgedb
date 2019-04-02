.. _ref_datamodel_scalars_datetime:

Date and Time
=============

EdgeDB has two classes of date/time types:

* a timezone-aware :eql:type:`std::datetime` type;

* a set of "local" date/time objects, not attached to any particular
  timezone: :eql:type:`std::local_datetime`, :eql:type:`std::local_date`,
  and :eql:type:`std::local_time`.

All date/time :ref:`operators <ref_eql_operators_datetime>`,
:ref:`functions <ref_eql_functions_datetime>`,
:ref:`type converters <ref_eql_functions_converters>`, and type casts
are designed to maintain a strict separation between timezone-aware
and "local" date/time values.

EdgeDB stores and outputs timezone-aware values in UTC.


.. eql:type:: std::datetime

    A timezone-aware type representing date and time.

    :ref:`Casting <ref_eql_expr_typecast>` is required to obtain a
    :eql:type:`datetime` value in an expression:

    .. code-block:: edgeql

        SELECT <datetime>'2018-05-07T15:01:22.306916+00';
        SELECT <datetime>'2018-05-07T15:01:22+00';

    Note that when casting from strings a timezone is always required:

    .. code-block:: edgeql-repl

        db> SELECT <datetime>'January 01 2019';
        InvalidValueError: an explicit timezone is required

        db> SELECT <datetime>'January 01 2019 UTC';
        {<datetime>'2019-01-01T00:00:00+00:00'}

    See functions :eql:func:`datetime_get`, :eql:func:`to_datetime`,
    and :eql:func:`to_str` for more ways of working with
    :eql:type:`datetime`.

.. eql:type:: std::local_datetime

    A type representing date and time without time zone.

    :ref:`Casting <ref_eql_expr_typecast>` is required to obtain a
    :eql:type:`local_datetime` value in an expression:

    .. code-block:: edgeql

        SELECT <local_datetime>'2018-05-07T15:01:22.306916';
        SELECT <local_datetime>'2018-05-07T15:01:22';

    Note that when casting from strings a timezone must not be
    included:

    .. code-block:: edgeql-repl

        db> SELECT <local_datetime>'January 01 2019 UTC';
        InvalidValueError: invalid input syntax for type std::local_datetime

        db> SELECT <local_datetime>'January 01 2019';
        {<local_datetime>'2019-01-01T00:00:00'}

    See functions :eql:func:`datetime_get`, :eql:func:`to_local_datetime`,
    and :eql:func:`to_str` for more ways of working with
    :eql:type:`local_datetime`.

.. eql:type:: std::local_date

    A type representing date without time zone.

    :ref:`Casting <ref_eql_expr_typecast>` is required to obtain a
    :eql:type:`local_date` value in an expression:

    .. code-block:: edgeql

        SELECT <local_date>'2018-05-07';

    See functions :eql:func:`date_get`, :eql:func:`to_local_date`,
    and :eql:func:`to_str` for more ways of working with
    :eql:type:`local_date`.

.. eql:type:: std::local_time

    A type representing time without time zone.

    :ref:`Casting <ref_eql_expr_typecast>` is required to obtain a
    :eql:type:`local_time` value in an expression:

    .. code-block:: edgeql

        SELECT <local_time>'15:01:22.306916';
        SELECT <local_time>'15:01:22';

    See functions :eql:func:`time_get`, :eql:func:`to_local_time`,
    and :eql:func:`to_str` for more ways of working with
    :eql:type:`local_time`.

.. eql:type:: std::timedelta

    A type representing a relative time interval.

    The time interval can be specified in terms of *microseconds*,
    *milliseconds*, *seconds*, *minutes*, *hours*, *days*, *weeks*,
    *months*, *years*, *decades*, *centuries*, *millennia*, e.g.:

    .. code-block:: edgeql

        SELECT <timedelta>'15 minutes';
        SELECT <timedelta>'45.6 seconds';
        SELECT <timedelta>'2.3 millennia 3 weeks';

    It's worth noting that time intervals are inherently ambiguous
    when it comes to some units like *days*, *months* or *years*, but
    for other units the conversion is unambiguous. For this reason,
    the scalar actually stores its component parts independently. They
    are grouped as follows:

    - The value of units ranging from *microseconds* to *hours* can
      all be unambiguously converted and this is done automatically.
      This portion is stored as one whole part.
    - The number of *hours* in a *day* is ambiguous (technically it's
      not exactly 24, leap years and other leap rules exist to
      compensate for this). So "next day" could mean something
      slightly different from "in 86400 seconds exactly". For this
      reason *days* are stored as a separate part. Number of *days* in
      a *week* is well-defined and *weeks* are converted to *days*.
    - The number of *days* in a *month* is ambiguous (simply because
      different months have 28, 29, 30, or 31 days). So "next month"
      could mean different things in terms of days. However,
      everything bigger than a *month* is well-defined (12 *months* in
      a *year*, 10 *years* in a *decade*, etc.). So the time interval
      larger than a *month* gets normalized and stored as a whole
      part.

    .. code-block:: edgeql-repl

        db> SELECT <timedelta>
        ...     '12 decades 2403 months 3987 days 12348943ms';
        {'320 years 3 mons 3987 days 03:25:48.943'}

    All date/time types support the ``+`` and ``-`` arithmetic operations
    with time intervals:

    .. code-block:: edgeql-repl

        db> select <datetime>'January 01 2019 UTC' - <timedelta>'1 day';
        {<datetime>'2018-12-31T00:00:00+00:00'}
        db> select <local_time>'22:00' + <timedelta>'1 hour';
        {<local_time>'23:00:00'}

    See functions :eql:func:`timedelta_get`, :eql:func:`to_timedelta`,
    and :eql:func:`to_str` and date/time :
    :ref:`operators <ref_eql_operators_datetime>` for more ways of
    working with :eql:type:`timedelta`.
