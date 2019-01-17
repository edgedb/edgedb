.. _ref_datamodel_scalars_datetime:

Date and Time
=============

.. eql:type:: std::datetime

    A type representing date, time, and time zone.

.. eql:type:: std::naive_datetime

    A type representing date and time without time zone.

.. eql:type:: std::naive_date

    A type representing date without time zone.

.. eql:type:: std::naive_time

    A type representing time without time zone.

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
