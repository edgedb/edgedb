.. _ref_datamodel_scalars_datetime:

Date/Time Types
===============

.. eql:type:: std::datetime

    A type representing date, time, and time zone.

.. eql:type:: std::date

    A type representing date and time zone.

.. eql:type:: std::time

    A type representing time and time zone.

.. eql:type:: std::timedelta

    A type representing a relative time interval.

    The time interval can be specified in terms of microseconds,
    milliseconds, seconds, minutes, hours, days, weeks, months, years,
    decades, centuries, millennia, e.g.:

    .. code-block:: edgeql

        SELECT <timedelta>'2.3 millennia 3 weeks';
