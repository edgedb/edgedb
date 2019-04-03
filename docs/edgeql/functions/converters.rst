..
    Portions Copyright (c) 2019 MagicStack Inc. and the EdgeDB authors.

    Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
    Portions Copyright (c) 1994, The Regents of the University of California

    Permission to use, copy, modify, and distribute this software and its
    documentation for any purpose, without fee, and without a written agreement
    is hereby granted, provided that the above copyright notice and this
    paragraph and the following two paragraphs appear in all copies.

    IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
    DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
    LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
    DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.

    THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
    INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
    AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
    ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
    PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.


.. _ref_eql_functions_converters:


Type Converters
===============

These functions convert between different scalar types. When a
simple cast is not sufficient to specify how data must be converted,
the functions below allow more options for such conversions.


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
    to ISO 8601 format. However, the optional argument *fmt* can be
    used to override the input format to other forms.

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

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_datetime_fmt>`.


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

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_datetime_fmt>`.


------------


.. eql:function:: std::to_local_date(s: str, fmt: OPTIONAL str={}) \
                    -> local_date
                  std::to_local_date(year: int64, month: int64, \
                    day: int64) -> local_date

    :index: parse local_date

    Create a :eql:type:`local_date` value.

    Similar to :eql:func:`to_datetime`, the :eql:type:`local_date`
    value can be parsed from the input :eql:type:`str` *s* with an
    optional *fmt* argument or it can be given in terms of its
    component parts: *year*, *month*, *day*.

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

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_datetime_fmt>`.


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

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_datetime_fmt>`.


------------


.. eql:function:: std::to_timedelta( \
                    NAMED ONLY years: int64=0, \
                    NAMED ONLY months: int64=0, \
                    NAMED ONLY weeks: int64=0, \
                    NAMED ONLY days: int64=0, \
                    NAMED ONLY hours: int64=0, \
                    NAMED ONLY mins: int64=0, \
                    NAMED ONLY secs: float64=0 \
                  ) -> timedelta

    :index: timedelta

    Create a :eql:type:`timedelta` value.

    This function uses ``NAMED ONLY`` arguments  to create a
    :eql:type:`timedelta` value. The available timedelta fields are:
    *years*, *months*, *weeks*, *days*, *hours*, *mins*, *secs*.

    .. code-block:: edgeql-repl

        db> SELECT to_timedelta(hours := 1,
        ...                     mins := 20,
        ...                     secs := 45);
        {<timedelta>'1:20:45'}
        db> SELECT to_timedelta(secs := 4845);
        {<timedelta>'1:20:45'}

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_datetime_fmt>`.


------------


.. eql:function:: std::to_decimal(s: str, fmt: OPTIONAL str={}) -> decimal

    :index: parse decimal

    Create a :eql:type:`decimal` value.

    Parse a :eql:type:`decimal` from the input *s* and optional format
    specification *fmt*.

    .. code-block:: edgeql-repl

        db> SELECT to_decimal('-000,012,345', 'S099,999,999,999');
        {-12345n}
        db> SELECT to_decimal('-012.345');
        {-12.345n}
        db> SELECT to_decimal('31st', '999th');
        {31n}

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_number_fmt>`.


------------


.. eql:function:: std::to_int16(s: str, fmt: OPTIONAL str={}) -> int16

    :index: parse int16

    Create a :eql:type:`int16` value.

    Parse a :eql:type:`int16` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_number_fmt>`.


------------


.. eql:function:: std::to_int32(s: str, fmt: OPTIONAL str={}) -> int32

    :index: parse int32

    Create a :eql:type:`int32` value.

    Parse a :eql:type:`int32` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_number_fmt>`.


------------


.. eql:function:: std::to_int64(s: str, fmt: OPTIONAL str={}) -> int64

    :index: parse int64

    Create a :eql:type:`int64` value.

    Parse a :eql:type:`int64` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_number_fmt>`.


------------


.. eql:function:: std::to_float32(s: str, fmt: OPTIONAL str={}) -> float32

    :index: parse float32

    Create a :eql:type:`float32` value.

    Parse a :eql:type:`float32` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_number_fmt>`.


------------


.. eql:function:: std::to_float64(s: str, fmt: OPTIONAL str={}) -> float64

    :index: parse float64

    Create a :eql:type:`float64` value.

    Parse a :eql:type:`float64` from the input *s* and optional format
    specification *fmt*.

    For more details on formatting see :ref:`here
    <ref_eql_functions_converters_number_fmt>`.


------------


.. eql:function:: std::to_json(string: str) -> json

    :index: json parse loads

    Return JSON value represented by the input *string*.

    .. code-block:: edgeql-repl

        db> SELECT to_json('[1, "hello", null]')[1];
        {'"hello"'}
        db> SELECT to_json('{"hello": "world"}')['hello'];
        {'"world"'}


------------


.. eql:function:: std::to_str(val: datetime, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: local_datetime, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: local_date, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: local_time, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: timedelta, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: int64, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: float64, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: decimal, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: json, fmt: OPTIONAL str={}) -> str
                  std::to_str(array: array<str>, delimiter: str) -> str

    :index: stringify dumps join array_to_string

    Return string representation of the input value.

    This is a very versatile polymorphic function that is defined for
    many different input types. In general, there are corresponding
    converter functions from :eql:type:`str` back to the specific
    types, which share the meaning of the format argument *fmt*.

    When converting :eql:type:`datetime`, :eql:type:`local_datetime`,
    :eql:type:`local_date`, :eql:type:`local_time`,
    :eql:type:`timedelta` this function is the inverse of
    :eql:func:`to_datetime`, :eql:func:`to_local_datetime`,
    :eql:func:`to_local_date`, :eql:func:`to_local_time`,
    :eql:func:`to_timedelta`, correspondingly.

    .. code-block:: edgeql-repl

        db> SELECT to_str(<datetime>'2018-05-07 15:01:22.306916-05',
        ...               'FMDDth of FMMonth, YYYY');
        {'7th of May, 2018'}
        db> SELECT to_str(<local_date>'2018-05-07', 'CCth "century"');
        {'21st century'}

    When converting one of the numeric types, this function is the
    reverse of: :eql:func:`to_decimal`, :eql:func:`to_int16`,
    :eql:func:`to_int32`, :eql:func:`to_int64`,
    :eql:func:`to_float32`, :eql:func:`to_float64`.

    See also :eql:func:`to_json`.

    .. code-block:: edgeql-repl

        db> SELECT to_str(123, '999999');
        {'    123'}
        db> SELECT to_str(123, '099999');
        {' 000123'}
        db> SELECT to_str(123.45, 'S999.999');
        {'+123.450'}
        db> SELECT to_str(123.45e-20, '9.99EEEE');
        {' 1.23e-18'}
        db> SELECT to_str(-123.45n, 'S999.99');
        {'-123.45'}

    When converting :eql:type:`json`, this function can take
    ``'pretty'`` as the optional *fmt* argument to produce
    pretty-formatted JSON string.

    See also :eql:func:`to_json`.

    .. code-block:: edgeql-repl

        db> SELECT to_str(<json>2);
        {'2'}

        db> SELECT to_str(<json>['hello', 'world']);
        {'["hello", "world"]'}

        db> SELECT to_str(<json>(a := 2, b := 'hello'), 'pretty');
        {'{
            "a": 2,
            "b": "hello"
        }'}


    When converting :eql:type:`arrays <array>`, a *delimiter* argument
    is required:

    .. code-block:: edgeql-repl

        db> SELECT to_str(['one', 'two', 'three'], ', ');
        {'one, two, three'}


------------


Formatting
----------

Some of the type converter functions take an extra argument specifying
the formatting (either for converting to a :eql:type:`str` or parsing
from one). The different formatting options are collected in this section.


.. _ref_eql_functions_converters_datetime_fmt:

Date and time formatting options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-------------------------+----------------------------------------+
| Pattern                 | Description                            |
+=========================+========================================+
| HH                      | hour of day (01-12)                    |
+-------------------------+----------------------------------------+
| HH12                    | hour of day (01-12)                    |
+-------------------------+----------------------------------------+
| HH24                    | hour of day (00-23)                    |
+-------------------------+----------------------------------------+
| MI                      | minute (00-59)                         |
+-------------------------+----------------------------------------+
| SS                      | second (00-59)                         |
+-------------------------+----------------------------------------+
| MS                      | millisecond (000-999)                  |
+-------------------------+----------------------------------------+
| US                      | microsecond (000000-999999)            |
+-------------------------+----------------------------------------+
| SSSS                    | seconds past midnight (0-86399)        |
+-------------------------+----------------------------------------+
| AM, am, PM or pm        | meridiem indicator (without periods)   |
+-------------------------+----------------------------------------+
| A.M., a.m., P.M. or     | meridiem indicator (with periods)      |
| p.m.                    |                                        |
+-------------------------+----------------------------------------+
| Y,YYY                   | year (4 or more digits) with comma     |
+-------------------------+----------------------------------------+
| YYYY                    | year (4 or more digits)                |
+-------------------------+----------------------------------------+
| YYY                     | last 3 digits of year                  |
+-------------------------+----------------------------------------+
| YY                      | last 2 digits of year                  |
+-------------------------+----------------------------------------+
| Y                       | last digit of year                     |
+-------------------------+----------------------------------------+
| IYYY                    | ISO 8601 week-numbering year (4 or     |
|                         | more digits)                           |
+-------------------------+----------------------------------------+
| IYY                     | last 3 digits of ISO 8601 week-        |
|                         | numbering year                         |
+-------------------------+----------------------------------------+
| IY                      | last 2 digits of ISO 8601 week-        |
|                         | numbering year                         |
+-------------------------+----------------------------------------+
| I                       | last digit of ISO 8601 week-numbering  |
|                         | year                                   |
+-------------------------+----------------------------------------+
| BC, bc, AD or ad        | era indicator (without periods)        |
+-------------------------+----------------------------------------+
| B.C., b.c., A.D. or     | era indicator (with periods)           |
| a.d.                    |                                        |
+-------------------------+----------------------------------------+
| MONTH                   | full upper case month name (blank-     |
|                         | padded to 9 chars)                     |
+-------------------------+----------------------------------------+
| Month                   | full capitalized month name (blank-    |
|                         | padded to 9 chars)                     |
+-------------------------+----------------------------------------+
| month                   | full lower case month name (blank-     |
|                         | padded to 9 chars)                     |
+-------------------------+----------------------------------------+
| MON                     | abbreviated upper case month name (3   |
|                         | chars in English, localized lengths    |
|                         | vary)                                  |
+-------------------------+----------------------------------------+
| Mon                     | abbreviated capitalized month name (3  |
|                         | chars in English, localized lengths    |
|                         | vary)                                  |
+-------------------------+----------------------------------------+
| mon                     | abbreviated lower case month name (3   |
|                         | chars in English, localized lengths    |
|                         | vary)                                  |
+-------------------------+----------------------------------------+
| MM                      | month number (01-12)                   |
+-------------------------+----------------------------------------+
| DAY                     | full upper case day name (blank-padded |
|                         | to 9 chars)                            |
+-------------------------+----------------------------------------+
| Day                     | full capitalized day name (blank-      |
|                         | padded to 9 chars)                     |
+-------------------------+----------------------------------------+
| day                     | full lower case day name (blank-padded |
|                         | to 9 chars)                            |
+-------------------------+----------------------------------------+
| DY                      | abbreviated upper case day name (3     |
|                         | chars in English, localized lengths    |
|                         | vary)                                  |
+-------------------------+----------------------------------------+
| Dy                      | abbreviated capitalized day name (3    |
|                         | chars in English, localized lengths    |
|                         | vary)                                  |
+-------------------------+----------------------------------------+
| dy                      | abbreviated lower case day name (3     |
|                         | chars in English, localized lengths    |
|                         | vary)                                  |
+-------------------------+----------------------------------------+
| DDD                     | day of year (001-366)                  |
+-------------------------+----------------------------------------+
| IDDD                    | day of ISO 8601 week-numbering year    |
|                         | (001-371; day 1 of the year is Monday  |
|                         | of the first ISO week)                 |
+-------------------------+----------------------------------------+
| DD                      | day of month (01-31)                   |
+-------------------------+----------------------------------------+
| D                       | day of the week, Sunday (1) to         |
|                         | Saturday (7)                           |
+-------------------------+----------------------------------------+
| ID                      | ISO 8601 day of the week, Monday (1)   |
|                         | to Sunday (7)                          |
+-------------------------+----------------------------------------+
| W                       | week of month (1-5) (the first week    |
|                         | starts on the first day of the month)  |
+-------------------------+----------------------------------------+
| WW                      | week number of year (1-53) (the first  |
|                         | week starts on the first day of the    |
|                         | year)                                  |
+-------------------------+----------------------------------------+
| IW                      | week number of ISO 8601 week-numbering |
|                         | year (01-53; the first Thursday of the |
|                         | year is in week 1)                     |
+-------------------------+----------------------------------------+
| CC                      | century (2 digits) (the twenty-first   |
|                         | century starts on 2001-01-01)          |
+-------------------------+----------------------------------------+
| J                       | Julian Day (integer days since         |
|                         | November 24, 4714 BC at midnight UTC)  |
+-------------------------+----------------------------------------+
| Q                       | quarter                                |
+-------------------------+----------------------------------------+
| RM                      | month in upper case Roman numerals     |
|                         | (I-XII; I=January)                     |
+-------------------------+----------------------------------------+
| rm                      | month in lower case Roman numerals     |
|                         | (i-xii; i=January)                     |
+-------------------------+----------------------------------------+
| TZ                      | upper case time-zone abbreviation      |
|                         | (only supported in to_char)            |
+-------------------------+----------------------------------------+
| tz                      | lower case time-zone abbreviation      |
|                         | (only supported in to_char)            |
+-------------------------+----------------------------------------+
| TZH                     | time-zone hours                        |
+-------------------------+----------------------------------------+
| TZM                     | time-zone minutes                      |
+-------------------------+----------------------------------------+
| OF                      | time-zone offset from UTC (only        |
|                         | supported in to_char)                  |
+-------------------------+----------------------------------------+

Some additional formatting modifiers:

+---------------+-----------------------------------+---------------+
| Modifier      | Description                       | Example       |
+===============+===================================+===============+
| FM prefix     | fill mode (suppress leading       | FMMonth       |
|               | zeroes and padding blanks)        |               |
+---------------+-----------------------------------+---------------+
| TH suffix     | upper case ordinal number suffix  | DDTH, e.g.,   |
|               |                                   | 12TH          |
+---------------+-----------------------------------+---------------+
| th suffix     | lower case ordinal number suffix  | DDth, e.g.,   |
|               |                                   | 12th          |
+---------------+-----------------------------------+---------------+
| FX prefix     | fixed format global option (see   | FX Month DD   |
|               | usage notes)                      | Day           |
+---------------+-----------------------------------+---------------+

Normally when parsing a string input whitespace is ignored, unless
the *FX* prefix modifier is used. For example:

.. code-block:: edgeql-repl

    db> SELECT to_local_date(
    ...     '2000    JUN', 'YYYY MON');
    {<local_date>'2000-06-01'}
    db> SELECT to_local_date(
    ...     '2000    JUN', 'FXYYYY MON');
    InternalServerError: invalid value "   " for "MON"


.. _ref_eql_functions_converters_number_fmt:

Number formatting options
^^^^^^^^^^^^^^^^^^^^^^^^^

+------------+-----------------------------------------------------+
| Pattern    | Description                                         |
+============+=====================================================+
| 9          | digit position (can be dropped if insignificant)    |
+------------+-----------------------------------------------------+
| 0          | digit position (will not be dropped, even if        |
|            | insignificant)                                      |
+------------+-----------------------------------------------------+
| .          | period)  decimal point                              |
+------------+-----------------------------------------------------+
| ,          | comma)   group (thousands) separator                |
+------------+-----------------------------------------------------+
| PR         | negative value in angle brackets                    |
+------------+-----------------------------------------------------+
| S          | sign anchored to number (uses locale)               |
+------------+-----------------------------------------------------+
| L          | currency symbol (uses locale)                       |
+------------+-----------------------------------------------------+
| D          | decimal point (uses locale)                         |
+------------+-----------------------------------------------------+
| G          | group separator (uses locale)                       |
+------------+-----------------------------------------------------+
| MI         | minus sign in specified position (if number < 0)    |
+------------+-----------------------------------------------------+
| PL         | plus sign in specified position (if number > 0)     |
+------------+-----------------------------------------------------+
| SG         | plus/minus sign in specified position               |
+------------+-----------------------------------------------------+
| RN         | Roman numeral (input between 1 and 3999)            |
+------------+-----------------------------------------------------+
| TH or th   | ordinal number suffix                               |
+------------+-----------------------------------------------------+
| V          | shift specified number of digits (see notes)        |
+------------+-----------------------------------------------------+
| EEEE       | exponent for scientific notation                    |
+------------+-----------------------------------------------------+

Some additional formatting modifiers:

+---------------+-----------------------------------+---------------+
| Modifier      | Description                       | Example       |
+===============+===================================+===============+
| FM prefix     | fill mode (suppress leading       | FM99.99       |
|               | zeroes and padding blanks)        |               |
+---------------+-----------------------------------+---------------+
| TH suffix     | upper case ordinal number suffix  | 999TH         |
+---------------+-----------------------------------+---------------+
| th suffix     | lower case ordinal number suffix  | 999th         |
+---------------+-----------------------------------+---------------+
