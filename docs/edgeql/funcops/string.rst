.. _ref_eql_funcops_string:

======
String
======

:edb-alt-title: String Functions and Operators


.. list-table::
    :class: funcoptable

    * - :eql:op:`str[i] <STRIDX>`
      - :eql:op-desc:`STRIDX`

    * - :eql:op:`str[from:to] <STRSLICE>`
      - :eql:op-desc:`STRSLICE`

    * - :eql:op:`str ++ str <STRPLUS>`
      - :eql:op-desc:`STRPLUS`

    * - :eql:op:`str LIKE pattern <LIKE>`
      - :eql:op-desc:`LIKE`

    * - :eql:op:`str ILIKE pattern <ILIKE>`
      - :eql:op-desc:`ILIKE`

    * - :eql:op:`str = str <EQ>`, :eql:op:`str \< str <LT>`, ...
      - Comparison operators.

    * - :eql:func:`to_str`
      - :eql:func-desc:`to_str`

    * - :eql:func:`len`
      - Return string's length.

    * - :eql:func:`contains`
      - Test if a string contains a substring.

    * - :eql:func:`find`
      - Find index of a substring.

    * - :eql:func:`str_lower`
      - :eql:func-desc:`str_lower`

    * - :eql:func:`str_upper`
      - :eql:func-desc:`str_upper`

    * - :eql:func:`str_title`
      - :eql:func-desc:`str_title`

    * - :eql:func:`str_lpad`
      - :eql:func-desc:`str_lpad`

    * - :eql:func:`str_rpad`
      - :eql:func-desc:`str_rpad`

    * - :eql:func:`str_trim`
      - :eql:func-desc:`str_trim`

    * - :eql:func:`str_ltrim`
      - :eql:func-desc:`str_ltrim`

    * - :eql:func:`str_rtrim`
      - :eql:func-desc:`str_rtrim`

    * - :eql:func:`str_repeat`
      - :eql:func-desc:`str_repeat`

    * - :eql:func:`re_match`
      - :eql:func-desc:`re_match`

    * - :eql:func:`re_match_all`
      - :eql:func-desc:`re_match_all`

    * - :eql:func:`re_replace`
      - :eql:func-desc:`re_replace`

    * - :eql:func:`re_test`
      - :eql:func-desc:`re_test`


----------


.. eql:operator:: STRIDX: str [ int64 ] -> str

    String indexing.

    .. code-block:: edgeql-repl

        db> SELECT 'some text'[1];
        {'o'}
        db> SELECT 'some text'[1:3];
        {'om'}
        db> SELECT 'some text'[-4:];
        {'text'}


----------


.. eql:operator:: STRSLICE: str [ int64 : int64 ] -> str

    String slicing.

    .. code-block:: edgeql-repl

        db> SELECT 'some text'[1:3];
        {'om'}
        db> SELECT 'some text'[-4:];
        {'text'}


----------


.. eql:operator:: STRPLUS: str ++ str -> str

    String concatenation.

    .. code-block:: edgeql-repl

        db> SELECT 'some' ++ ' text';
        {'some text'}


----------


.. eql:operator:: LIKE: str LIKE str -> bool
                        str NOT LIKE str -> bool

    Case-sensitive simple string matching.

    Returns ``true`` if the *value* ``V`` matches the *pattern* ``P``
    and ``false`` otherwise.  The operator :eql:op:`NOT LIKE<LIKE>` is
    the negation of :eql:op:`LIKE`.

    The pattern matching rules are as follows:

    .. list-table::
        :widths: auto
        :header-rows: 1

        * - pattern
          - interpretation
        * - ``%``
          - matches zero or more characters
        * - ``_``
          - matches exactly one character
        * - ``\%``
          - matches a literal "%"
        * - ``\_``
          - matches a literal "_"
        * - any other character
          - matches itself

    In particular, this means that if there are no special symbols in
    the *pattern*, the operators :eql:op:`LIKE` and :eql:op:`NOT
    LIKE<LIKE>` work identical to :eql:op:`EQ` and :eql:op:`NEQ`,
    respectively.

    .. code-block:: edgeql-repl

        db> SELECT 'abc' LIKE 'abc';
        {true}
        db> SELECT 'abc' LIKE 'a%';
        {true}
        db> SELECT 'abc' LIKE '_b_';
        {true}
        db> SELECT 'abc' LIKE 'c';
        {false}
        db> SELECT 'a%%c' NOT LIKE r'a\%c';
        {true}


----------


.. eql:operator:: ILIKE: str ILIKE str -> bool
                         str NOT ILIKE str -> bool

    Case-insensitive simple string matching.

    The operators :eql:op:`ILIKE` and :eql:op:`NOT ILIKE<ILIKE>` work
    the same way as :eql:op:`LIKE` and :eql:op:`NOT LIKE<LIKE>`,
    except that the *pattern* is matched in a case-insensitive manner.

    .. code-block:: edgeql-repl

        db> SELECT 'Abc' ILIKE 'a%';
        {true}


----------


.. eql:function:: std::str_lower(string: str) -> str

    Return a lowercase copy of the input *string*.

    .. code-block:: edgeql-repl

        db> SELECT str_lower('Some Fancy Title');
        {'some fancy title'}


----------


.. eql:function:: std::str_upper(string: str) -> str

    Return an uppercase copy of the input *string*.

    .. code-block:: edgeql-repl

        db> SELECT str_upper('Some Fancy Title');
        {'SOME FANCY TITLE'}


----------


.. eql:function:: std::str_title(string: str) -> str

    Return a titlecase copy of the input *string*.

    Every word in the *string* will have the first letter capitalized
    and the rest converted to lowercase.

    .. code-block:: edgeql-repl

        db> SELECT str_title('sOmE fAnCy TiTlE');
        {'Some Fancy Title'}


----------


.. eql:function:: std::str_lpad(string: str, n: int64, fill: str = ' ') -> str

    Return the input *string* left-padded to the length *n*.

    If the *string* is longer than *n*, then it is truncated to the
    first *n* characters. Otherwise, the *string* is padded on the
    left up to the total length *n* using *fill* characters (space by
    default).

    .. code-block:: edgeql-repl

        db> SELECT str_lpad('short', 10);
        {'     short'}
        db> SELECT str_lpad('much too long', 10);
        {'much too l'}
        db> SELECT str_lpad('short', 10, '.:');
        {'.:.:.short'}


----------


.. eql:function:: std::str_rpad(string: str, n: int64, fill: str = ' ') -> str

    Return the input *string* right-padded to the length *n*.

    If the *string* is longer than *n*, then it is truncated to the
    first *n* characters. Otherwise, the *string* is padded on the
    right up to the total length *n* using *fill* characters (space by
    default).

    .. code-block:: edgeql-repl

        db> SELECT str_rpad('short', 10);
        {'short     '}
        db> SELECT str_rpad('much too long', 10);
        {'much too l'}
        db> SELECT str_rpad('short', 10, '.:');
        {'short.:.:.'}


----------


.. eql:function:: std::str_ltrim(string: str, trim: str = ' ') -> str

    Return the input *string* with all leftmost *trim* characters removed.

    If the *trim* specifies more than one character they will be
    removed from the beginning of the *string* regardless of the order
    in which they appear.

    .. code-block:: edgeql-repl

        db> SELECT str_ltrim('     data');
        {'data'}
        db> SELECT str_ltrim('.....data', '.:');
        {'data'}
        db> SELECT str_ltrim(':::::data', '.:');
        {'data'}
        db> SELECT str_ltrim(':...:data', '.:');
        {'data'}
        db> SELECT str_ltrim('.:.:.data', '.:');
        {'data'}


----------


.. eql:function:: std::str_rtrim(string: str, trim: str = ' ') -> str

    Return the input *string* with all rightmost *trim* characters removed.

    If the *trim* specifies more than one character they will be
    removed from the end of the *string* regardless of the order
    in which they appear.

    .. code-block:: edgeql-repl

        db> SELECT str_rtrim('data     ');
        {'data'}
        db> SELECT str_rtrim('data.....', '.:');
        {'data'}
        db> SELECT str_rtrim('data:::::', '.:');
        {'data'}
        db> SELECT str_rtrim('data:...:', '.:');
        {'data'}
        db> SELECT str_rtrim('data.:.:.', '.:');
        {'data'}


----------


.. eql:function:: std::str_trim(string: str, trim: str = ' ') -> str

    Return the input *string* with *trim* characters removed from both ends.

    If the *trim* specifies more than one character they will be
    removed from both ends of the *string* regardless of the order
    in which they appear. This is the same as applying
    :eql:func:`str_ltrim` and :eql:func:`str_rtrim`.

    .. code-block:: edgeql-repl

        db> SELECT str_trim('  data     ');
        {'data'}
        db> SELECT str_trim('::data.....', '.:');
        {'data'}
        db> SELECT str_trim('..data:::::', '.:');
        {'data'}
        db> SELECT str_trim('.:data:...:', '.:');
        {'data'}
        db> SELECT str_trim(':.:.data.:.', '.:');
        {'data'}


----------


.. eql:function:: std::str_repeat(string: str, n: int64) -> str

    Repeat the input *string* *n* times.

    If *n* is zero or negative an empty string is returned.

    .. code-block:: edgeql-repl

        db> SELECT str_repeat('.', 3);
        {'...'}
        db> SELECT str_repeat('foo', -1);
        {''}


----------


.. eql:function:: std::re_match(pattern: str, \
                                string: str) -> array<str>

    :index: regex regexp regular

    Find the first regular expression match in a string.

    Given an input *string* and a regular expression :ref:`pattern
    <string_regexp>` find the first match for the regular expression
    within the *string*. Return the match, each match represented by
    an :eql:type:`array\<str\>` of matched groups.

    .. code-block:: edgeql-repl

        db> SELECT re_match(r'\w{4}ql', 'I ❤️ edgeql');
        {['edgeql']}


----------


.. eql:function:: std::re_match_all(pattern: str, \
                                    string: str) -> SET OF array<str>

    :index: regex regexp regular

    Find all regular expression matches in a string.

    Given an input *string* and a regular expression :ref:`pattern
    <string_regexp>` repeatedly match the regular expression within
    the *string*. Return the set of all matches, each match
    represented by an :eql:type:`array\<str\>` of matched groups.

    .. code-block:: edgeql-repl

        db> SELECT re_match_all(r'a\w+', 'an abstract concept');
        {['an'], ['abstract']}


----------


.. eql:function:: std::re_replace(pattern: str, sub: str, \
                                  string: str, \
                                  NAMED ONLY flags: str='') \
                  -> str

    :index: regex regexp regular replace

    Replace matching substrings in a given string.

    Given an input *string* and a regular expression :ref:`pattern
    <string_regexp>` replace matching substrings with the replacement
    string *sub*. Optional :ref:`flag <string_regexp_flags>` argument
    can be used to specify additional regular expression flags. Return
    the string resulting from substring replacement.

    .. code-block:: edgeql-repl

        db> SELECT re_replace(r'l', r'L', 'Hello World',
        ...                   flags := 'g');
        {'HeLLo WorLd'}


----------


.. eql:function:: std::re_test(pattern: str, string: str) -> bool

    :index: regex regexp regular match

    Test if a regular expression has a match in a string.

    Given an input *string* and a regular expression :ref:`pattern
    <string_regexp>` test whether there is a match for the regular
    expression within the *string*. Return ``true`` if there is a
    match, ``false`` otherwise.

    .. code-block:: edgeql-repl

        db> SELECT re_test(r'a', 'abc');
        {true}


------------


.. eql:function:: std::to_str(val: datetime, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: local_datetime, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: local_date, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: local_time, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: duration, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: int64, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: float64, fmt: OPTIONAL str={}) -> str
                  std::to_str(val: bigint, fmt: OPTIONAL str={}) -> str
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
    :eql:type:`duration` this function is the inverse of
    :eql:func:`to_datetime`, :eql:func:`to_local_datetime`,
    :eql:func:`to_local_date`, :eql:func:`to_local_time`,
    :eql:func:`to_duration`, correspondingly.

    For valid date and time formatting patterns see
    :ref:`here <ref_eql_functions_converters_datetime_fmt>`.

    .. code-block:: edgeql-repl

        db> SELECT to_str(<datetime>'2018-05-07 15:01:22.306916-05',
        ...               'FMDDth of FMMonth, YYYY');
        {'7th of May, 2018'}
        db> SELECT to_str(<local_date>'2018-05-07', 'CCth "century"');
        {'21st century'}

    When converting one of the numeric types, this function is the
    reverse of: :eql:func:`to_bigint`, :eql:func:`to_decimal`,
    :eql:func:`to_int16`, :eql:func:`to_int32`, :eql:func:`to_int64`,
    :eql:func:`to_float32`, :eql:func:`to_float64`.

    For valid number formatting patterns see
    :ref:`here <ref_eql_functions_converters_number_fmt>`.

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


----------


.. _string_regexp:

Regular Expressions
-------------------

EdgeDB supports Regular expressions (REs), as defined in POSIX 1003.2.
They come in two forms: BRE (basic RE) and ERE (extended RE). In
addition to that EdgeDB supports certain common extensions to the
POSIX standard commonly known as ARE (advanced RE). More details about
BRE, ERE, and ARE support can be found in `PostgreSQL documentation`_.


.. _`PostgreSQL documentation`:
                https://www.postgresql.org/docs/10/static/
                functions-matching.html#POSIX-SYNTAX-DETAILS

For convenience, here's a table outlining the different options
accepted as the ``flag`` argument to various regular expression
functions:

.. _string_regexp_flags:

Option Flags
^^^^^^^^^^^^

======  ==================================================================
Option  Description
======  ==================================================================
``b``   rest of RE is a BRE
``c``   case-sensitive matching (overrides operator type)
``e``   rest of RE is an ERE
``i``   case-insensitive matching (overrides operator type)
``m``   historical synonym for n
``n``   newline-sensitive matching
``p``   partial newline-sensitive matching
``q``   rest of RE is a literal ("quoted") string, all ordinary characters
``s``   non-newline-sensitive matching (default)
``t``   tight syntax (default)
``w``   inverse partial newline-sensitive ("weird") matching
``x``   expanded syntax ignoring white-space characters
======  ==================================================================


----------


Formatting
----------

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
