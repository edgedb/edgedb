.. _ref_std_string:

=======
Strings
=======

:edb-alt-title: String Functions and Operators

.. list-table::
    :class: funcoptable

    * - :eql:type:`str`
      - String

    * - :eql:op:`str[i] <stridx>`
      - :eql:op-desc:`stridx`

    * - :eql:op:`str[from:to] <strslice>`
      - :eql:op-desc:`strslice`

    * - :eql:op:`str ++ str <strplus>`
      - :eql:op-desc:`strplus`

    * - :eql:op:`str like pattern <like>`
      - :eql:op-desc:`like`

    * - :eql:op:`str ilike pattern <ilike>`
      - :eql:op-desc:`ilike`

    * - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
        :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
        :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
      - Comparison operators

    * - :eql:func:`to_str`
      - :eql:func-desc:`to_str`

    * - :eql:func:`len`
      - Returns a string's length.

    * - :eql:func:`contains`
      - Tests if a string contains a substring.

    * - :eql:func:`find`
      - Finds the index of a substring.

    * - :eql:func:`str_lower`
      - :eql:func-desc:`str_lower`

    * - :eql:func:`str_upper`
      - :eql:func-desc:`str_upper`

    * - :eql:func:`str_title`
      - :eql:func-desc:`str_title`

    * - :eql:func:`str_pad_start`
      - :eql:func-desc:`str_pad_start`

    * - :eql:func:`str_pad_end`
      - :eql:func-desc:`str_pad_end`

    * - :eql:func:`str_trim`
      - :eql:func-desc:`str_trim`

    * - :eql:func:`str_trim_start`
      - :eql:func-desc:`str_trim_start`

    * - :eql:func:`str_trim_end`
      - :eql:func-desc:`str_trim_end`

    * - :eql:func:`str_repeat`
      - :eql:func-desc:`str_repeat`

    * - :eql:func:`str_replace`
      - :eql:func-desc:`str_replace`

    * - :eql:func:`str_reverse`
      - :eql:func-desc:`str_reverse`

    * - :eql:func:`str_split`
      - Splits a string into an array using a delimiter.

    * - :eql:func:`re_match`
      - :eql:func-desc:`re_match`

    * - :eql:func:`re_match_all`
      - :eql:func-desc:`re_match_all`

    * - :eql:func:`re_replace`
      - :eql:func-desc:`re_replace`

    * - :eql:func:`re_test`
      - :eql:func-desc:`re_test`


----------


.. eql:type:: std::str

    :index: continuation cont

    A unicode string of text.

    Most primitive types (except :eql:type:`bytes`) can be
    :eql:op:`cast <cast>` to and from a string:

    .. code-block:: edgeql-repl

        db> select <str>42;
        {'42'}
        db> select <bool>'true';
        {true}
        db> select "I ❤️ EdgeDB";
        {'I ❤️ EdgeDB'}

    Note that when a :eql:type:`str` is cast into a :eql:type:`json`,
    the result is a JSON string value. The same applies for casting back
    from :eql:type:`json` - only a JSON string value can be cast into
    a :eql:type:`str`:

    .. code-block:: edgeql-repl

        db> select <json>'Hello, world';
        {'"Hello, world"'}

    There are two kinds of string literals in EdgeQL: regular and *raw*.
    Raw string literals do not evaluate ``\``, so ``\n`` in in a raw string
    is two characters ``\`` and ``n``.

    The regular string literal syntax is ``'a string'`` or a ``"a string"``.
    Two *raw* string syntaxes are illustrated below:

    .. code-block:: edgeql-repl

        db> select r'A raw \n string';
        {'A raw \\n string'}
        db> select 'Not a raw \n string';
        {
          'Not a raw
         string',
        }
        db> select $$something$$;
        {'something'}
        db> select $marker$something $$
        ... nested \!$$$marker$;
        {'something $$
        nested \!$$'}

    Regular strings use ``\`` to indicate line continuation. When a
    line continuation symbol is encountered, the symbol itself as well
    as all the whitespace characters up to the next non-whitespace
    character are omitted from the string:

    .. code-block:: edgeql-repl

        db> select 'Hello, \
        ...         world';
        {'"Hello, world"'}

    .. note::

        This type is subject to `the Postgres maximum field size`_
        of 1GB.

    .. versionadded:: 6.0

    Regular strings may use ``\(expr)`` to interpolate the value of
    ``expr`` into the string. The value will be cast to ``str`` if it
    is not already. For example:

    .. code-block:: edgeql-repl

        db> select '1 + 1 = \(1+1)';
        {'1 + 1 = 2'}
        db> select User { name := '\(.first_name) \(.last_name)' };
        {
          default::User {
            name := 'Keanu Reeves',
          },
          ...
        }


.. lint-off
.. _the Postgres maximum field size: https://wiki.postgresql.org/wiki/FAQ#What_is_the_maximum_size_for_a_row.2C_a_table.2C_and_a_database.3F>
.. lint-on

----------


.. eql:operator:: stridx: str [ int64 ] -> str

    String indexing.

    Indexing starts at 0. Negative indexes are also valid and count from
    the *end* of the string.

    .. code-block:: edgeql-repl

        db> select 'some text'[1];
        {'o'}
        db> select 'some text'[-1];
        {'t'}

    It is an error to attempt to extract a character at an index
    outside the bounds of the string:

    .. code-block:: edgeql-repl

        db> select 'some text'[8];
        {'t'}
        db> select 'some text'[9];
        InvalidValueError: string index 9 is out of bounds

    A slice up to the next index can be used if an empty string is
    preferred to an error when outside the bounds of the string:

    .. code-block:: edgeql-repl

        db> select 'some text'[8:9];
        {'t'}
        db> select 'some text'[9:10];
        {''}


----------


.. eql:operator:: strslice: str [ int64 : int64 ] -> str

    String slicing.

    Indexing starts at 0. Negative indexes are also valid and count from
    the *end* of the string.

    .. code-block:: edgeql-repl

        db> select 'some text'[1:3];
        {'om'}
        db> select 'some text'[-4:];
        {'text'}
        db> select 'some text'[:-5];
        {'some'}
        db> select 'some text'[5:-2];
        {'te'}

    It is perfectly acceptable to use indexes outside the bounds of a
    string in a *slice*:

    .. code-block:: edgeql-repl

        db> select 'some text'[-4:100];
        {'text'}
        db> select 'some text'[-100:-5];
        {'some'}


----------


.. eql:operator:: strplus: str ++ str -> str

    String concatenation.

    .. code-block:: edgeql-repl

        db> select 'some' ++ ' text';
        {'some text'}


----------


.. eql:operator:: like: str like str -> bool
                        str not like str -> bool

    Case-sensitive simple string matching.

    Returns ``true`` if the *value* (the ``str`` on the left) matches the
    *pattern* (the ``str`` on the right) and ``false`` otherwise. The operator
    ``not like`` is the negation of ``like``.

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
    the *pattern*, the operators ``like`` and ``not
    like`` work identical to :eql:op:`= <eq>` and :eql:op:`\!= <neq>`,
    respectively.

    .. code-block:: edgeql-repl

        db> select 'abc' like 'abc';
        {true}
        db> select 'abc' like 'a%';
        {true}
        db> select 'abc' like '_b_';
        {true}
        db> select 'abc' like 'c';
        {false}
        db> select 'a%%c' not like r'a\%c';
        {true}


----------


.. eql:operator:: ilike: str ilike str -> bool
                         str not ilike str -> bool

    Case-insensitive simple string matching.

    The operators ``ilike`` and ``not ilike`` work
    the same way as :eql:op:`like` and :eql:op:`not like<like>`,
    except that the *pattern* is matched in a case-insensitive manner.

    .. code-block:: edgeql-repl

        db> select 'Abc' ilike 'a%';
        {true}


----------


.. eql:function:: std::str_lower(string: str) -> str

    Returns a lowercase copy of the input string.

    .. code-block:: edgeql-repl

        db> select str_lower('Some Fancy Title');
        {'some fancy title'}


----------


.. eql:function:: std::str_upper(string: str) -> str

    Returns an uppercase copy of the input string.

    .. code-block:: edgeql-repl

        db> select str_upper('Some Fancy Title');
        {'SOME FANCY TITLE'}


----------


.. eql:function:: std::str_title(string: str) -> str

    Returns a titlecase copy of the input string.

    Every word in the string will have the first letter capitalized
    and the rest converted to lowercase.

    .. code-block:: edgeql-repl

        db> select str_title('sOmE fAnCy TiTlE');
        {'Some Fancy Title'}


----------


.. eql:function:: std::str_pad_start(string: str, n: int64, fill: str = ' ') \
                    -> str

    Returns the input string padded at the start to the length *n*.

    If the string is longer than *n*, then it is truncated to the
    first *n* characters. Otherwise, the string is padded on the
    left up to the total length *n* using *fill* characters (space by
    default).

    .. code-block:: edgeql-repl

        db> select str_pad_start('short', 10);
        {'     short'}
        db> select str_pad_start('much too long', 10);
        {'much too l'}
        db> select str_pad_start('short', 10, '.:');
        {'.:.:.short'}


----------


.. eql:function:: std::str_pad_end(string: str, n: int64, fill: str = ' ') \
                    -> str

    Returns the input string padded at the end to the length *n*.

    If the string is longer than *n*, then it is truncated to the
    first *n* characters. Otherwise, the string is padded on the
    right up to the total length *n* using *fill* characters (space by
    default).

    .. code-block:: edgeql-repl

        db> select str_pad_end('short', 10);
        {'short     '}
        db> select str_pad_end('much too long', 10);
        {'much too l'}
        db> select str_pad_end('short', 10, '.:');
        {'short.:.:.'}


----------


.. eql:function:: std::str_trim_start(string: str, trim: str = ' ') -> str

    Returns the input string with all *trim* characters removed
    from the start.

    If *trim* specifies more than one character they will be removed from
    the beginning of the string regardless of the order in which they appear.

    .. code-block:: edgeql-repl

        db> select str_trim_start('     data');
        {'data'}
        db> select str_trim_start('.....data', '.:');
        {'data'}
        db> select str_trim_start(':::::data', '.:');
        {'data'}
        db> select str_trim_start(':...:data', '.:');
        {'data'}
        db> select str_trim_start('.:.:.data', '.:');
        {'data'}


----------


.. eql:function:: std::str_trim_end(string: str, trim: str = ' ') -> str

    Returns the input string with all *trim* characters removed from the end.

    If *trim* specifies more than one character they will be removed from
    the end of the string regardless of the order in which they appear.

    .. code-block:: edgeql-repl

        db> select str_trim_end('data     ');
        {'data'}
        db> select str_trim_end('data.....', '.:');
        {'data'}
        db> select str_trim_end('data:::::', '.:');
        {'data'}
        db> select str_trim_end('data:...:', '.:');
        {'data'}
        db> select str_trim_end('data.:.:.', '.:');
        {'data'}


----------


.. eql:function:: std::str_trim(string: str, trim: str = ' ') -> str

    Returns the input string with *trim* characters removed from both ends.

    If *trim* specifies more than one character they will be removed from
    both ends of the string regardless of the order in which they appear.
    This is the same as applying
    :eql:func:`str_trim_start` and :eql:func:`str_trim_end`.

    .. code-block:: edgeql-repl

        db> select str_trim('  data     ');
        {'data'}
        db> select str_trim('::data.....', '.:');
        {'data'}
        db> select str_trim('..data:::::', '.:');
        {'data'}
        db> select str_trim('.:data:...:', '.:');
        {'data'}
        db> select str_trim(':.:.data.:.', '.:');
        {'data'}


----------


.. eql:function:: std::str_repeat(string: str, n: int64) -> str

    Repeats the input string *n* times.

    An empty string is returned if *n* is zero or negative.

    .. code-block:: edgeql-repl

        db> select str_repeat('.', 3);
        {'...'}
        db> select str_repeat('foo', -1);
        {''}


----------


.. eql:function:: std::str_replace(s: str, old: str, new: str) -> str

    Replaces all occurrences of a substring with a new one.

    Given a string *s*, finds all non-overlapping occurrences of the
    substring *old* and replaces them with the substring *new*.

    .. code-block:: edgeql-repl

        db> select str_replace('hello world', 'h', 'H');
        {'Hello world'}
        db> select str_replace('hello world', 'l', '[L]');
        {'he[L][L]o wor[L]d'}
        db> select str_replace('hello world', 'o', '😄');
        {'hell😄 w😄rld'}


----------


.. eql:function:: std::str_reverse(string: str) -> str

    Reverses the order of the characters in the string.

    .. code-block:: edgeql-repl

        db> select str_reverse('Hello world');
        {'dlrow olleH'}
        db> select str_reverse('Hello 👋 world 😄');
        {'😄 dlrow 👋 olleH'}


----------


.. eql:function:: std::str_split(s: str, delimiter: str) -> array<str>

    :index: split str_split explode

    Splits a string into array elements using the supplied delimiter.

    .. code-block:: edgeql-repl

        db> select str_split('1, 2, 3', ', ');
        {['1', '2', '3']}

    .. code-block:: edgeql-repl

        db> select str_split('123', '');
        {['1', '2', '3']}


----------


.. eql:function:: std::re_match(pattern: str, \
                                string: str) -> array<str>

    :index: regex regexp regular

    Finds the first regular expression match in a string.

    Given an input string and a regular expression :ref:`pattern
    <string_regexp>`, finds the first match for the regular expression
    within the string. Each match returned is represented by an
    :eql:type:`array\<str\>` of matched groups.

    .. code-block:: edgeql-repl

        db> select re_match(r'\w{4}ql', 'I ❤️ edgeql');
        {['edgeql']}


----------


.. eql:function:: std::re_match_all(pattern: str, \
                                    string: str) -> set of array<str>

    :index: regex regexp regular

    Finds all regular expression matches in a string.

    Given an input string and a regular expression :ref:`pattern
    <string_regexp>`, repeatedly matches the regular expression within
    the string. Returns the set of all matches, with each match
    represented by an :eql:type:`array\<str\>` of matched groups.

    .. code-block:: edgeql-repl

        db> select re_match_all(r'a\w+', 'an abstract concept');
        {['an'], ['abstract']}


----------


.. eql:function:: std::re_replace(pattern: str, sub: str, \
                                  string: str, \
                                  named only flags: str='') \
                  -> str

    :index: regex regexp regular replace

    Replaces matching substrings in a given string.

    Takes an input string and a regular expression :ref:`pattern
    <string_regexp>`, replacing matching substrings with the replacement
    string *sub*. Optional :ref:`flag arguments<string_regexp_flags>`
    can be used to specify additional regular expression flags.

    .. code-block:: edgeql-repl

        db> select re_replace('a', 'A', 'Alabama');
        {'AlAbama'}
        db> select re_replace('a', 'A', 'Alabama', flags := 'g');
        {'AlAbAmA'}
        db> select re_replace('A', 'A', 'Alabama', flags := 'ig');
        {'AlAbAmA'}


----------


.. eql:function:: std::re_test(pattern: str, string: str) -> bool

    :index: regex regexp regular match

    Tests if a regular expression has a match in a string.

    Given an input string and a regular expression :ref:`pattern
    <string_regexp>`, tests whether there is a match for the regular
    expression within the string. Returns ``true`` if there is a
    match, ``false`` otherwise.

    .. code-block:: edgeql-repl

        db> select re_test(r'a', 'abc');
        {true}


------------


.. eql:function:: std::to_str(val: datetime, fmt: optional str={}) -> str
                  std::to_str(val: duration, fmt: optional str={}) -> str
                  std::to_str(val: int64, fmt: optional str={}) -> str
                  std::to_str(val: float64, fmt: optional str={}) -> str
                  std::to_str(val: bigint, fmt: optional str={}) -> str
                  std::to_str(val: decimal, fmt: optional str={}) -> str
                  std::to_str(val: json, fmt: optional str={}) -> str
                  std::to_str(val: bytes) -> str
                  std::to_str(val: cal::local_datetime, \
                              fmt: optional str={}) -> str
                  std::to_str(val: cal::local_date, \
                              fmt: optional str={}) -> str
                  std::to_str(val: cal::local_time, \
                              fmt: optional str={}) -> str

    :index: stringify dumps join array_to_string decode TextDecoder

    Returns the string representation of the input value.

    A versatile polymorphic function defined for different input types,
    ``to_str`` uses corresponding converter functions from :eql:type:`str`
    to specific types via the format argument *fmt*.

    When converting :eql:type:`bytes`, :eql:type:`datetime`,
    :eql:type:`cal::local_datetime`, :eql:type:`cal::local_date`,
    :eql:type:`cal::local_time`, :eql:type:`duration` this function
    is the inverse of :eql:func:`to_bytes`, :eql:func:`to_datetime`,
    :eql:func:`cal::to_local_datetime`, :eql:func:`cal::to_local_date`,
    :eql:func:`cal::to_local_time`, :eql:func:`to_duration`, correspondingly.

    For valid date and time formatting patterns see
    :ref:`here <ref_std_converters_datetime_fmt>`.

    .. code-block:: edgeql-repl

        db> select to_str(<datetime>'2018-05-07 15:01:22.306916-05',
        ...               'FMDDth "of" FMMonth, YYYY');
        {'7th of May, 2018'}
        db> select to_str(<cal::local_date>'2018-05-07', 'CCth "century"');
        {'21st century'}

    .. note::

        If you want to use literal text in your format string, it's best to
        enclose it in double quotes as shown above with ``of`` and
        ``century``.

    When converting one of the numeric types, this function is the
    reverse of: :eql:func:`to_bigint`, :eql:func:`to_decimal`,
    :eql:func:`to_int16`, :eql:func:`to_int32`, :eql:func:`to_int64`,
    :eql:func:`to_float32`, :eql:func:`to_float64`.

    For valid number formatting patterns see
    :ref:`here <ref_std_converters_number_fmt>`.

    See also :eql:func:`to_json`.

    .. code-block:: edgeql-repl

        db> select to_str(123, '999999');
        {'    123'}
        db> select to_str(123, '099999');
        {' 000123'}
        db> select to_str(123.45, 'S999.999');
        {'+123.450'}
        db> select to_str(123.45e-20, '9.99EEEE');
        {' 1.23e-18'}
        db> select to_str(-123.45n, 'S999.99');
        {'-123.45'}

    When converting :eql:type:`json`, this function can take
    ``'pretty'`` as an optional *fmt* argument to produce a
    pretty-formatted JSON string.

    See also :eql:func:`to_json`.

    .. code-block:: edgeql-repl

        db> select to_str(<json>2);
        {'2'}

        db> select to_str(<json>['hello', 'world']);
        {'["hello", "world"]'}

        db> select to_str(<json>(a := 2, b := 'hello'), 'pretty');
        {'{
            "a": 2,
            "b": "hello"
        }'}

    When converting :eql:type:`arrays <array>`, a *delimiter* argument
    is required:

    .. code-block:: edgeql-repl

        db> select to_str(['one', 'two', 'three'], ', ');
        {'one, two, three'}

    .. warning::

        A version of ``std::to_str`` exists which operates on arrays but has
        been deprecated; :eql:func:`array_join` should be used instead.

    A :eql:type:`bytes` value can be converted to a :eql:type:`str` using
    UTF-8 encoding. Returns an InvalidValueError if input UTF-8 is invalid.

    .. code-block:: edgeql-repl

        db> select to_str(b'\xe3\x83\x86');
        {'テ'}
        db> select to_str(b'\xe3\x83');
        edgedb error: InvalidValueError: invalid byte sequence for
        encoding "UTF8": 0xe3 0x83


----------


.. _string_regexp:

Regular Expressions
-------------------

EdgeDB supports Regular expressions (REs), as defined in POSIX 1003.2.
They come in two forms: BRE (basic RE) and ERE (extended RE). In
addition, EdgeDB supports certain common extensions to the POSIX
standard commonly known as ARE (advanced RE). More details about
BRE, ERE, and ARE support can be found in `PostgreSQL documentation`_.


.. _`PostgreSQL documentation`:
                https://www.postgresql.org/docs/10/static/
                functions-matching.html#POSIX-SYNTAX-DETAILS

The table below outlines the different options accepted as the ``flags``
argument to various regular expression functions, or as
`embedded options`_ in the pattern itself, e.g. ``'(?i)fooBAR'``:

.. _`embedded options`:
  https://www.postgresql.org/docs/10/functions-matching.html#POSIX-METASYNTAX

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
``x``   expanded syntax ignoring whitespace characters
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
    INCLUDING, BUT not LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
    AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
    ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
    PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.


Some of the type converter functions take an extra argument specifying
formatting (either for converting to a :eql:type:`str` or parsing from one).
The different formatting options are collected in this section.


.. _ref_std_converters_datetime_fmt:

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
|                         | (only supported in :eql:func:`to_str`) |
+-------------------------+----------------------------------------+
| tz                      | lower case time-zone abbreviation      |
|                         | (only supported in :eql:func:`to_str`) |
+-------------------------+----------------------------------------+
| TZH                     | time-zone hours                        |
+-------------------------+----------------------------------------+
| TZM                     | time-zone minutes                      |
+-------------------------+----------------------------------------+
| OF                      | time-zone offset from UTC (only        |
|                         | supported in :eql:func:`to_str`)       |
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

Whitespace is normally ignored when parsing string input, unless
the *FX* prefix modifier is used. For example:

.. code-block:: edgeql-repl

    db> select cal::to_local_date(
    ...     '2000    JUN', 'YYYY MON');
    {<cal::local_date>'2000-06-01'}
    db> select cal::to_local_date(
    ...     '2000    JUN', 'FXYYYY MON');
    InternalServerError: invalid value "   " for "MON"

Ordinary text is allowed in :eql:func:`to_str` format strings and will be
output literally.  For better compatibility you should put a plain substring
in double quotes to force it to be interpreted as literal text even if it
contains template patterns now or could in the future.

.. _ref_std_converters_number_fmt:

Number formatting options
^^^^^^^^^^^^^^^^^^^^^^^^^

+------------+-----------------------------------------------------+
| Pattern    | Description                                         |
+============+=====================================================+
| 9          | digit position (can be dropped if insignificant)    |
+------------+-----------------------------------------------------+
| 0          | digit position (will not be dropped even if         |
|            | insignificant)                                      |
+------------+-----------------------------------------------------+
| .          | (period)  decimal point                             |
+------------+-----------------------------------------------------+
| ,          | (comma)   group (thousands) separator               |
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
