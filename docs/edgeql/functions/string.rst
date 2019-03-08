.. _ref_eql_functions_string:


String
======

.. eql:function:: std::str_lower(string: str) -> str

    Return a lowercase copy of the input *string*.

    .. code-block:: edgeql-repl

        db> SELECT str_lower('Some Fancy Title');
        {'some fancy title'}

.. eql:function:: std::str_upper(string: str) -> str

    Return an uppercase copy of the input *string*.

    .. code-block:: edgeql-repl

        db> SELECT str_upper('Some Fancy Title');
        {'SOME FANCY TITLE'}

.. eql:function:: std::str_title(string: str) -> str

    Return a titlecase copy of the input *string*.

    Every word in the *string* will have the first letter capitalized
    and the rest converted to lowercase.

    .. code-block:: edgeql-repl

        db> SELECT str_title('sOmE fAnCy TiTlE');
        {'Some Fancy Title'}

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

.. eql:function:: std::str_repeat(string: str, n: int64) -> str

    Repeat the input *string* *n* times.

    If *n* is zero or negative an empty string is returned.

    .. code-block:: edgeql-repl

        db> SELECT str_repeat('.', 3);
        {'...'}
        db> SELECT str_repeat('foo', -1);
        {''}

.. eql:function:: std::re_match(pattern: str, \
                                string: str) -> array<str>

    :index: regex regexp regular

    Find the first regular expression match in a string.

    Given an input *string* and a regular expression *pattern* find
    the first match for the regular expression within the *string*.
    Return the match, each match represented by an
    :eql:type:`array\<str\>` of matched groups.

    .. code-block:: edgeql-repl

        db> SELECT re_match(r'\w{4}ql', 'I ❤️ edgeql');
        {['edgeql']}

.. eql:function:: std::re_match_all(pattern: str, \
                                    string: str) -> SET OF array<str>

    :index: regex regexp regular

    Find all regular expression matches in a string.

    Given an input *string* and a regular expression *pattern*
    repeatedly match the regular expression within the *string*.
    Return the set of all matches, each match represented by an
    :eql:type:`array\<str\>` of matched groups.

    .. code-block:: edgeql-repl

        db> SELECT re_match_all(r'a\w+', 'an abstract concept');
        {['an'], ['abstract']}

.. eql:function:: std::re_replace(pattern: str, sub: str, \
                                  string: str, \
                                  NAMED ONLY flags: str='') \
                  -> str

    :index: regex regexp regular replace

    Replace matching substrings in a given string.

    Given an input *string* and a regular expression *pattern* replace
    matching substrings with the replacement string *sub*. Optional
    :ref:`flag <string_regexp_flags>` argument can be used to specify
    additional regular expression flags. Return the string resulting
    from substring replacement.

    .. code-block:: edgeql-repl

        db> SELECT re_replace(r'l', r'L', 'Hello World',
        ...                   flags := 'g');
        {'HeLLo WorLd'}

.. eql:function:: std::re_test(pattern: str, string: str) -> bool

    :index: regex regexp regular match

    Test if a regular expression has a match in a string.

    Given an input *string* and a regular expression *pattern* test
    whether there is a match for the regular expression within the
    *string*. Return ``true`` if there is a match, ``false``
    otherwise.

    .. code-block:: edgeql-repl

        db> SELECT re_test(r'a', 'abc');
        {true}

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
