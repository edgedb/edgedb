.. _ref_eql_functions_string:


String
======

.. eql:function:: std::lower(string: str) -> str

    Return a lowercase copy of the input string.

    .. code-block:: edgeql-repl

        db> SELECT lower('Some Fancy Title');
        {'some fancy title'}

.. eql:function:: std::str_to_json(string: str) -> json

    :index: json parse loads

    Return JSON value represented by the input string.

    This is the reverse of :eql:func:`json_to_str`.

    .. code-block:: edgeql-repl

        db> SELECT str_to_json('[1, "foo", null]');
        {[1, 'foo', None]}

        db> SELECT str_to_json('{"hello": "world"}');
        {{hello: 'world'}}

.. eql:function:: std::re_match(pattern: str, \
                                string: str) -> array<str>

    :index: regex regexp regular

    Find the first regular expression match in a string.

    Given an input *string* and a regular expression *pattern* find
    the first match for the regular expression within the *string*.
    Return the match, each match represented by an
    :eql:type:`array\<str\>` of matched groups.

    .. code-block:: edgeql-repl

        db> SELECT std::re_match(r'\w{4}ql', 'I ❤️ edgeql');
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

        db> SELECT std::re_match_all(r'a\w+', 'an abstract concept');
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

        db> SELECT std::re_replace(r'l', r'L', 'Hello World',
                                   flags := 'g');
        {'HeLLo WorLd'}

.. eql:function:: std::re_test(pattern: str, string: str) -> bool

    :index: regex regexp regular match

    Test if a regular expression has a match in a string.

    Given an input *string* and a regular expression *pattern* test
    whether there is a match for the regular expression within the
    *string*. Return ``True`` if there is a match, ``False``
    otherwise.

    .. code-block:: edgeql-repl

        db> SELECT std::re_test(r'a', 'abc');
        {True}

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
