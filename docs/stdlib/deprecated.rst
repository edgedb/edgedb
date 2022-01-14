.. _ref_std_deprecated:

==========
Deprecated
==========

:edb-alt-title: Deprecated Functions


.. list-table::
    :class: funcoptable

    * - :eql:func:`str_lpad`
      - :eql:func-desc:`str_lpad`

    * - :eql:func:`str_rpad`
      - :eql:func-desc:`str_rpad`

    * - :eql:func:`str_ltrim`
      - :eql:func-desc:`str_ltrim`

    * - :eql:func:`str_rtrim`
      - :eql:func-desc:`str_rtrim`


----------


.. eql:function:: std::str_lpad(string: str, n: int64, fill: str = ' ') -> str

    Return the input *string* left-padded to the length *n*.

    .. warning::

        This function is deprecated. Use
        :eql:func:`std::str_pad_start` instead.

    If the *string* is longer than *n*, then it is truncated to the
    first *n* characters. Otherwise, the *string* is padded on the
    left up to the total length *n* using *fill* characters (space by
    default).

    .. code-block:: edgeql-repl

        db> select str_lpad('short', 10);
        {'     short'}
        db> select str_lpad('much too long', 10);
        {'much too l'}
        db> select str_lpad('short', 10, '.:');
        {'.:.:.short'}


----------


.. eql:function:: std::str_rpad(string: str, n: int64, fill: str = ' ') -> str

    Return the input *string* right-padded to the length *n*.

    .. warning::

        This function is deprecated. Use
        :eql:func:`std::str_pad_end` instead.

    If the *string* is longer than *n*, then it is truncated to the
    first *n* characters. Otherwise, the *string* is padded on the
    right up to the total length *n* using *fill* characters (space by
    default).

    .. code-block:: edgeql-repl

        db> select str_rpad('short', 10);
        {'short     '}
        db> select str_rpad('much too long', 10);
        {'much too l'}
        db> select str_rpad('short', 10, '.:');
        {'short.:.:.'}


----------


.. eql:function:: std::str_ltrim(string: str, trim: str = ' ') -> str

    Return the input string with all leftmost *trim* characters removed.

    .. warning::

        This function is deprecated. Use
        :eql:func:`std::str_trim_start` instead.

    If the *trim* specifies more than one character they will be
    removed from the beginning of the *string* regardless of the order
    in which they appear.

    .. code-block:: edgeql-repl

        db> select str_ltrim('     data');
        {'data'}
        db> select str_ltrim('.....data', '.:');
        {'data'}
        db> select str_ltrim(':::::data', '.:');
        {'data'}
        db> select str_ltrim(':...:data', '.:');
        {'data'}
        db> select str_ltrim('.:.:.data', '.:');
        {'data'}


----------


.. eql:function:: std::str_rtrim(string: str, trim: str = ' ') -> str

    Return the input string with all rightmost *trim* characters removed.

    .. warning::

        This function is deprecated. Use
        :eql:func:`std::str_trim_end` instead.

    If the *trim* specifies more than one character they will be
    removed from the end of the *string* regardless of the order
    in which they appear.

    .. code-block:: edgeql-repl

        db> select str_rtrim('data     ');
        {'data'}
        db> select str_rtrim('data.....', '.:');
        {'data'}
        db> select str_rtrim('data:::::', '.:');
        {'data'}
        db> select str_rtrim('data:...:', '.:');
        {'data'}
        db> select str_rtrim('data.:.:.', '.:');
        {'data'}
