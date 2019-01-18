.. _ref_eql_functions_generic:

=======
Generic
=======

This section describes generic functions provided by EdgeDB.


.. eql:function:: std::len(value: str) -> int64
                  std::len(value: bytes) -> int64
                  std::len(value: array<anytype>) -> int64

    :index: length count array

    A polymorphic function to calculate a "length" of its first
    argument.

    Return the number of characters in a :eql:type:`str`, or the
    number of bytes in :eql:type:`bytes`, or the number of elements in
    an :eql:type:`array`.

    .. code-block:: edgeql-repl

        db> SELECT len('foo');
        {3}

        db> SELECT len(b'bar');
        {3}

        db> SELECT len([2, 5, 7]);
        {3}


.. eql:function:: std::find(haystack: str, needle: str) -> int32
                  std::find(haystack: bytes, needle: bytes) -> int32
                  std::find(haystack: array<anytype>, needle: anytype, \
                            from_pos: int64=0) -> int32

    :index: find position array

    A polymorphic function to find index of an element in a sequence.

    When the *haystack* is :eql:type:`str` or :eql:type:`bytes`,
    return the index of the first occurrence of *needle* in it.

    When the *haystack* is an :eql:type:`array`, return the index of
    the first occurrence of the specific *needle* element. For
    :eql:type:`array` inputs it is also possible to provide an
    optional *from_pos* argument to specify the position from
    which to start the search.

    If the *needle* is not found, return ``-1``.

    .. code-block:: edgeql-repl

        db> SELECT find('qwerty', 'we');
        {1}

        db> SELECT find(b'qwerty', b'42');
        {-1}

        db> SELECT find([2, 5, 7, 2, 100], 2);
        {0}

        db> SELECT find([2, 5, 7, 2, 100], 2, 1);
        {3}


.. eql:function:: std::round(value: int64) -> float64
                  std::round(value: float64) -> float64
                  std::round(value: decimal) -> decimal
                  std::round(value: decimal, d: int64) -> decimal

    Round to the nearest value.

    There's a difference in how ties (which way ``0.5`` is rounded)
    are handled depending on the type of the input *value*.

    :eql:type:`float64` tie is rounded to the nearest even number:

    .. code-block:: edgeql-repl

        db> SELECT round(1.2);
        {1}

        db> SELECT round(1.5);
        {2}

        db> SELECT round(2.5);
        {2}

    :eql:type:`decimal` tie is rounded away from 0:

    .. code-block:: edgeql-repl

        db> SELECT round(<decimal>1.2);
        {1}

        db> SELECT round(<decimal>1.5);
        {2}

        db> SELECT round(<decimal>2.5);
        {3}

    Additionally, when rounding a :eql:type:`decimal` *value* an
    optional argument *d* can be provided to specify to what decimal
    point the *value* must to be rounded.

    .. code-block:: edgeql-repl

        db> SELECT round(<decimal>163.278, 2);
        {163.28}

        db> SELECT round(<decimal>163.278, 1);
        {163.3}

        db> SELECT round(<decimal>163.278, 0);
        {163}

        db> SELECT round(<decimal>163.278, -1);
        {160}

        db> SELECT round(<decimal>163.278, -2);
        {200}


.. eql:function:: std::random() -> float64

    Return a pseudo-random number in the range ``0.0 <= x < 1.0``.

    .. code-block:: edgeql-repl

        db> SELECT random();
        {0.62649393780157}


.. eql:function:: std::bytes_get_bit(bytes: bytes, nth: int64) -> int64

    Get the *nth* bit of the *bytes* value.

    When looking for the *nth* bit, this function enumerates bits from
    least to most significant in each byte.

    .. code-block:: edgeql-repl

        db> FOR n IN {0, 1, 2, 3, 4, 5, 6, 7,
        ...           8, 9, 10, 11, 12, 13 ,14, 15}
        ... UNION bytes_get_bit(b'ab', n);
        {1, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 1, 1, 0}


.. eql:function:: std::uuid_generate_v1mc() -> uuid

    Return a version 1 UUID.

    The algorithm uses a random multicast MAC address instead of the
    real MAC address of the computer.

    .. code-block:: edgeql-repl

        db> SELECT uuid_generate_v1mc();
        {'1893e2b6-57ce-11e8-8005-13d4be166783'}
