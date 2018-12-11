.. _ref_eql_functions_generic:

=======
Generic
=======

This section describes mathematical functions
provided by EdgeDB.


.. eql:function:: std::len(value: anytype) -> int64

    :index: length count array

    A polymorphic function to calculate a "length" of its first
    argument.

    Return the number of characters in a :eql:type:`str`, or the
    number of bytes in :eql:type:`bytes`, or the number of elements in
    an :eql:type:`array`.

    .. code-block:: edgeql-repl

        db> SELECT len('foo');
        {3}

        db> SELECT len([2, 5, 7]);
        {3}


.. eql:function:: std::random() -> float64

    Return a pseudo-random number in the range ``0.0 <= x < 1.0``.

    .. code-block:: edgeql-repl

        db> SELECT std::random();
        {0.62649393780157}


.. eql:function:: std::uuid_generate_v1mc() -> uuid

    Return a version 1 UUID.

    The algorithm uses a random multicast MAC address instead of the
    real MAC address of the computer.

    .. code-block:: edgeql-repl

        db> SELECT std::uuid_generate_v1mc();
        {'1893e2b6-57ce-11e8-8005-13d4be166783'}
