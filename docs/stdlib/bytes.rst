.. _ref_std_bytes:

=====
Bytes
=====

:edb-alt-title: Bytes Functions and Operators

.. list-table::
    :class: funcoptable

    * - :eql:type:`bytes`
      - Byte sequence

    * - :eql:op:`bytes[i] <BYTESIDX>`
      - :eql:op-desc:`BYTESIDX`

    * - :eql:op:`bytes[from:to] <BYTESSLICE>`
      - :eql:op-desc:`BYTESSLICE`

    * - :eql:op:`bytes ++ bytes <BYTEPLUS>`
      - :eql:op-desc:`BYTEPLUS`

    * - :eql:op:`bytes = bytes <EQ>`, :eql:op:`bytes \< bytes <LT>`, ...
      - Comparison operators.

    * - :eql:func:`bytes_get_bit`
      - :eql:func-desc:`bytes_get_bit`


----------


.. eql:type:: std::bytes

    A sequence of bytes.

    Bytes cannot be cast into any other type. They represent raw data.

    There's a special byte literal:

    .. code-block:: edgeql-repl

        db> SELECT b'Hello, world';
        {b'Hello, world'}
        db> SELECT b'Hello,\x20world\x01';
        {b'Hello, world\x01'}

    There are also some :ref:`generic <ref_std_generic>`
    functions that can operate on bytes:

    .. code-block:: edgeql-repl

        db> SELECT contains(b'qwerty', b'42');
        {false}


----------


.. eql:operator:: BYTESIDX: bytes [ int64 ] -> bytes

    Bytes indexing.

    Examples:

    .. code-block:: edgeql-repl

        db> SELECT b'binary \x01\x02\x03\x04 ftw!'[8];
        {b'\x02'}


----------


.. eql:operator:: BYTESSLICE: bytes [ int64 : int64 ] -> bytes

    Bytes slicing.

    Examples:

    .. code-block:: edgeql-repl

        db> SELECT b'\x01\x02\x03\x04 ftw!'[2:-1];
        {b'\x03\x04 ftw'}
        db> SELECT b'some bytes'[2:-3];
        {b'me by'}


---------


.. eql:operator:: BYTEPLUS: bytes ++ bytes -> bytes

    Bytes concatenation.

    .. code-block:: edgeql-repl

        db> SELECT b'\x01\x02' ++ b'\x03\x04';
        {b'\x01\x02\x03\x04'}


---------


.. eql:function:: std::bytes_get_bit(bytes: bytes, nth: int64) -> int64

    Get the *nth* bit of the *bytes* value.

    When looking for the *nth* bit, this function enumerates bits from
    least to most significant in each byte.

    .. code-block:: edgeql-repl

        db> FOR n IN {0, 1, 2, 3, 4, 5, 6, 7,
        ...           8, 9, 10, 11, 12, 13 ,14, 15}
        ... UNION bytes_get_bit(b'ab', n);
        {1, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 1, 1, 0}
