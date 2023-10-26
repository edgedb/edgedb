.. _ref_std_bytes:

=====
Bytes
=====

:edb-alt-title: Bytes Functions and Operators

.. list-table::
    :class: funcoptable

    * - :eql:type:`bytes`
      - Byte sequence

    * - :eql:op:`bytes[i] <bytesidx>`
      - :eql:op-desc:`bytesidx`

    * - :eql:op:`bytes[from:to] <bytesslice>`
      - :eql:op-desc:`bytesslice`

    * - :eql:op:`bytes ++ bytes <bytesplus>`
      - :eql:op-desc:`bytesplus`

    * - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
        :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
        :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
      - Comparison operators

    * - :eql:func:`len`
      - Returns the number of bytes.

    * - :eql:func:`contains`
      - Checks if the byte sequence contains a given subsequence.

    * - :eql:func:`find`
      - Finds the index of the first occurrence of a subsequence.

    * - :eql:func:`to_bytes`
      - :eql:func-desc:`to_bytes`

    * - :eql:func:`bytes_get_bit`
      - :eql:func-desc:`bytes_get_bit`


----------


.. eql:type:: std::bytes

    A sequence of bytes representing raw data.

    Bytes can be represented as a literal using this syntax: ``b''``.

    .. code-block:: edgeql-repl

        db> select b'Hello, world';
        {b'Hello, world'}
        db> select b'Hello,\x20world\x01';
        {b'Hello, world\x01'}

    There are also some :ref:`generic <ref_std_generic>`
    functions that can operate on bytes:

    .. code-block:: edgeql-repl

        db> select contains(b'qwerty', b'42');
        {false}

    Bytes are rendered as base64-encoded strings in JSON. When you cast a
    ``bytes`` value into JSON, that's what you'll get. In order to
    :eql:op:`cast <cast>` a :eql:type:`json` value into bytes, it must be a
    base64-encoded string.

    .. code-block:: edgeql-repl

        db> select <json>b'Hello EdgeDB!';
        {"\"SGVsbG8gRWRnZURCIQ==\""}
        db> select <bytes>to_json("\"SGVsbG8gRWRnZURCIQ==\"");
        {b'Hello EdgeDB!'}

----------


.. eql:operator:: bytesidx: bytes [ int64 ] -> bytes

    Accesses a byte at a given index.

    Examples:

    .. code-block:: edgeql-repl

        db> select b'binary \x01\x02\x03\x04 ftw!'[2];
        {b'n'}
        db> select b'binary \x01\x02\x03\x04 ftw!'[8];
        {b'\x02'}


----------


.. eql:operator:: bytesslice: bytes [ int64 : int64 ] -> bytes

    Produces a bytes sub-sequence from an existing bytes value.

    Examples:

    .. code-block:: edgeql-repl

        db> select b'\x01\x02\x03\x04 ftw!'[2:-1];
        {b'\x03\x04 ftw'}
        db> select b'some bytes'[2:-3];
        {b'me by'}


---------


.. eql:operator:: bytesplus: bytes ++ bytes -> bytes

    Concatenates two bytes values into one.

    .. code-block:: edgeql-repl

        db> select b'\x01\x02' ++ b'\x03\x04';
        {b'\x01\x02\x03\x04'}


---------

.. eql:function:: std::to_bytes(s: str) -> bytes

    :index: encode stringencoder

    Convert a :eql:type:`str` value to :eql:type:`bytes` using UTF-8 encoding.

    .. code-block:: edgeql-repl

        db> select to_bytes('テキスト');
        {b'\xe3\x83\x86\xe3\x82\xad\xe3\x82\xb9\xe3\x83\x88'}

---------

.. eql:function:: std::bytes_get_bit(bytes: bytes, nth: int64) -> int64

    Returns the specified bit of the bytes value.

    When looking for the *nth* bit, this function will enumerate bits from
    least to most significant in each byte.

    .. code-block:: edgeql-repl

        db> for n in {0, 1, 2, 3, 4, 5, 6, 7,
        ...           8, 9, 10, 11, 12, 13 ,14, 15}
        ... union bytes_get_bit(b'ab', n);
        {1, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 1, 1, 0}
