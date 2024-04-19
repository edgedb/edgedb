.. _ref_std_bytes:

=====
Bytes
=====

:edb-alt-title: Bytes Functions and Operators

.. list-table::
    :class: funcoptable

    * - :eql:type:`bytes`
      - Byte sequence

    * - :eql:type:`Endian`
      - An enum for indicating integer value encoding.

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

    * - :eql:func:`to_str`
      - :eql:func-desc:`to_str`

    * - :eql:func:`to_int16`
      - :eql:func-desc:`to_int16`

    * - :eql:func:`to_int32`
      - :eql:func-desc:`to_int32`

    * - :eql:func:`to_int64`
      - :eql:func-desc:`to_int64`

    * - :eql:func:`to_uuid`
      - :eql:func-desc:`to_uuid`

    * - :eql:func:`bytes_get_bit`
      - :eql:func-desc:`bytes_get_bit`

    * - :eql:func:`bit_count`
      - :eql:func-desc:`bit_count`

    * - :eql:func:`enc::base64_encode`
      - :eql:func-desc:`enc::base64_encode`

    * - :eql:func:`enc::base64_decode`
      - :eql:func-desc:`enc::base64_decode`

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


.. eql:type:: std::Endian

    .. versionadded:: 5.0

    An enum for indicating integer value encoding.

    This enum is used by the :eql:func:`to_int16`, :eql:func:`to_int32`,
    :eql:func:`to_int64` and the :eql:func:`to_bytes` converters working with
    :eql:type:`bytes` and integers.

    ``Endian.Big`` stands for big-endian encoding going from most significant
    byte to least. ``Endian.Little`` stands for little-endian encoding going
    from least to most significant byte.

    .. code-block:: edgeql-repl

        db> select to_bytes(<int32>16908295, Endian.Big);
        {b'\x01\x02\x00\x07'}
        db> select to_int32(b'\x01\x02\x00\x07', Endian.Big);
        {16908295}
        db> select to_bytes(<int32>16908295, Endian.Little);
        {b'\x07\x00\x02\x01'}
        db> select to_int32(b'\x07\x00\x02\x01', Endian.Little);
        {16908295}


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

.. TODO: Function signatures except the first need to be revealed only for v5+

.. eql:function:: std::to_bytes(s: str) -> bytes
                  std::to_bytes(val: int16, endian: Endian) -> bytes
                  std::to_bytes(val: int32, endian: Endian) -> bytes
                  std::to_bytes(val: int64, endian: Endian) -> bytes
                  std::to_bytes(val: uuid) -> bytes

    :index: encode stringencoder

    .. versionadded:: 4.0

    Converts a given value into binary representation as :eql:type:`bytes`.

    The strings get converted using UTF-8 encoding:

    .. code-block:: edgeql-repl

        db> select to_bytes('テキスト');
        {b'\xe3\x83\x86\xe3\x82\xad\xe3\x82\xb9\xe3\x83\x88'}

    The integer values can be encoded as big-endian (most significant bit
    comes first) byte strings:

    .. code-block:: edgeql-repl

        db> select to_bytes(<int16>31, Endian.Big);
        {b'\x00\x1f'}
        db> select to_bytes(<int32>31, Endian.Big);
        {b'\x00\x00\x00\x1f'}
        db> select to_bytes(123456789123456789, Endian.Big);
        {b'\x01\xb6\x9bK\xac\xd0_\x15'}

    .. note::

        Due to underlying implementation details using big-endian encoding
        results in slightly faster performance of ``to_bytes`` when converting
        integers.

    The UUID values are converted to the underlying string of 16 bytes:

    .. code-block:: edgeql-repl

        db> select to_bytes(<uuid>'1d70c86e-cc92-11ee-b4c7-a7aa0a34e2ae');
        {b'\x1dp\xc8n\xcc\x92\x11\xee\xb4\xc7\xa7\xaa\n4\xe2\xae'}

    To perform the reverse conversion there are corresponding functions:
    :eql:func:`to_str`, :eql:func:`to_int16`, :eql:func:`to_int32`,
    :eql:func:`to_int64`, :eql:func:`to_uuid`.


---------

.. eql:function:: std::bytes_get_bit(bytes: bytes, nth: int64) -> int64

    Returns the specified bit of the :eql:type:`bytes` value.

    When looking for the *nth* bit, this function will enumerate bits from
    least to most significant in each byte.

    .. code-block:: edgeql-repl

        db> for n in {0, 1, 2, 3, 4, 5, 6, 7,
        ...           8, 9, 10, 11, 12, 13 ,14, 15}
        ... union bytes_get_bit(b'ab', n);
        {1, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 1, 1, 0}


---------


.. eql:function:: enc::base64_encode(b: bytes) -> str

    .. versionadded:: 4.0

    Returns a Base64-encoded :eql:type:`str` of the :eql:type:`bytes` value.

    .. code-block:: edgeql-repl

        db> select enc::base64_encode(b'hello');
        {'aGVsbG8='}

---------

.. eql:function:: enc::base64_decode(s: str) -> bytes

    .. versionadded:: 4.0

    Returns the :eql:type:`bytes` of a Base64-encoded :eql:type:`str`.

    Returns an InvalidValueError if input is not valid Base64.

    .. code-block:: edgeql-repl

        db> select enc::base64_decode('aGVsbG8=');
        {b'hello'}
        db> select enc::base64_decode('aGVsbG8');
        edgedb error: InvalidValueError: invalid base64 end sequence
