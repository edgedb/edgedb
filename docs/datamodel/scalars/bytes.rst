.. _ref_datamodel_scalars_bytes:

Bytes
=====

.. eql:type:: std::bytes

    A sequence of bytes.

    Bytes cannot be cast into any other type. They represent raw data.

    There's a special byte literal:

    .. code-block:: edgeql-repl

        db> SELECT b'Hello, world';
        {b'Hello, world'}
        db> SELECT b'Hello,\x20world\x01';
        {b'Hello, world\x01'}

    There are also some :ref:`generic <ref_eql_functions_generic>`
    functions that can operate on bytes:

    .. code-block:: edgeql-repl

        db> SELECT contains(b'qwerty', b'42');
        {false}
