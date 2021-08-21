.. _ref_datamodel_scalars_bytes:

=====
Bytes
=====

:edb-alt-title: Bytes Type


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


See Also
--------

Scalar type
:ref:`SDL <ref_eql_sdl_scalars>`,
:ref:`DDL <ref_eql_ddl_scalars>`,
:ref:`introspection <ref_eql_introspection_scalar_types>`,
:ref:`bytes functions and operators <ref_std_bytes>`,
and :ref:`bytes literal lexical structure <ref_eql_lexical_bytes>`.
