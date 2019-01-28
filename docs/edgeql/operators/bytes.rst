.. _ref_eql_operators_bytes:

=====
Bytes
=====

Much like with :ref:`arrays <ref_eql_expr_array_elref>` individual bytes
and bytes slices can be produced by using ``[]``:

.. code-block:: edgeql-repl

    db> SELECT b'some bytes'[1];
    {b'o'}
    db> SELECT b'some bytes'[1:3];
    {b'om'}
    db> SELECT b'some bytes'[2:-3];
    {b'me by'}

.. eql:operator:: BYTEPLUS: A ++ B

    :optype A: bytes
    :optype B: bytes
    :resulttype: bytes

    Bytes concatenation.

    .. code-block:: edgeql-repl

        db> SELECT b'some' ++ b' text';
        {b'some text'}
