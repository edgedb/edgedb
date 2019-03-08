.. _ref_eql_operators_string:

======
String
======

Much like with :ref:`arrays <ref_eql_expr_array_elref>` string
elements and string slices can be produced by using ``[]``:

.. code-block:: edgeql-repl

    db> SELECT 'some text'[1];
    {'o'}
    db> SELECT 'some text'[1:3];
    {'om'}
    db> SELECT 'some text'[-4:];
    {'text'}


.. eql:operator:: STRPLUS: A ++ B

    :optype A: str
    :optype B: str
    :resulttype: str

    String concatenation.

    .. code-block:: edgeql-repl

        db> SELECT 'some' ++ ' text';
        {'some text'}


.. eql:operator:: LIKE: A LIKE B

    :optype A: str or bytes
    :optype B: str or bytes
    :resulttype: bool

    Case-sensitive simple string matching.

    .. code-block:: edgeql-repl

        db> SELECT 'abc' LIKE 'abc';
        {true}
        db> SELECT 'abc' LIKE 'a%';
        {true}
        db> SELECT 'abc' LIKE '_b_';
        {true}
        db> SELECT 'abc' LIKE 'c';
        {false}


.. eql:operator:: ILIKE: A ILIKE B

    :optype A: str or bytes
    :optype B: str or bytes
    :resulttype: bool

    Case-insensitive simple string matching.

    .. code-block:: edgeql-repl

        db> SELECT 'Abc' ILIKE 'a%';
        {true}
