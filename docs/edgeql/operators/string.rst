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


----------


.. eql:operator:: STRPLUS: A ++ B

    :optype A: str
    :optype B: str
    :resulttype: str

    String concatenation.

    .. code-block:: edgeql-repl

        db> SELECT 'some' ++ ' text';
        {'some text'}


----------


.. eql:operator:: LIKE: V LIKE P or V NOT LIKE P

    :optype V: str or bytes
    :optype P: str or bytes
    :resulttype: bool

    Case-sensitive simple string matching.

    Returns ``true`` if the *value* ``V`` matches the *pattern* ``P``
    and ``false`` otherwise.  The operator :eql:op:`NOT LIKE<LIKE>` is
    the negation of :eql:op:`LIKE`.

    The pattern matching rules are as follows:

    .. list-table::
        :widths: auto
        :header-rows: 1

        * - pattern
          - interpretation
        * - ``%``
          - matches zero or more characters
        * - ``_``
          - matches exactly one character
        * - ``\%``
          - matches a literal "%"
        * - ``\_``
          - matches a literal "_"
        * - any other character
          - matches itself

    In particular, this means that if there are no special symbols in
    the *pattern*, the operators :eql:op:`LIKE` and :eql:op:`NOT
    LIKE<LIKE>` work identical to :eql:op:`EQ` and :eql:op:`NEQ`,
    respectively.

    .. code-block:: edgeql-repl

        db> SELECT 'abc' LIKE 'abc';
        {true}
        db> SELECT 'abc' LIKE 'a%';
        {true}
        db> SELECT 'abc' LIKE '_b_';
        {true}
        db> SELECT 'abc' LIKE 'c';
        {false}
        db> SELECT 'a%%c' NOT LIKE 'a\%c';
        {true}


----------


.. eql:operator:: ILIKE: V ILIKE P or V NOT ILIKE P

    :optype V: str or bytes
    :optype P: str or bytes
    :resulttype: bool

    Case-insensitive simple string matching.

    The operators :eql:op:`ILIKE` and :eql:op:`NOT ILIKE<ILIKE>` work
    the same way as :eql:op:`LIKE` and :eql:op:`NOT LIKE<LIKE>`,
    except that the *pattern* is matched in a case-insensitive manner.

    .. code-block:: edgeql-repl

        db> SELECT 'Abc' ILIKE 'a%';
        {true}
