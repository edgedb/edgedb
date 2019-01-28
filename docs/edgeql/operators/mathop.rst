.. _ref_eql_operators_math:

============
Mathematical
============

This section describes mathematical operators
provided by EdgeDB.

.. eql:operator:: PLUS: A + B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: plus add

    Arithmetic addition.

    .. code-block:: edgeql-repl

        db> SELECT 2 + 2;
        {4}


.. eql:operator:: MINUS: A - B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: minus subtract

    Arithmetic subtraction.

    .. code-block:: edgeql-repl

        db> SELECT 3 - 2;
        {1}


.. eql:operator:: UMINUS: -A

    :optype A: anyreal
    :resulttype: anyreal
    :index: unary minus subtract

    Arithmetic negation.

    .. code-block:: edgeql-repl

        db> SELECT -5;
        {-5}


.. eql:operator:: MULT: A * B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: multiply multiplication

    Arithmetic multiplication.

    .. code-block:: edgeql-repl

        db> SELECT 2 * 10;
        {20}


.. eql:operator:: DIV: A / B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: divide division

    Arithmetic division.

    .. code-block:: edgeql-repl

        db> SELECT 10 / 4;
        {2.5}


.. eql:operator:: FLOORDIV: A // B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: floor divide division

    Integer division.

    The result is rounded down to the nearest integer. It is
    equivalent to using regular division and the applying
    :eql:func:`math::floor` to the result.

    .. code-block:: edgeql-repl

        db> SELECT 10 // 4;
        {2}
        db> SELECT math::floor(10 / 4);
        {2}
        db> SELECT -10 // 4;
        {-3}

    Regular division, integer division and :eql:op:`%<MOD>` are
    related in the following way: ``A / B = (A // B + A % B)``


.. eql:operator:: MOD: A % B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: modulo mod division

    Remainder from division (modulo).

    .. code-block:: edgeql-repl

        db> SELECT 7 % 4;
        {3}


.. eql:operator:: POW: A ^ B

    :optype A: anyreal
    :optype B: anyreal
    :resulttype: anyreal
    :index: power pow

    Power operation.

    .. code-block:: edgeql-repl

        db> SELECT 2 ^ 4;
        {16}
