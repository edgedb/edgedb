.. _ref_eql_operators_logical:

=======
Logical
=======

EdgeDB supports the following boolean logical operators:
``AND``, ``OR``, and ``NOT``.

.. eql:operator:: OR: A OR B

    :optype A: bool
    :optype B: bool
    :resulttype: bool

    Logical disjunction.

    .. code-block:: edgeql-repl

        db> select False or True;
        {True}


.. eql:operator:: AND: A AND B

    :optype A: bool
    :optype B: bool
    :resulttype: bool

    Logical conjunction.

    .. code-block:: edgeql-repl

        db> SELECT False AND True;
        {False}


.. eql:operator:: NOT: NOT A

    :optype A: bool
    :resulttype: bool

    Logical negation.

    .. code-block:: edgeql-repl

        db> SELECT NOT False;
        {True}


The ``AND`` and ``OR`` operators are commutative.

The truth tables are as follows:

+-------+-------+-----------+----------+
|   a   |   b   |  a AND b  |  a OR b  |
+=======+=======+===========+==========+
| TRUE  | TRUE  |   TRUE    |   TRUE   |
+-------+-------+-----------+----------+
| TRUE  | FALSE |   FALSE   |   TRUE   |
+-------+-------+-----------+----------+
| FALSE | TRUE  |   FALSE   |   TRUE   |
+-------+-------+-----------+----------+
| FALSE | FALSE |   FALSE   |   FALSE  |
+-------+-------+-----------+----------+

+-------+---------+
|   a   |  NOT a  |
+=======+=========+
| TRUE  |  FALSE  |
+-------+---------+
| FALSE |  TRUE   |
+-------+---------+
