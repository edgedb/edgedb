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

        db> select false or true;
        {true}


----------


.. eql:operator:: AND: A AND B

    :optype A: bool
    :optype B: bool
    :resulttype: bool

    Logical conjunction.

    .. code-block:: edgeql-repl

        db> SELECT false AND true;
        {false}


----------


.. eql:operator:: NOT: NOT A

    :optype A: bool
    :resulttype: bool

    Logical negation.

    .. code-block:: edgeql-repl

        db> SELECT NOT false;
        {true}


----------


The ``AND`` and ``OR`` operators are commutative.

The truth tables are as follows:

+-------+-------+-----------+----------+
|   a   |   b   |  a AND b  |  a OR b  |
+=======+=======+===========+==========+
| true  | true  |   true    |   true   |
+-------+-------+-----------+----------+
| true  | false |   false   |   true   |
+-------+-------+-----------+----------+
| false | true  |   false   |   true   |
+-------+-------+-----------+----------+
| false | false |   false   |   false  |
+-------+-------+-----------+----------+

+-------+---------+
|   a   |  NOT a  |
+=======+=========+
| true  |  false  |
+-------+---------+
| false |  true   |
+-------+---------+
