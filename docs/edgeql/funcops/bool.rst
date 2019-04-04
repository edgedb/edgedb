.. _ref_eql_operators_logical:


=======
Logical
=======

:edb-alt-title: Logical Operators


.. list-table::
    :class: funcoptable

    * - :eql:op:`a OR b <OR>`
      - :eql:op-desc:`OR`

    * - :eql:op:`a AND b <AND>`
      - :eql:op-desc:`AND`

    * - :eql:op:`NOT a <NOT>`
      - :eql:op-desc:`NOT`


----------


.. eql:operator:: OR: bool OR bool -> bool

    Logical disjunction.

    .. code-block:: edgeql-repl

        db> select false or true;
        {true}


----------


.. eql:operator:: AND: bool AND bool -> bool

    Logical conjunction.

    .. code-block:: edgeql-repl

        db> SELECT false AND true;
        {false}


----------


.. eql:operator:: NOT: NOT bool -> bool

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
