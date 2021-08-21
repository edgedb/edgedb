.. _ref_std_logical:


=======
Logical
=======

:edb-alt-title: Boolean Type and Logical Operators


.. list-table::
    :class: funcoptable

    * - :eql:op:`bool OR bool <OR>`
      - :eql:op-desc:`OR`

    * - :eql:op:`bool AND bool <AND>`
      - :eql:op-desc:`AND`

    * - :eql:op:`NOT bool <NOT>`
      - :eql:op-desc:`NOT`

    * - :eql:op:`bool = bool <EQ>`, :eql:op:`bool \< bool <LT>`, ...
      - Comparison operators.


----------


.. eql:operator:: OR: bool OR bool -> bool

    Logical disjunction.

    .. code-block:: edgeql-repl

        db> SELECT false OR true;
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

+-------+-------+-----------+----------+----------+
|   a   |   b   |  a AND b  |  a OR b  |  NOT a   |
+=======+=======+===========+==========+==========+
| true  | true  |   true    |   true   |   false  |
+-------+-------+-----------+----------+----------+
| true  | false |   false   |   true   |   false  |
+-------+-------+-----------+----------+----------+
| false | true  |   false   |   true   |   true   |
+-------+-------+-----------+----------+----------+
| false | false |   false   |   false  |   true   |
+-------+-------+-----------+----------+----------+
