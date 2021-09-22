.. _ref_std_logical:


=======
Logical
=======

:edb-alt-title: Boolean Type and Logical Operators


.. list-table::
    :class: funcoptable

    * - :eql:type:`bool`
      - Boolean type

    * - :eql:op:`bool OR bool <OR>`
      - :eql:op-desc:`OR`

    * - :eql:op:`bool AND bool <AND>`
      - :eql:op-desc:`AND`

    * - :eql:op:`NOT bool <NOT>`
      - :eql:op-desc:`NOT`

    * - :eql:op:`bool = bool <EQ>`, :eql:op:`bool \< bool <LT>`, ...
      - Comparison operators.


----------


.. eql:type:: std::bool

    A boolean type with possible values of ``true`` and ``false``.

    EdgeQL has case-insensitive keywords and that includes the boolean
    literals:

    .. code-block:: edgeql-repl

        db> SELECT (True, true, TRUE);
        {(true, true, true)}
        db> SELECT (False, false, FALSE);
        {(false, false, false)}

    A boolean value may arise as a result of a :ref:`logical
    <ref_std_logical>` or :eql:op:`comparison <EQ>`
    operations as well as :eql:op:`IN`
    and :eql:op:`NOT IN <IN>`:

    .. code-block:: edgeql-repl

        db> SELECT true AND 2 < 3;
        {true}
        db> SELECT '!' IN {'hello', 'world'};
        {false}

    It is also possible to :eql:op:`cast <CAST>` between
    :eql:type:`bool`, :eql:type:`str`, and :eql:type:`json`:

    .. code-block:: edgeql-repl

        db> SELECT <json>true;
        {'true'}
        db> SELECT <bool>'True';
        {true}

    :ref:`Filter <ref_eql_statements_select_filter>` clauses must
    always evaluate to a boolean:

    .. code-block:: edgeql

        SELECT User
        FILTER .name ILIKE 'alice';


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
