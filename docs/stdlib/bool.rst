.. _ref_std_logical:


========
Booleans
========

:edb-alt-title: Boolean Functions and Operators


.. list-table::
    :class: funcoptable

    * - :eql:type:`bool`
      - Boolean type

    * - :eql:op:`bool or bool <or>`
      - :eql:op-desc:`or`

    * - :eql:op:`bool and bool <and>`
      - :eql:op-desc:`and`

    * - :eql:op:`not bool <not>`
      - :eql:op-desc:`not`

    * - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
        :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
        :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
      - Comparison operators

    * - :eql:func:`all`
      - :eql:func-desc:`all`

    * - :eql:func:`any`
      - :eql:func-desc:`any`


----------


.. eql:type:: std::bool

    Represents a logical boolean type of either ``true`` or ``false``.

    EdgeQL has case-insensitive keywords, including boolean literals:

    .. code-block:: edgeql-repl

        db> select (True, true, TRUE);
        {(true, true, true)}
        db> select (False, false, FALSE);
        {(false, false, false)}

    Boolean values may arise as a result of :ref:`logical <ref_std_logical>`
    or :eql:op`comparison <eq>` checks, as well as :eql:op`in` and
    :eql:op`not in <in>` operations:

    .. code-block:: edgeql-repl

        db> select true and 2 < 3;
        {true}
        db> select '!' IN {'hello', 'world'};
        {false}

    It is also possible to :eql:op:`cast <cast>` between a :eql:type:`bool`,
    :eql:type:`str` or :eql:type:`json` type:

    .. code-block:: edgeql-repl

        db> select <json>true;
        {'true'}
        db> select <bool>'True';
        {true}

    :ref:`Filter clauses <ref_eql_statements_select_filter>` must always
    evaluate to a boolean.

    .. code-block:: edgeql

        select User
        filter .name ilike 'alice';


----------


.. eql:operator:: or: bool or bool -> bool

    Logically differentiates the truthfulness between two booleans:

    .. code-block:: edgeql-repl

        db> select false or true;
        {true}


----------


.. eql:operator:: and: bool and bool -> bool

    Performs a logical coexistence check between two booleans:

    .. code-block:: edgeql-repl

        db> select false and true;
        {false}


----------


.. eql:operator:: not: not bool -> bool

    Logically negates a given boolean value:

    .. code-block:: edgeql-repl

        db> select not false;
        {true}


----------


The ``and`` and ``or`` operators are commutative.

The truth tables are as follows:

+-------+-------+---------------+--------------+--------------+
|   a   |   b   |  a ``and`` b  |  a ``or`` b  |  ``not`` a   |
+=======+=======+===============+==============+==============+
| true  | true  |   true        |   true       |   false      |
+-------+-------+---------------+--------------+--------------+
| true  | false |   false       |   true       |   false      |
+-------+-------+---------------+--------------+--------------+
| false | true  |   false       |   true       |   true       |
+-------+-------+---------------+--------------+--------------+
| false | false |   false       |   false      |   true       |
+-------+-------+---------------+--------------+--------------+


----------


It is important to understand the difference between using
``and``/``or`` and :eql:func:`all`/:eql:func:`any`. The difference is
in how they handle ``{}``. Both ``and`` and ``or`` operators apply to
the cross-product of their operands. Thus, if any of the operands are
``{}``, the result will be that.

The :eql:func:`all` and :eql:func:`any` functions are generalized to apply to
sets of values, including ``{}``. They have the following truth table:

+-------+-------+-----------------+-----------------+
|   a   |   b   | ``all({a, b})`` | ``any({a, b})`` |
+=======+=======+=================+=================+
| true  | true  |   true          |   true          |
+-------+-------+-----------------+-----------------+
| true  | false |   false         |   true          |
+-------+-------+-----------------+-----------------+
| {}    | true  |   true          |   true          |
+-------+-------+-----------------+-----------------+
| {}    | false |   false         |   false         |
+-------+-------+-----------------+-----------------+
| false | true  |   false         |   true          |
+-------+-------+-----------------+-----------------+
| false | false |   false         |   false         |
+-------+-------+-----------------+-----------------+
| true  | {}    |   true          |   true          |
+-------+-------+-----------------+-----------------+
| false | {}    |   false         |   false         |
+-------+-------+-----------------+-----------------+
| {}    | {}    |   true          |   false         |
+-------+-------+-----------------+-----------------+

Since :eql:func:`all` and :eql:func:`any` apply to sets as a whole,
missing values (represented by ``{}``) are just that - missing. They
don't affect the overall result.

To understand the last line in the above truth table it's useful to
remember that ``all({a, b}) = all(a) and all(b)`` and ``any({a, b}) =
any(a) or any(b)``.

For more customized handling of ``{}``, the :eql:op:`?? <coalesce>` operator
should be used.
