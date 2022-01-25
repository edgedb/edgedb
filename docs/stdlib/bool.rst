.. _ref_std_logical:


=======
Boolean
=======

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

    A boolean type with possible values of ``true`` and ``false``.

    EdgeQL has case-insensitive keywords and that includes the boolean
    literals:

    .. code-block:: edgeql-repl

        db> select (True, true, TRUE);
        {(true, true, true)}
        db> select (False, false, FALSE);
        {(false, false, false)}

    A boolean value may arise as a result of a :ref:`logical
    <ref_std_logical>` or :eql:op:`comparison <eq>`
    operations as well as :eql:op:`in`
    and :eql:op:`not in <in>`:

    .. code-block:: edgeql-repl

        db> select true and 2 < 3;
        {true}
        db> select '!' IN {'hello', 'world'};
        {false}

    It is also possible to :eql:op:`cast <cast>` between
    :eql:type:`bool`, :eql:type:`str`, and :eql:type:`json`:

    .. code-block:: edgeql-repl

        db> select <json>true;
        {'true'}
        db> select <bool>'True';
        {true}

    :ref:`Filter <ref_eql_statements_select_filter>` clauses must
    always evaluate to a boolean:

    .. code-block:: edgeql

        select User
        filter .name ilike 'alice';


----------


.. eql:operator:: or: bool or bool -> bool

    Logical disjunction.

    .. code-block:: edgeql-repl

        db> select false or true;
        {true}


----------


.. eql:operator:: and: bool and bool -> bool

    Logical conjunction.

    .. code-block:: edgeql-repl

        db> select false and true;
        {false}


----------


.. eql:operator:: not: not bool -> bool

    Logical negation.

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
