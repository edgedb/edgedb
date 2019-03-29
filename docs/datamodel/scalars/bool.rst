.. _ref_datamodel_scalars_bool:

Boolean Type
============

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
    <ref_eql_operators_logical>` or :ref:`comparison
    <ref_eql_operators_comparison>` operations as well as :eql:op:`IN`
    and :eql:op:`NOT IN <IN>`:

    .. code-block:: edgeql-repl

        db> SELECT true AND 2 < 3;
        {true}
        db> SELECT '!' IN {'hello', 'world'};
        {false}

    It is also possible to :ref:`cast <ref_eql_expr_typecast>` between
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
