.. _ref_datamodel_scalars_json:

JSON
====

.. eql:type:: std::json

    Arbitrary JSON data.

    Any other type (except for :eql:type`bytes`) can be :ref:`cast
    <ref_eql_expr_typecast>` to and from :eql:type:`json`:

    .. code-block:: edgeql-repl

        db> SELECT <json>42;
        {'42'}
        db> SELECT <bool>to_json('true');
        {true}

    Note that when a :eql:type:`str` is cast into a :eql:type:`json`,
    the result is JSON string value. Same applies for casting back
    from :eql:type:`json` - only a JSON string value can be cast into
    a :eql:type:`str`:

    .. code-block:: edgeql-repl

        db> SELECT <json>'Hello, world';
        {'"Hello, world"'}

    There are :ref:`converter <ref_eql_functions_converters>`
    functions that can be used to dump or parse a :eql:type:`json`
    value to or from a :eql:type:`str`:

    .. code-block:: edgeql-repl

        db> SELECT to_json('[1, "a"]');
        {'[1, "a"]'}
        db> SELECT to_str(<json>[1, 2]);
        {'[1, 2]'}

