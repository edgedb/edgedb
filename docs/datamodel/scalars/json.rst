.. _ref_datamodel_scalars_json:

JSON
====

.. eql:type:: std::json

    Arbitrary JSON data.

    Any other type (except for :eql:type:`bytes`) can be :ref:`cast
    <ref_eql_expr_typecast>` to and from JSON:

    .. code-block:: edgeql-repl

        db> SELECT <json>42;
        {'42'}
        db> SELECT <bool>to_json('true');
        {true}

    Note that a :eql:type:`json` value can be cast into a :eql:type:`str`
    only when it is a JSON string.  Therefore the following will work
    as expected:

    .. code-block:: edgeql-repl

        db> SELECT <str>to_json('"something"');
        {'something'}

    while the below operation (casting a JSON array of string
    ``["a", "b", "c"]`` to a *str*) will result in an error:

    .. code-block:: edgeql-repl

        db> SELECT <str>to_json('["a", "b", "c"]');
        InternalServerError: expected json string, null; got json array

    Use the :ref:`converter <ref_eql_functions_converters>`
    functions to dump or parse a :eql:type:`json` value to or
    from a :eql:type:`str`:

    .. code-block:: edgeql-repl

        db> SELECT to_json('[1, "a"]');
        {'[1, "a"]'}
        db> SELECT to_str(<json>[1, 2]);
        {'[1, 2]'}

    See also the list of standard
    :ref:`JSON functions <ref_eql_functions_json>` and
    :ref:`operators <ref_eql_operators_json>`.
