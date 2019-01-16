.. _ref_eql_functions_json:


====
JSON
====

.. eql:function:: std::json_to_str(json: json) -> str

    :index: stringify dumps

    Return string representation of the input JSON value.

    This is the reverse of :eql:func:`to_json`.

    .. code-block:: edgeql-repl

        db> SELECT json_to_str(<json>2);
        {'2'}

        db> SELECT json_to_str(<json>'hello world');
        {'"hello world"'}


.. eql:function:: std::json_array_unpack(json: json) -> SET OF json

    :index: array unpack

    Return elements of JSON array as a set of :eql:type:`json`.

    Calling this function on anything other than a JSON array will
    cause a runtime error.

    This function should be used if the ordering of elements is not
    important or when set ordering is preserved (such as an immediate
    input to an aggregate function).

    .. code-block:: edgeql-repl

        db> SELECT json_array_unpack(to_json('[1, "a"]'));
        {1, 'a'}

.. eql:function:: std::json_get(json: json, \
                                VARIADIC path: str) -> OPTIONAL json

    :index: safe navigation

    Return the JSON value at the end of the specified path or ``{}``.

    This function provides "safe" navigation of a JSON value. If the
    input path is a valid path for the input JSON object/array, the
    JSON value at the end of that path is returned. If the path cannot
    be followed for any reason, the empty set is returned.

    .. code-block:: edgeql-repl

        db> SELECT json_get(to_json('{
        ...     "q": 1,
        ...     "w": [2, "foo"],
        ...     "e": true
        ... }', 'w', '1'));
        {'foo'}

    This is useful when certain structure of JSON data is assumed, but
    cannot be reliably guaranteed:

    .. code-block:: edgeql-repl

        db> SELECT json_get(to_json('{
        ...     "q": 1,
        ...     "w": [2, "foo"],
        ...     "e": true
        ... }', 'w', '2'));
        {}

    Also, a default value can be supplied by using the
    :eql:op:`coalescing <COALESCE>` operator:

    .. code-block:: edgeql-repl

        db> SELECT json_get(to_json('{
        ...     "q": 1,
        ...     "w": [2, "foo"],
        ...     "e": true
        ... }', 'w', '2')) ?? <json>'"mydefault"';
        {'mydefault'}

.. eql:function:: std::json_object_unpack(json: json) -> \
                  SET OF tuple<str, json>

    Return set of key/value tuples that make up the JSON object.

    Calling this function on anything other than a JSON object will
    cause a runtime error.

    .. code-block:: edgeql-repl

        db> SELECT json_object_unpack(to_json('{
        ...     "q": 1,
        ...     "w": [2, "foo"],
        ...     "e": true
        ... }'));
        {['e', True], ['q', 1], ['w', [2, 'foo']]}

.. eql:function:: std::json_typeof(json: json) -> str

    :index: type

    Return the type of the outermost JSON value as a string.

    Possible return values are: ``'object'``, ``'array'``,
    ``'string'``, ``'number'``, ``'boolean'``, ``'null'``.

    .. code-block:: edgeql-repl

        db> SELECT json_typeof(<json>2);
        {'number'}

        db> SELECT json_typeof(to_json('null'));
        {'null'}

        db> SELECT json_typeof(to_json('{"a": 2}'));
        {'object'}
