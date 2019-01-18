.. _ref_eql_functions_converters:


Type Converters
===============

These functions convert between different scalar types. When a
simple cast is not sufficient to specify how data must be converted,
the functions below allow more options for such conversions.

.. eql:function:: std::to_json(string: str) -> json

    :index: json parse loads

    Return JSON value represented by the input *string*.

    This is the reverse of :eql:func:`to_str`.

    .. code-block:: edgeql-repl

        db> SELECT to_json('[1, "hello", null]')[1];
        {'hello'}

        db> SELECT to_json('{"hello": "world"}')['hello'];
        {'world'}

.. eql:function:: std::to_str(json: json, fmt: OPTIONAL str={}) -> str

    :index: stringify dumps

    Return string representation of the input value.

    When converting :eql:type:`json`, this function can take
    ``'pretty'`` as the optional *fmt* argument to produce
    pretty-formatted JSON string.

    See also :eql:func:`to_json`.

    .. code-block:: edgeql-repl

        db> SELECT to_str(<json>2);
        {'2'}

        db> SELECT to_str(<json>['hello', 'world']);
        {'["hello", "world"]'}

        db> SELECT to_str(<json>(a := 2, b := 'hello'), 'pretty');
        {'{
            "a": 2,
            "b": "hello"
        }'}
