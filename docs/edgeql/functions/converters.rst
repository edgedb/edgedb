.. _ref_eql_functions_converters:


Type Converters
===============

These functions convert between different scalar types. When a
simple cast is not sufficient to specify how data must be converted,
the functions below allow more options for such conversions.

.. eql:function:: std::to_json(string: str) -> json

    :index: json parse loads

    Return JSON value represented by the input *string*.

    This is the reverse of :eql:func:`json_to_str`.

    .. code-block:: edgeql-repl

        db> SELECT to_json('[1, "hello", null]')[1];
        {'hello'}

        db> SELECT to_json('{"hello": "world"}')['hello'];
        {'world'}
