.. _ref_std_json:

====
JSON
====

:edb-alt-title: JSON Functions and Operators

.. list-table::
    :class: funcoptable

    * - :eql:type:`json`
      - JSON scalar type

    * - :eql:op:`json[i] <jsonidx>`
      - :eql:op-desc:`jsonidx`

    * - :eql:op:`json[from:to] <jsonslice>`
      - :eql:op-desc:`jsonslice`

    * - :eql:op:`json ++ json <jsonplus>`
      - :eql:op-desc:`jsonplus`

    * - :eql:op:`json[name] <jsonobjdest>`
      - :eql:op-desc:`jsonobjdest`

    * - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
        :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
        :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
      - Comparison operators

    * - :eql:func:`to_json`
      - :eql:func-desc:`to_json`

    * - :eql:func:`to_str`
      - Render JSON value to a string.

    * - :eql:func:`json_get`
      - :eql:func-desc:`json_get`

    * - :eql:func:`json_set`
      - :eql:func-desc:`json_set`

    * - :eql:func:`json_array_unpack`
      - :eql:func-desc:`json_array_unpack`

    * - :eql:func:`json_object_pack`
      - :eql:func-desc:`json_object_pack`

    * - :eql:func:`json_object_unpack`
      - :eql:func-desc:`json_object_unpack`

    * - :eql:func:`json_typeof`
      - :eql:func-desc:`json_typeof`

.. _ref_std_json_construction:

Constructing JSON Values
------------------------

JSON in EdgeDB is a :ref:`scalar type <ref_datamodel_scalar_types>`. This type
doesn't have its own literal, and instead can be obtained by either casting a
value to the :eql:type:`json` type, or by using the :eql:func:`to_json`
function:

.. code-block:: edgeql-repl

    db> select to_json('{"hello": "world"}');
    {Json("{\"hello\": \"world\"}")}
    db> select <json>'hello world';
    {Json("\"hello world\"")}

Any value in EdgeDB can be cast to a :eql:type:`json` type as well:

.. code-block:: edgeql-repl

    db> select <json>2019;
    {Json("2019")}
    db> select <json>cal::to_local_date(datetime_current(), 'UTC');
    {Json("\"2022-11-21\"")}

.. versionadded:: 3.0

    The :eql:func:`json_object_pack` function provides one more way to
    construct JSON. It constructs a JSON object from an array of key/value
    tuples:

    .. code-block:: edgeql-repl

        db> select json_object_pack({("hello", <json>"world")});
        {Json("{\"hello\": \"world\"}")}

Additionally, any :eql:type:`Object` in EdgeDB can be cast as a
:eql:type:`json` type. This produces the same JSON value as the
JSON-serialized result of that said object. Furthermore, this result will
be the same as the output of a :eql:stmt:`select expression <select>` in
*JSON mode*, including the shape of that type:

.. code-block:: edgeql-repl

    db> select <json>(
    ...     select schema::Object {
    ...         name,
    ...         timestamp := cal::to_local_date(
    ...             datetime_current(), 'UTC')
    ...     }
    ...     filter .name = 'std::bool');
    {Json("{\"name\": \"std::bool\", \"timestamp\": \"2022-11-21\"}")}

JSON values can also be cast back into scalars. Casting JSON is symmetrical
meaning that, if a scalar value can be cast into JSON, a compatible JSON value
can be cast into a scalar of that type. Some scalar types will have specific
conditions for casting:

- JSON strings can be cast to a :eql:type:`str` type. Casting :eql:type:`uuid`
  and :ref:`date/time <ref_std_datetime>` types to JSON results in a JSON
  string representing its original value. This means it is also possible to
  cast a JSON string back to those types. The value of the UUID or datetime
  string must be properly formatted to successfully cast from JSON, otherwise
  EdgeDB will raise an exception.
- JSON numbers can be cast to any :ref:`numeric type <ref_std_numeric>`.
- JSON booleans can be cast to a :eql:type:`bool` type.
- JSON ``null`` is unique because it can be cast to an empty set (``{}``) of
  any type.
- JSON arrays can be cast to any valid array type, as long as the JSON array
  is homogeneous, does not contain ``null`` as an element of the array, and
  does not contain another array.

A named :eql:type:`tuple` is converted into a JSON object when cast as a
:eql:type:`json` while a standard :eql:type:`tuple` is converted into a
JSON array.

----------


.. eql:type:: std::json

    Arbitrary JSON data.

    Any other type can be :eql:op:`cast <cast>` to and from JSON:

    .. code-block:: edgeql-repl

        db> select <json>42;
        {Json("42")}
        db> select <bool>to_json('true');
        {true}

    A :eql:type:`json` value can also be cast as a :eql:type:`str` type, but
    only when recognized as a JSON string:

    .. code-block:: edgeql-repl

        db> select <str>to_json('"something"');
        {'something'}

    Casting a JSON array of strings (``["a", "b", "c"]``) to a :eql:type:`str`
    will result in an error:

    .. code-block:: edgeql-repl

        db> select <str>to_json('["a", "b", "c"]');
        InvalidValueError: expected json string or null; got JSON array

    Instead, use the :eql:func:`to_str` function to dump a JSON value to a
    :eql:type:`str` value. Use the :eql:func:`to_json` function to parse a
    JSON string to a :eql:type:`json` value:

    .. code-block:: edgeql-repl

        db> select to_json('[1, "a"]');
        {Json("[1, \"a\"]")}
        db> select to_str(<json>[1, 2]);
        {'[1, 2]'}

    .. note::

        This type is backed by the Postgres ``jsonb`` type which has a size
        limit of 256MiB minus one byte. The EdgeDB ``json`` type is also
        subject to this limitation.


----------


.. eql:operator:: jsonidx: json [ int64 ] -> json

    Accesses the element of the JSON string or array at a given index.

    The contents of JSON *arrays* and *strings* can also be
    accessed via ``[]``:

    .. code-block:: edgeql-repl

        db> select <json>'hello'[1];
        {Json("\"e\"")}
        db> select <json>'hello'[-1];
        {Json("\"o\"")}
        db> select to_json('[1, "a", null]')[1];
        {Json("\"a\"")}
        db> select to_json('[1, "a", null]')[-1];
        {Json("null")}

    This will raise an exception if the specified index is not valid for the
    base JSON value. To access an index that is potentially out of bounds, use
    :eql:func:`json_get`.


----------


.. eql:operator:: jsonslice: json [ int64 : int64 ] -> json

    Produces a JSON value comprising a portion of the existing JSON value.

    JSON *arrays* and *strings* can be sliced in the same way as
    regular arrays, producing a new JSON array or string:

    .. code-block:: edgeql-repl

        db> select <json>'hello'[0:2];
        {Json("\"he\"")}
        db> select <json>'hello'[2:];
        {Json("\"llo\"")}
        db> select to_json('[1, 2, 3]')[0:2];
        {Json("[1, 2]")}
        db> select to_json('[1, 2, 3]')[2:];
        {Json("[3]")}
        db> select to_json('[1, 2, 3]')[:1];
        {Json("[1]")}
        db> select to_json('[1, 2, 3]')[:-2];
        {Json("[1]")}

----------


.. eql:operator:: jsonplus: json ++ json -> json

    Concatenates two JSON arrays, objects, or strings into one.

    JSON arrays, objects and strings can be concatenated with JSON values of
    the same type into a new JSON value.

    If you concatenate two JSON objects, you get a new object whose keys will
    be a union of the keys of the input objects. If a key is present in both
    objects, the value from the second object is taken.

    .. code-block:: edgeql-repl

        db> select to_json('[1, 2]') ++ to_json('[3]');
        {Json("[1, 2, 3]")}
        db> select to_json('{"a": 1}') ++ to_json('{"b": 2}');
        {Json("{\"a\": 1, \"b\": 2}")}
        db> select to_json('{"a": 1, "b": 2}') ++ to_json('{"b": 3}');
        {Json("{\"a\": 1, \"b\": 3}")}
        db> select to_json('"123"') ++ to_json('"456"');
        {Json("\"123456\"")}

----------


.. eql:operator:: jsonobjdest: json [ str ] -> json

    Accesses an element of a JSON object given its key.

    The fields of JSON *objects* can also be accessed via ``[]``:

    .. code-block:: edgeql-repl

        db> select to_json('{"a": 2, "b": 5}')['b'];
        {Json("5")}
        db> select j := <json>(schema::Type {
        ...     name,
        ...     timestamp := cal::to_local_date(datetime_current(), 'UTC')
        ... })
        ... filter j['name'] = <json>'std::bool';
        {Json("{\"name\": \"std::bool\", \"timestamp\": \"2022-11-21\"}")}


    This will raise an exception if the specified field does not exist for the
    base JSON value. To access an index that is potentially out of bounds, use
    :eql:func:`json_get`.


----------


.. eql:function:: std::to_json(string: str) -> json

    :index: json parse loads

    Returns a JSON value parsed from the given string.

    .. code-block:: edgeql-repl

        db> select to_json('[1, "hello", null]');
        {Json("[1, \"hello\", null]")}
        db> select to_json('{"hello": "world"}');
        {Json("{\"hello\": \"world\"}")}


----------


.. eql:function:: std::json_array_unpack(json: json) -> set of json

    :index: array unpack

    Returns the elements of a JSON array as a set of :eql:type:`json`.

    Calling this function on anything other than a JSON array will
    result in a runtime error.

    This function should be used only if the ordering of elements is not
    important, or when the ordering of the set is preserved (such as an
    immediate input to an aggregate function).

    .. code-block:: edgeql-repl

        db> select json_array_unpack(to_json('[1, "a"]'));
        {Json("1"), Json("\"a\"")}


----------


.. eql:function:: std::json_get(json: json, \
                                variadic path: str) -> optional json

    :index: safe navigation

    Returns a value from a JSON object or array given its path.

    This function provides "safe" navigation of a JSON value. If the
    input path is a valid path for the input JSON object/array, the
    JSON value at the end of that path is returned:

    .. code-block:: edgeql-repl

        db> select json_get(to_json('{
        ...     "q": 1,
        ...     "w": [2, "foo"],
        ...     "e": true
        ... }'), 'w', '1');
        {Json("\"foo\"")}

    This is useful when certain structure of JSON data is assumed, but cannot
    be reliably guaranteed. If the path cannot be followed for any reason, the
    empty set is returned:

    .. code-block:: edgeql-repl

        db> select json_get(to_json('{
        ...     "q": 1,
        ...     "w": [2, "foo"],
        ...     "e": true
        ... }'), 'w', '2');
        {}

    If you want to supply your own default for the case where the path cannot
    be followed, you can do so using the :eql:op:`coalesce` operator:

    .. code-block:: edgeql-repl

        db> select json_get(to_json('{
        ...     "q": 1,
        ...     "w": [2, "foo"],
        ...     "e": true
        ... }'), 'w', '2') ?? <json>'mydefault';
        {Json("\"mydefault\"")}


----------


.. eql:function:: std::json_set( \
                    target: json, \
                    variadic path: str, \
                    named only value: optional json, \
                    named only create_if_missing: bool = true, \
                    named only empty_treatment: JsonEmpty = \
                      JsonEmpty.ReturnEmpty) \
                  -> optional json

    .. versionadded:: 2.0

    Returns an updated JSON target with a new value.

    .. code-block:: edgeql-repl

        db> select json_set(
        ...   to_json('{"a": 10, "b": 20}'),
        ...   'a',
        ...   value := <json>true,
        ... );
        {Json("{\"a\": true, \"b\": 20}")}
        db> select json_set(
        ...   to_json('{"a": {"b": {}}}'),
        ...   'a', 'b', 'c',
        ...   value := <json>42,
        ... );
        {Json("{\"a\": {\"b\": {\"c\": 42}}}")}

    If *create_if_missing* is set to ``false``, a new path for the value
    won't be created:

    .. code-block:: edgeql-repl

        db> select json_set(
        ...   to_json('{"a": 10, "b": 20}'),
        ...   'с',
        ...   value := <json>42,
        ... );
        {Json("{\"a\": 10, \"b\": 20, \"с\": 42}")}
        db> select json_set(
        ...   to_json('{"a": 10, "b": 20}'),
        ...   'с',
        ...   value := <json>42,
        ...   create_if_missing := false,
        ... );
        {Json("{\"a\": 10, \"b\": 20}")}

    The *empty_treatment* parameter defines the behavior of the function if an
    empty set is passed as *new_value*. This parameter can take these values:

    - ``ReturnEmpty``: return empty set, default
    - ``ReturnTarget``: return ``target`` unmodified
    - ``Error``: raise an ``InvalidValueError``
    - ``UseNull``: use a ``null`` JSON value
    - ``DeleteKey``: delete the object key

    .. code-block:: edgeql-repl

        db> select json_set(
        ...   to_json('{"a": 10, "b": 20}'),
        ...   'a',
        ...   value := <json>{}
        ... );
        {}
        db> select json_set(
        ...   to_json('{"a": 10, "b": 20}'),
        ...   'a',
        ...   value := <json>{},
        ...   empty_treatment := JsonEmpty.ReturnTarget,
        ... );
        {Json("{\"a\": 10, \"b\": 20}")}
        db> select json_set(
        ...   to_json('{"a": 10, "b": 20}'),
        ...   'a',
        ...   value := <json>{},
        ...   empty_treatment := JsonEmpty.Error,
        ... );
        InvalidValueError: invalid empty JSON value
        db> select json_set(
        ...   to_json('{"a": 10, "b": 20}'),
        ...   'a',
        ...   value := <json>{},
        ...   empty_treatment := JsonEmpty.UseNull,
        ... );
        {Json("{\"a\": null, \"b\": 20}")}
        db> select json_set(
        ...   to_json('{"a": 10, "b": 20}'),
        ...   'a',
        ...   value := <json>{},
        ...   empty_treatment := JsonEmpty.DeleteKey,
        ... );
        {Json("{\"b\": 20}")}

----------


.. eql:function:: std::json_object_pack(pairs: SET OF tuple<str, json>) -> \
                  json

    .. versionadded:: 3.0

    Returns the given set of key/value tuples as a JSON object.

    .. code-block:: edgeql-repl

        db> select json_object_pack({
        ...     ("foo", to_json("1")),
        ...     ("bar", to_json("null")),
        ...     ("baz", to_json("[]"))
        ... });
        {Json("{\"bar\": null, \"baz\": [], \"foo\": 1}")}

    If the key/value tuples being packed have common keys, the last value for
    each key will make the final object.

    .. code-block:: edgeql-repl

        db> select json_object_pack({
        ...     ("hello", <json>"world"),
        ...     ("hello", <json>true)
        ... });
        {Json("{\"hello\": true}")}


----------


.. eql:function:: std::json_object_unpack(json: json) -> \
                  set of tuple<str, json>

    Returns the data in a JSON object as a set of key/value tuples.

    Calling this function on anything other than a JSON object will
    result in a runtime error.

    .. code-block:: edgeql-repl

        db> select json_object_unpack(to_json('{
        ...     "q": 1,
        ...     "w": [2, "foo"],
        ...     "e": true
        ... }'));
        {('e', Json("true")), ('q', Json("1")), ('w', Json("[2, \"foo\"]"))}


----------


.. eql:function:: std::json_typeof(json: json) -> str

    :index: type

    Returns the type of the outermost JSON value as a string.

    Possible return values are: ``'object'``, ``'array'``,
    ``'string'``, ``'number'``, ``'boolean'``, or ``'null'``:

    .. code-block:: edgeql-repl

        db> select json_typeof(<json>2);
        {'number'}
        db> select json_typeof(to_json('null'));
        {'null'}
        db> select json_typeof(to_json('{"a": 2}'));
        {'object'}
