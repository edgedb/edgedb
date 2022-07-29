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

    * - :eql:func:`json_object_unpack`
      - :eql:func-desc:`json_object_unpack`

    * - :eql:func:`json_typeof`
      - :eql:func-desc:`json_typeof`

.. _ref_std_json_construction:

Constructing JSON Values
------------------------

JSON in EdgeDB is one of the :ref:`scalar types <ref_datamodel_scalar_types>`.
This scalar doesn't have its own literal and instead can be obtained
by casting a value into :eql:type:`json` or by using :eql:func:`to_json`:

.. code-block:: edgeql-repl

    db> select to_json('{"hello": "world"}');
    {'{"hello": "world"}'}
    db> select <json>'hello world';
    {'"hello world"'}

Anything in EdgeDB can be cast into :eql:type:`json`:

.. code-block:: edgeql-repl

    db> select <json>2019;
    {'2019'}
    db> select <json>cal::to_local_date(datetime_current(), 'UTC');
    {'"2019-04-02"'}

Any :eql:type:`Object` can be cast into :eql:type:`json`. This
produces the same JSON value as the JSON serialization of that object.
That is, the result is the same as the output of :eql:stmt:`select
expression <select>` in *JSON mode*, including the type shape.

.. code-block:: edgeql-repl

    db> select <json>(
    ...     select schema::Object {
    ...         name,
    ...         timestamp := cal::to_local_date(
    ...             datetime_current(), 'UTC')
    ...     }
    ...     filter .name = 'std::bool');
    {'{"name": "std::bool", "timestamp": "2019-04-02"}'}

JSON values can also be cast back into scalars. This casting is
symmetrical meaning that if a scalar can be cast into JSON, only that
particular JSON type can be cast back into that scalar:

- JSON *string* can be cast into :eql:type:`str`. Casting
  :eql:type:`uuid` and :ref:`date and time types
  <ref_std_datetime>` to JSON results in a JSON
  *string* representing the original value. This means that it is
  also possible to cast a JSON *string* back into these types. The
  string value has to be properly formatted (much like in case of
  a :eql:type:`str` value being cast) or else the cast will raise an
  exception.
- JSON *number* can be cast into any of
  the :ref:`numeric types <ref_std_numeric>`
- JSON *boolean* can be cast into :eql:type:`bool`
- JSON *null* is special since it can be cast into an ``{}`` of any type
- JSON *array* can be cast into any valid EdgeDB array, so it must be
  homogeneous, and must not contain *null*

A *regular* :eql:type:`tuple` is converted into a JSON *array* when cast
into :eql:type:`json`, whereas a *named* :eql:type:`tuple` is converted
into a JSON *object*. These casts are not reversible, i.e. it is not
possible to cast a JSON value directly into a :eql:type:`tuple`.


----------


.. eql:type:: std::json

    Arbitrary JSON data.

    Any other type can be :eql:op:`cast <cast>` to and from JSON:

    .. code-block:: edgeql-repl

        db> select <json>42;
        {'42'}
        db> select <bool>to_json('true');
        {true}

    Note that a :eql:type:`json` value can be cast into a :eql:type:`str`
    only when it is a JSON string.  Therefore, while the following will work
    as expected:

    .. code-block:: edgeql-repl

        db> select <str>to_json('"something"');
        {'something'}

    The operation below (casting a JSON array of
    string ``["a", "b", "c"]`` to a *str*) will result in an error:

    .. code-block:: edgeql-repl

        db> select <str>to_json('["a", "b", "c"]');
        InvalidValueError: expected json string or null; got JSON array

    Use the :eql:func:`to_json` and :eql:func:`to_str`
    functions to dump or parse a :eql:type:`json` value to or
    from a :eql:type:`str`:

    .. code-block:: edgeql-repl

        db> select to_json('[1, "a"]');
        {'[1, "a"]'}
        db> select to_str(<json>[1, 2]);
        {'[1, 2]'}


----------


.. eql:operator:: jsonidx: json [ int64 ] -> json

    JSON array/string indexing.

    The contents of JSON *arrays* and *strings* can also be
    accessed via ``[]``:

    .. code-block:: edgeql-repl

        db> select <json>'hello'[1];
        {'"e"'}
        db> select <json>'hello'[-1];
        {'"o"'}
        db> select to_json('[1, "a", null]')[1];
        {'"a"'}
        db> select to_json('[1, "a", null]')[-1];
        {'null'}

    The element access operator ``[]`` will raise an exception if the
    specified index is not valid for the base JSON value.  To access
    potentially out of bound indexes use the :eql:func:`json_get`
    function.


----------


.. eql:operator:: jsonslice: json [ int64 : int64 ] -> json

    JSON array/string slicing.

    JSON *arrays* and *strings* can be sliced in the same way as
    regular arrays, producing a new JSON array or string:

    .. code-block:: edgeql-repl

        db> select <json>'hello'[0:2];
        {'"he"'}
        db> select <json>'hello'[2:];
        {'"llo"'}
        db> select to_json('[1, 2, 3]')[0:2];
        {'[1, 2]'}
        db> select to_json('[1, 2, 3]')[2:];
        {'[3]'}
        db> select to_json('[1, 2, 3]')[:1];
        {'[1]'}
        db> select to_json('[1, 2, 3]')[:-2];
        {'[1]'}


----------


.. eql:operator:: jsonplus: json ++ json -> json

    JSON concatenation.

    JSON arrays, objects and strings can be concatenated with JSON values of
    the same type into a new JSON value.

    If you concatenate two JSON objects, you get a new object whose keys will
    be a union of the keys of the input objects. If a key is present in both
    objects, the value from the second object is taken.

    .. code-block:: edgeql-repl

        db> select to_json('[1, 2]') ++ to_json('[3]');
        {'[1, 2, 3]'}
        db> select to_json('{"a": 1}') ++ to_json('{"b": 2}');
        {'{"a": 1, "b": 2}'}
        db> select to_json('{"a": 1, "b": 2}') ++ to_json('{"b": 3}');
        {'{"a": 1, "b": 3}'}
        db> select to_json('"123"') ++ to_json('"456"');
        {'"123456"'}


----------


.. eql:operator:: jsonobjdest: json [ str ] -> json

    JSON object property access.

    The fields of JSON *objects* can also be accessed via ``[]``:

    .. code-block:: edgeql-repl

        db> select to_json('{"a": 2, "b": 5}')['b'];
        {'5'}
        db> select j := <json>(schema::Type {
        ...     name,
        ...     timestamp := cal::to_local_date(datetime_current(), 'UTC')
        ... })
        ... filter j['name'] = <json>'std::bool';
        {'{"name": "std::bool", "timestamp": "2019-04-02"}'}

    The field access operator ``[]`` will raise an exception if the
    specified field does not exist for the base JSON value. To access
    potentially non-existent fields use the :eql:func:`json_get` function.


----------


.. eql:function:: std::to_json(string: str) -> json

    :index: json parse loads

    Return JSON value represented by the input *string*.

    .. code-block:: edgeql-repl

        db> select to_json('[1, "hello", null]')[1];
        {'"hello"'}
        db> select to_json('{"hello": "world"}')['hello'];
        {'"world"'}


----------


.. eql:function:: std::json_array_unpack(json: json) -> set of json

    :index: array unpack

    Return elements of JSON array as a set of :eql:type:`json`.

    Calling this function on anything other than a JSON array will
    cause a runtime error.

    This function should be used if the ordering of elements is not
    important or when set ordering is preserved (such as an immediate
    input to an aggregate function).

    .. code-block:: edgeql-repl

        db> select json_array_unpack(to_json('[1, "a"]'));
        {'1', '"a"'}


----------


.. eql:function:: std::json_get(json: json, \
                                variadic path: str) -> optional json

    :index: safe navigation

    Return the JSON value at the end of the specified path or an empty set.

    This function provides "safe" navigation of a JSON value. If the
    input path is a valid path for the input JSON object/array, the
    JSON value at the end of that path is returned. If the path cannot
    be followed for any reason, the empty set is returned.

    .. code-block:: edgeql-repl

        db> select json_get(to_json('{
        ...     "q": 1,
        ...     "w": [2, "foo"],
        ...     "e": true
        ... }'), 'w', '1');
        {'"foo"'}

    This is useful when certain structure of JSON data is assumed, but
    cannot be reliably guaranteed:

    .. code-block:: edgeql-repl

        db> select json_get(to_json('{
        ...     "q": 1,
        ...     "w": [2, "foo"],
        ...     "e": true
        ... }'), 'w', '2');
        {}

    Also, a default value can be supplied by using the
    :eql:op:`coalescing <coalesce>` operator:

    .. code-block:: edgeql-repl

        db> select json_get(to_json('{
        ...     "q": 1,
        ...     "w": [2, "foo"],
        ...     "e": true
        ... }'), 'w', '2') ?? <json>'mydefault';
        {'"mydefault"'}


----------


.. eql:function:: std::json_set( \
                    target: json, \
                    variadic path: str, \
                    named only value: optional json, \
                    named only create_if_missing: bool = true, \
                    named only empty_treatment: JsonEmpty = \
                      JsonEmpty.ReturnEmpty) \
                  -> optional json

    Return an updated JSON target with a new value.

    .. warning::

      This function is only available in EdgeDB 2.0 or later.

    .. code-block:: edgeql-repl

        db> select json_set(
        ...   to_json('{"a": 10, "b": 20}'),
        ...   'a',
        ...   value := <json>true,
        ... );
        {'{"a": true, "b": 20}'}
        db> select json_set(
        ...   to_json('{"a": {"b": {}}}'),
        ...   'a', 'b', 'c',
        ...   value := <json>42,
        ... );
        {'{"a": {"b": {"c": 42}}}'}

    If ``create_if_missing`` is set to ``false``, a new path for the value
    won't be created.

    .. code-block:: edgeql-repl

        db> select json_set(
        ...   to_json('{"a": 10, "b": 20}'),
        ...   'с',
        ...   value := <json>42,
        ... );
        {'{"a": 10, "b": 20, "c": 42}'}
        db> select json_set(
        ...   to_json('{"a": 10, "b": 20}'),
        ...   'с',
        ...   value := <json>42,
        ...   create_if_missing := false,
        ... );
        {'{"a": 10, "b": 20}'}

    ``empty_treatment`` is an enumeration responsible for the behavior of the
    function if an empty set is passed to ``new_value``. It contains one of
    the following values:

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
        {'{"a": 10, "b": 20}'}
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
        {'{"a": null, "b": 20}'}
        db> select json_set(
        ...   to_json('{"a": 10, "b": 20}'),
        ...   'a',
        ...   value := <json>{},
        ...   empty_treatment := JsonEmpty.DeleteKey,
        ... );
        {'{"b": 20}'}

----------


.. eql:function:: std::json_object_unpack(json: json) -> \
                  set of tuple<str, json>

    Return set of key/value tuples that make up the JSON object.

    Calling this function on anything other than a JSON object will
    cause a runtime error.

    .. code-block:: edgeql-repl

        db> select json_object_unpack(to_json('{
        ...     "q": 1,
        ...     "w": [2, "foo"],
        ...     "e": true
        ... }'));
        {('e', 'true'), ('q', '1'), ('w', '[2, "foo"]')}


----------


.. eql:function:: std::json_typeof(json: json) -> str

    :index: type

    Return the type of the outermost JSON value as a string.

    Possible return values are: ``'object'``, ``'array'``,
    ``'string'``, ``'number'``, ``'boolean'``, ``'null'``.

    .. code-block:: edgeql-repl

        db> select json_typeof(<json>2);
        {'number'}
        db> select json_typeof(to_json('null'));
        {'null'}
        db> select json_typeof(to_json('{"a": 2}'));
        {'object'}
