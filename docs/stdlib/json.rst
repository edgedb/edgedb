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

JSON in EdgeDB is a :ref:`scalar type <ref_datamodel_scalar_types>`. This type
doesn't have its own literal, and instead can be obtained by either casting a
value to the :eql:type:`json` type, or by using the :eql:func:`to_json`
function:

.. code-block:: edgeql-repl

    db> select to_json('{"hello": "world"}');
    {'{"hello": "world"}'}
    db> select <json>'hello world';
    {'"hello world"'}

Any value in EdgeDB can be cast to a :eql:type:`json` type as well:

.. code-block:: edgeql-repl

    db> select <json>2019;
    {'2019'}
    db> select <json>cal::to_local_date(datetime_current(), 'UTC');
    {'"2019-04-02"'}

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
    {'{"name": "std::bool", "timestamp": "2019-04-02"}'}

JSON values can also be cast back into scalars. Casting JSON is symmetrical:
if a scalar value can be cast into JSON, a compatible JSON value can be cast
into a scalar of that type. Some scalar types will have specific conditions
for casting:

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
- JSON arrays can be cast to an array, as long as it's homogeneous, does not
  contain ``null`` as an element of the array, and does not contain another
  array., as long as it's homogeneous and do not contain *null* as a value.

A named :eql:type:`tuple` is converted into a JSON object when cast as a
:eql:type:`json` unlike a standard :eql:type:`tuple` which is converted into a
JSON array. Unlike other casts to JSON, tuple casts to JSON are *not*
reversible (i.e., it is not possible to cast a JSON value directly into a
:eql:type:`tuple`).


----------


.. eql:type:: std::json

    Arbitrary JSON data.

    .. code-block:: edgeql-repl

        db> select <json>42;
        {'42'}
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
    :eql:type:`str` value. Use the :eql:func`to_json` function to parse a JSON
    string to a :eql:type:`json` value:

    .. code-block:: edgeql-repl

        db> select to_json('[1, "a"]');
        {'[1, "a"]'}
        db> select to_str(<json>[1, 2]);
        {'[1, 2]'}


----------


.. eql:operator:: jsonidx: json [ int64 ] -> json

    Indexes an array or string of :eql:type:`json`.

    The operator produces the value at the given index as :eql:type:`json`.
    
    Examples:

    .. code-block:: edgeql-repl

        db> select <json>'hello'[1];
        {'"e"'}
        db> select <json>'hello'[-1];
        {'"o"'}
        db> select to_json('[1, "a", null]')[1];
        {'"a"'}
        db> select to_json('[1, "a", null]')[-1];
        {'null'}

    This may raise an exception if the specified index is not valid for the
    base JSON value. To access an index that is potentially out of bounds, use
    :eql:func:`json_get`.


----------


.. eql:operator:: jsonslice: json [ int64 : int64 ] -> json

    Produces a JSON value comprising a portion of the existing JSON value.

    Arrays and strings of JSON can be sliced in the same manner as an
    :eql:type:`array`, producing a :eql:type:`json` value:

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

    Concatenates two :eql:type:`json` arrays, objects, or string values into one.

    The result is a new :eql:type:`json` value containing the elements of both
    concatenated values. If concatenated objects have identical keys, the value
    of the second one will be used.
    
    Examples:
    
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

    Accesses an element of a :eql:type:`json` object given its key:

    This results in a :eql:type:`json` type. The fields of any JSON object may
    also be accessed via ``[]``:

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
    potentially non-existent fields, use the :eql:func:`json_get` function.


----------


.. eql:function:: std::to_json(string: str) -> json

    :index: json parse loads

    Returns a :eql:type:`json` value parsed from the given :eql:type:`str`.
    
    Examples:

    .. code-block:: edgeql-repl

        db> select to_json('[1, "hello", null]');
        {'[1, "hello", null]'}
        db> select to_json('{"hello": "world"}');
        {'{"hello": "world"}'}


----------


.. eql:function:: std::json_array_unpack(json: json) -> set of json

    :index: array unpack

    Returns the elements of a JSON array as a set of :eql:type:`json`.

    Calling this function on anything other than an :eql:type:`json` array will
    result in a runtime error.

    .. code-block:: edgeql-repl

        db> select json_array_unpack(to_json('[1, "a"]'));
        {'1', '"a"'}

    This function should be only used if the ordering of elements are not
    deemed important, or when the ordering of the set is preserved (such as an
    immediate input to an aggregate function)


----------


.. eql:function:: std::json_get(json: json, \
                                variadic path: str) -> optional json

    :index: safe navigation

    Returns a value from a :eql:type:`json` object or array given its path.

    This function provides "safe" navigation of a JSON value. If the
    input path is a valid path for the input JSON object/array, the
    JSON value at the end of that path is returned. If the path cannot
    be followed for any reason, the empty set is returned:

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
    :eql:op:`coalescence <coalesce>` operator:

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

    Returns an updated :eql:type:`json` target with a new value.

    .. note::

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

    If *create_if_missing* is set to ``false``, a new path for the value
    won't be created:

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

    *empty_treatment* can be any value from the `JsonEmpty` enumeration. It
    defines the behavior of the function if an empty set is passed as
    *new_value*. `JsonEmpty` contains these values:

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

    Returns the data in a :eql:type:`json` object as a set of key/value tuples.

    Calling this function on anything other than a JSON object will
    result in a runtime error.

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

    Returns the :eql:type:`str` type of the outermost :eql:type:`json` value.

    Possible return values are ``'object'``, ``'array'``,
    ``'string'``, ``'number'``, ``'boolean'``, or ``'null'``:

    .. code-block:: edgeql-repl

        db> select json_typeof(<json>2);
        {'number'}
        db> select json_typeof(to_json('null'));
        {'null'}
        db> select json_typeof(to_json('{"a": 2}'));
        {'object'}
