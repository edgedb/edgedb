.. _ref_datamodel_scalars_json:

====
JSON
====

:edb-alt-title: JSON Type


.. eql:type:: std::json

    Arbitrary JSON data.

    Any other type (except for :eql:type:`bytes`) can be
    :eql:op:`cast <CAST>` to and from JSON:

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

    On the other hand, the below operation (casting a JSON array of
    string ``["a", "b", "c"]`` to a *str*) will result in an error:

    .. code-block:: edgeql-repl

        db> SELECT <str>to_json('["a", "b", "c"]');
        InternalServerError: expected json string, null; got json array

    Use the :eql:func:`to_json` and :eql:func:`to_str`
    functions to dump or parse a :eql:type:`json` value to or
    from a :eql:type:`str`:

    .. code-block:: edgeql-repl

        db> SELECT to_json('[1, "a"]');
        {'[1, "a"]'}
        db> SELECT to_str(<json>[1, 2]);
        {'[1, 2]'}


Constructing JSON Values
========================

JSON in EdgeDB is one of the :ref:`scalar types <ref_datamodel_scalar_types>`.
This scalar doesn't have its own literal and instead can be obtained
by casting a value into :eql:type:`json` or by using :eql:func:`to_json`:

.. code-block:: edgeql-repl

    db> SELECT to_json('{"hello": "world"}');
    {'{"hello": "world"}'}
    db> SELECT <json>'hello world';
    {'"hello world"'}

Anything in EdgeDB can be cast into :eql:type:`json`:

.. code-block:: edgeql-repl

    db> SELECT <json>2019;
    {'2019'}
    db> SELECT <json>cal::to_local_date(datetime_current(), 'UTC');
    {'"2019-04-02"'}

Any :eql:type:`Object` can be cast into :eql:type:`json`. This
produces the same JSON value as the JSON serialization of that object.
That is, the result is the same as the output of :ref:`SELECT
expression<ref_eql_statements_select>` in *JSON mode*, including the
type shape.

.. code-block:: edgeql-repl

    db> SELECT <json>(
    ...     SELECT schema::Object {
    ...         name,
    ...         timestamp := cal::to_local_date(
    ...             datetime_current(), 'UTC')
    ...     }
    ...     FILTER .name = 'std::bool');
    {'{"name": "std::bool", "timestamp": "2019-04-02"}'}

JSON values can also be cast back into scalars. This casting is
symmetrical meaning that if a scalar can be cast into JSON, only that
particular JSON type can be cast back into that scalar:

- JSON *string* can be cast into :eql:type:`str`. Casting
  :eql:type:`uuid` and :ref:`date and time types
  <ref_datamodel_scalars_datetime>` to JSON results in a JSON
  *string* representing the original value. This means that it is
  also possible to cast a JSON *string* back into these types. The
  string value has to be properly formatted (much like in case of
  a :eql:type:`str` value being cast) or else the cast will raise an
  exception.
- JSON *number* can be cast into any of
  the :ref:`numeric types <ref_datamodel_scalars_numeric>`
- JSON *boolean* can be cast into :eql:type:`bool`
- JSON *null* is special since it can be cast into an ``{}`` of any type
- JSON *array* can be cast into any valid EdgeDB array, so it must be
  homogeneous, and must not contain *null*

*Regular* :eql:type:`tuple` is converted into a JSON *array* when cast
into :eql:type:`json`. Whereas *named* :eql:type:`tuple` is converted
into a JSON *object*. These casts are not reversible, i.e. it is not
possible to cast a JSON value directly into a :eql:type:`tuple`.


See Also
--------

Scalar type
:ref:`SDL <ref_eql_sdl_scalars>`,
:ref:`DDL <ref_eql_ddl_scalars>`,
:ref:`introspection <ref_eql_introspection_scalar_types>`,
and :ref:`JSON functions and operators <ref_eql_funcops_json>`.
