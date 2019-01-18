.. _ref_eql_operators_json:


====
JSON
====

JSON in EdgeDB is one of the :ref:`scalar types <ref_datamodel_scalar_types>`.
This scalar doesn't have its own literal and instead can be obtained
by casting a value into :eql:type:`json` or by using :eql:func:`to_json`:

.. code-block:: edgeql-repl

    db> SELECT to_json('{"hello": "world"}');
    {{hello: 'world'}}

    db> SELECT <json>'hello world';
    {'hello world'}

Anything in EdgeDB can be cast into :eql:type:`json`:

.. code-block:: edgeql-repl

    db> SELECT <json>2018;
    {2018}

    db> SELECT <json>current_date();
    {'2018-10-18'}

    db> SELECT <json>(
    ...     SELECT schema::Object {
    ...         name,
    ...         timestamp := current_date()
    ...     }
    ...     FILTER .name = 'std::bool');
    {{name: 'std::bool', timestamp: '2018-10-18'}}

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


Casting Objects into JSON
=========================

Any :eql:type:`Object` can be cast into :eql:type:`json`. This
produces the same JSON value as the JSON serialization of that object.
That is, the result is the same as the output of :ref:`SELECT
expression<ref_eql_statements_select>` in *JSON mode*, including the
type shape.

.. code-block:: edgeql-repl

    db> WITH MODULE schema
    ... SELECT <json>(Type {
    ...     name,
    ...     timestamp := <naive_date>datetime_current()
    ... })
    ... FILTER Type.name = 'std::bool';
    {{name: 'std::bool', timestamp: '2019-01-18'}}


Accessing JSON Array Elements
=============================

The contents of JSON *arrays* can also be accessed via ``[]``:

.. code-block:: edgeql-repl

    db> SELECT to_json('[1, "a", null]')[1];
    {'a'}

    db> SELECT to_json('[1, "a", null]')[-1];
    {None}

The element access operator ``[]`` will raise an exception if the
specified index is not valid for the base JSON value. To access
potentially out of bound indexes use the :eql:func:`json_get`
function.


Slicing JSON Arrays
===================

JSON arrays can be sliced in the same way as regular arrays, producing
a new JSON array:

.. code-block:: edgeql-repl

    db> SELECT to_json('[1, 2, 3]')[0:2];
    {[1, 2]}

    db> SELECT to_json('[1, 2, 3]')[2:];
    {[3]}

    db> SELECT to_json('[1, 2, 3]')[:1];
    {[1]}

    db> SELECT to_json('[1, 2, 3]')[:-2];
    {[1]}


Accessing JSON Object Fields
============================

The fields of JSON *objects* can also be accessed via ``[]``:

.. code-block:: edgeql-repl

    db> SELECT to_json('{"a": 2, "b": 5}')['b'];
    {5}

    db> SELECT j := <json>(schema::Type {
    ...     name,
    ...     timestamp := <naive_date>datetime_current()
    ... })
    ... FILTER j['name'] = <json>'std::bool';
    {{name: 'std::bool', timestamp: '2019-01-18'}}

The field access operator ``[]`` will raise an exception if the
specified field does not exist for the base JSON value. To access
potentially non-existent fields use the :eql:func:`json_get` function.
