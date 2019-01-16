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

- JSON *string* can be cast into :eql:type:`str`
- JSON *number* can be cast into any of
  the :ref:`numeric types <ref_datamodel_scalars_numeric>`
- JSON *boolean* can be cast into :eql:type:`bool`
- JSON *null* is special since it can be cast into an ``{}`` of any type
- JSON *array* can be cast into any valid EdgeDB array, so it must be
  homogeneous, and must not contain *null*
- JSON *object* can be cast into a valid EdgeDB named tuple, but it must not
  contain *null*

The contents of JSON *objects* and *arrays* can also be accessed via ``[]``:

.. code-block:: edgeql-repl

    db> SELECT to_json('[1, "a", null]')[1];
    {'a'}

    db> SELECT to_json('[1, "a", null]')[-1];
    {None}

    db> SELECT j := <json>(schema::Object {
    ...     name,
    ...     timestamp := current_date()
    ... })
    ... FILTER j['name'] = <json>'std::bool';
    {{name: 'std::bool', timestamp: '2018-10-18'}}
