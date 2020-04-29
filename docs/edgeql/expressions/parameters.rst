.. _ref_eql_expr_params:

Parameters
==========

:edb-alt-title: Query Parameters

A parameter reference is used to indicate a value that is supplied externally
to an EdgeQL expression.

An example of query, that can be parametrized:

.. code-block:: edgeql-repl

    db> SELECT Person { birth_date } FILTER .first_name = 'John';
    {
        Object {birth_date: <cal::local_date>'1971-01-01'},
    }

The equivalent parametrized query:

.. code-block:: edgeql-repl

    db> SELECT Person { birth_date } FILTER .first_name = <str>$name;
    Parameter <str>$name: John
    {
        Object {birth_date: <cal::local_date>'1971-01-01'},
    }

Note how the actual parameter value is asked by REPL in a separate prompt,
after the whole query is accepted.

Here's how parameters can be used with language bindings. e.g. with Python:

.. code-block:: python

    await connection.fetchone(
        "SELECT Person { birth_date } FILTER .first_name = <str>$name",
        name="John")

And with JavaScript:

.. code-block:: javascript

    await connection.fetchOne(
        "SELECT Person { birth_date } FILTER .first_name = <str>$name",
        {name: "John"})


Parameter Types
---------------

The following values can be turned into parameters:

* Any :ref:`scalar type <ref_datamodel_scalar_types>`
* :eql:type:`Array <array>` of any scalar type

The parameter type must be specified in angular brackets in front of the
variable name (the syntax is similar to type casts):

.. code-block:: edgeql

    INSERT Person {
        first_name := <str>$first_name,
        age := <int32>$age,
        interests := <array<str>>$interests,
    }

See :ref:`ref_eql_types` for more info on specifying types.


Optional Parameters
-------------------

By default, query parameters are ``REQUIRED`` which means that the query would
fail if parameter value is an empty set. You can use ``OPTIONAL`` keyword for
optional parameters.

Example in Python:

.. code-block:: python

    for name, age in [("John", 33), ("Jack", None)]:
        await conn.fetchone("""
            INSERT Person {
                first_name := <str>$name,
                age := <OPTIONAL str>$age,
            }
        """, name=name, age=age)

Example in JavaScript:

.. code-block:: javascript

    for(let [name, age] of [["John", 33], ["Jack", null]]) {
        await conn.fetchOne("""
            INSERT Person {
                first_name := <str>$name,
                age := <OPTIONAL str>$age,
            }
        """, {name, age})
    }

Note: denoting a *scalar EdgeQL empty set* is language-specific. We use
``None`` in Python and ``null`` in JavaScript.

The ``REQUIRED type_name`` is also a valid, although, redundant syntax:

.. code-block:: edgeql

    INSERT Person {
        first_name := <REQUIRED str>$name,
        age := <OPTIONAL str>$age,
    }


What Can be Parametrized?
-------------------------

Any data manipulation language (DML) statement can be
parametrized. Which means you can parametrize ``SELECT``, ``INSERT``
and ``UPDATE`` statements of arbitrary nesting and complexity. Any constant
value can be turned into a parameter in such queries.

``CONFIGURE`` statements and schema definition language (SDL) can **not** be
parametrized. And data definition language (DDL) has limitations on
what can be parametrized, so it's not recommended to parametrize
DDL statements.

Some of the limitations might be lifted in the future versions.


"LIMIT 1" Caveat
````````````````

While most of the time you can parametrize ``LIMIT`` clauses:

.. code-block:: edgeql

    SELECT User LIMIT <int32>$page_size;

When assigning the result of a similar query, you can see the following error:

.. code-block:: edgeql-repl

    db> INSERT TopUserName {
    ...    name := (SELECT User.name ORDER BY .rating DESC
    ...             LIMIT <int32>$limit),
    ... };
    Parameter <int32>$limit: 1
    error: possibly more than one element returned by an expression
    for a computable property 'name' declared as 'single'
       ┌── query:1:15 ───
       │
     2 │     name := (SELECT User.name ORDER BY .rating DESC
       │ ╭───^
     3 │ │            LIMIT <int32>$limit),
       │ ╰───────────────────────────────^ error
       │

In this case, there is no need to parametrize this specific ``LIMIT`` clause,
because no value other than ``1`` could be useful in this query:

.. code-block:: edgeql-repl

    db> INSERT TopUserName {
    ...    name := (SELECT User.name ORDER BY .rating
    ...             DESC LIMIT 1),
    ... };
    {Object {id: ce463a72-8a04-11ea-b04e-afc6067ece79}}
