.. _ref_eql_types:


=====
Types
=====

The foundation of EdgeQL is EdgeDB's rigorous type system. There is a set of
EdgeQL operators and functions for changing, introspecting, and filtering by
types.

.. _ref_eql_types_names:

Type expressions
----------------

Type expressions are exactly what they sound like: EdgeQL expressions that
refer to a type. Most commonly, these are simply the *names* of established
types: ``str``, ``int64``, ``BlogPost``, etc. Arrays and tuples have a
dedicated type syntax.

.. list-table::

  * - **Type**
    - **Syntax**
  * - Array
    - ``array<x>``
  * - Tuple (unnamed)
    - ``tuple<x, y, z>``
  * - Tuple (named)
    - ``tuple<foo: x, bar: y>``

For additional details on type syntax, see :ref:`Schema > Primitive Types
<ref_datamodel_primitives>`.

.. _ref_eql_types_typecast:

Type casting
------------

Type casting is used to convert primitive values into another type. Casts are
indicated with angle brackets containing a type expression.

.. code-block:: edgeql-repl

    db> select <str>10;
    {"10"}
    db> select <bigint>10;
    {10n}
    db> select <array<str>>[1, 2, 3];
    {['1', '2', '3']}
    db> select <tuple<str, float64, bigint>>(1, 2, 3);
    {('1', 2, 3n)}



Type casts are useful for declaring literals for types like ``datetime``,
``uuid``, and  ``int16`` that don't have a dedicated syntax.

.. code-block:: edgeql-repl

    db> select <datetime>'1999-03-31T15:17:00Z';
    {<datetime>'1999-03-31T15:17:00Z'}
    db> select <int16>42;
    {42}
    db> select <uuid>'89381587-705d-458f-b837-860822e1b219';
    {89381587-705d-458f-b837-860822e1b219}


There are limits to what values can be cast to a certain type. In some cases
two types are entirely incompatible, like ``bool`` and ``int64``; in other
cases, the source data must be in a particular format, like casting ``str`` to
``datetime``. For a comprehensive table of castability, see :ref:`Standard
Library > Casts <ref_eql_casts_table>`.

Type casts can only be used on primitive expressions, not object type
expressions. Every object stored in the database is strongly and immutably
typed; you can't simply convert an object to an object of a different type.

.. code-block:: edgeql-repl

  db> select <BlogPost>10;
  QueryError: cannot cast 'std::int64' to 'default::BlogPost'
  db> select <int64>'asdf';
  InvalidValueError: invalid input syntax for type std::int64: "asdf"
  db> select <int16>100000000000000n;
  NumericOutOfRangeError: std::int16 out of range


.. lint-off

.. versionadded:: 3.0

    You can cast a UUID into an object:

    .. code-block:: edgeql-repl

        db> select <Hero><uuid>'01d9cc22-b776-11ed-8bef-73f84c7e91e7';
        {default::Hero {id: 01d9cc22-b776-11ed-8bef-73f84c7e91e7}}

    If you try to cast a UUID that no object of the type has as its ``id``
    property, you'll get an error:

    .. code-block:: edgeql-repl

        db> select <Hero><uuid>'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
        edgedb error: CardinalityViolationError: 'default::Hero' with id 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa' does not exist

.. lint-on


.. _ref_eql_types_intersection:

Type intersections
------------------

All elements of a given set have the same type; however, in the context of
*sets of objects*, this type might be ``abstract`` and contain elements of
multiple concrete subtypes. For instance, a set of ``Media`` objects may
contain both ``Movie`` and ``TVShow`` objects.

.. code-block:: edgeql-repl

  db> select Media;
  {
    default::Movie {id: 9d2ce01c-35e8-11ec-acc3-83b1377efea0},
    default::Movie {id: 3bfe4900-3743-11ec-90ee-cb73d2740820},
    default::TVShow {id: b0e0dd0c-35e8-11ec-acc3-abf1752973be},
  }

We can use the *type intersection* operator to restrict the elements of a set
by subtype.

.. code-block:: edgeql-repl

  db> select Media[is Movie];
  {
    default::Movie {id: 9d2ce01c-35e8-11ec-acc3-83b1377efea0},
    default::Movie {id: 3bfe4900-3743-11ec-90ee-cb73d2740820},
  }

Logically, this computes the intersection of the ``Media`` and ``Movie`` sets;
since only ``Movie`` objects occur in both sets, this can be conceptualized as
a "filter" that removes all elements that aren't of type ``Movie``.

.. Type unions
.. -----------

.. You can create a type union with the pipe operator: :eql:op:`type | type
.. <typeor>`. This is mostly commonly used for object types.

.. .. code-block:: edgeql-repl

..   db> select 5 is int32 | int64;
..   {true}
..   db> select Media is Movie | TVShow;
..   {true, true, true}


Type checking
-------------

The ``[is foo]`` "type intersection" syntax should not be confused with the
*type checking* operator :eql:op:`is`.

.. code-block:: edgeql-repl

  db> select 5 is int64;
  {true}
  db> select {3.14, 2.718} is not int64;
  {true, true}
  db> select Media is Movie;
  {true, true, false}


The ``typeof`` operator
-----------------------

The type of any expression can be extracted with the :eql:op:`typeof`
operator. This can be used in any expression that expects a type.

.. code-block:: edgeql-repl

  db> select <typeof 5>'100';
  {100}
  db> select "tuna" is typeof "trout";
  {true}

Introspection
-------------

The entire type system of EdgeDB is *stored inside EdgeDB*. All types are
introspectable as instances of the ``schema::Type`` type. For a set of
introspection examples, see :ref:`Guides > Introspection
<ref_datamodel_introspection>`. To try introspection for yourself, see `our
interactive introspection tutorial
</tutorial/advanced-edgeql/introspection>`_.
