.. _ref_eql_types:


=====
Types
=====


The foundation of EdgeQL is EdgeDB's rigorous typesystem. There is a set of EdgeQL operators and functions for changing, introspecting, and filtering by types.

.. Introspection
.. -------------

.. The entire typesystem of EdgeDB is *stored inside EdgeDB*. All types are instances of the ``schema::Type`` type. This is a :ref:`fully-qualified name <ref_name_resolution>` that refers to an object type named ``Type`` in the ``schema`` module.

.. The ``schema::Type`` type is abstract, and is extended by ``schema::ScalarType`` and ``schema::ObjectType``. To see a full list of


.. _ref_eql_types_names:

Type expressions
----------------

Type expressions are exactly what they sound like: EdgeQL expressions that refer to a type. Most commonly, these are simply the *names* of established types: ``str``, ``int64``, ``BlogPost``, etc. Arrays and tuples have a dedicated type syntax.

.. list-table::

  * - Type
    - Syntax
  * - Array
    - ``array<x>``
  * - Tuple (unnamed)
    - ``tuple<x, y, z>``
  * - Tuple (named)
    - ``tuple<foo: x, bar: y>``

For additional details on type syntax, see :ref:`Schema > Primitive Types <ref_datamodel_arrays>`.

.. _ref_eql_types_typecast:

Type casting
------------

Type casting is used to convert primitive values into another type. Casts are indicated with angle brackets containing a type expression.

.. code-block:: edgeql-repl

    db> select <str>10;
    {"10"}
    db> select <bigint>10;
    {10n}
    db> select <array<str>>[1, 2, 3];
    {['1', '2', '3']}
    db> select <array<str>>[1, 2, 3];
    {['1', '2', '3']}
    db> select <tuple<str, float64, bigint>>(1, 2, 3);
    {('1', 2, 3n)}



Type casts are useful for declaring literals for types like ``datetime``, ``uuid``, and  ``int16`` that don't have a dedicated syntax.

.. code-block:: edgeql-repl

    db> select <datetime>'1999-03-31T15:17:00Z';
    {<datetime>'1999-03-31T15:17:00Z'}
    db> select <int16>42;
    {42}
    db> select <uuid>'89381587-705d-458f-b837-860822e1b219';
    {89381587-705d-458f-b837-860822e1b219}


There are limits to what values can to be cast to a certain type. In some cases two types are entirely incompatible, like ``bool`` and ``int64``; in other cases, the source data must be in a particular format, like casting ``str`` to ``datetime``.

.. code-block:: edgeql-repl

  edgedb> select <BlogPost>10;
  QueryError: cannot cast 'std::int64' to 'default::BlogPost'
  edgedb> select <int64>'asdf';
  InvalidValueError: invalid input syntax for type std::int64: "asdf"
  edgedb> select <int16>100000000000000n;
  NumericOutOfRangeError: std::int16 out of range

For a comprehensive table of castability, see :ref:`Standard Library > Casts <ref_std_casts_table>`.


.. _ref_eql_types_intersection:

Type intersections
------------------

Type casts can only be used on primitive expressions, not object type expressions. Every object stored in the database is strongly and immutably typed; you can't simply convert an object to an object of a different type.

All elements of a given set have the same type; however, in the context of *sets of objects*, this may be misleading. A set of ``Animal`` objects may contain both ``Cat`` and ``Dog`` instances.

.. code-block:: edgeql-repl

  db> select Animal;
  {
    default::Dog {id: 9d2ce01c-35e8-11ec-acc3-83b1377efea0},
    default::Dog {id: 3bfe4900-3743-11ec-90ee-cb73d2740820},
    default::Cat {id: b0e0dd0c-35e8-11ec-acc3-abf1752973be},
  }

We can use the *type intersection* operator to restrict the elements of a set by subtype.

.. code-block:: edgeql-repl

  db> select Animal[is Dog];
  {
    default::Dog {id: 9d2ce01c-35e8-11ec-acc3-83b1377efea0},
    default::Dog {id: 3bfe4900-3743-11ec-90ee-cb73d2740820},
  }

.. Type unions
.. -----------

.. You can create a type union with the pipe operator: :eql:op:`type | type <TYPEOR>`. This is mostly commonly used for object types.

.. .. code-block:: edgeql-repl

..   db> select 5 is int32 | int64;
..   {true}
..   db> select Animal is Dog | Cat;
..   {true, true, true}


Type checking
-------------

The ``[is foo]`` "type intersection" syntax should not be confused with the *type checking* operator :eql:op:`is <IS>`.

.. code-block:: edgeql-repl

  db> select 5 is int64;
  {true}
  db> select 3.14 is not int64;
  {true}
  db> select Animal is Dog;
  {true, true, false}



The typeof operator
-------------------

The type of any expression can be extracted with the :eql:op:`typeof <TYPEOF>` operator. This can be used in any expression that expects a type.

.. code-block:: edgeql-repl

  db> select <typeof 5>'100';
  {100}
  db> select "tuna" is typeof "trout";
  {true}
