.. _ref_eql_types:


==============
Type Operators
==============

- Names of types
- Array and tuples syntax
- Casting
- Type filter operator
- IS boolean

The foundation of EdgeQL is EdgeDB's rigorous typesystem. There is a set of EdgeQL operators and functions for changing, introspecting, and filtering by types.

.. Introspection
.. -------------

.. The entire typesystem of EdgeDB is *stored inside EdgeDB*. All types are instances of the ``schema::Type`` type. This is a :ref:`fully-qualified name <ref_name_resolution>` that refers to an object type named ``Type`` in the ``schema`` module.

.. The ``schema::Type`` type is abstract, and is extended by ``schema::ScalarType`` and ``schema::ObjectType``. To see a full list of


.. _ref_eql_types_names:

Naming types
------------

Most types are just referred to by their name: ``str``, ``int64``, ``BlogPost``, etc. However, arrays and tuples have a dedicated type syntax.

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

Type casting is used to convert a primitive expressions to another type. Casts are indicated with angle brackets containing a type expression.

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

For a comprehensive table of castability, refer to the :ref:`Standard Library > Casts <ref_std_casts_table>`.


.. _ref_eql_types_intersection:

Type intersections
------------------

Type casts can only be used on primitive expressions, not object type expressions. For objects, use type intersections.



The :eql:synopsis:`<type>` must be a valid type expression denoting a non-abstract scalar or a collection type.

For example, the following expression casts an integer value into a string:

.. code-block:: edgeql-repl

    db> SELECT <str>10;
    {"10"}

See the :eql:op:`type cast operator <CAST>` section for more
information on type casting rules.
