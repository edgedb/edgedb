.. _ref_eql_types:


Types Syntax
============

Most types are just referred to by their name, however, EdgeQL has a
special syntax for referring to :eql:type:`array`,
:eql:type:`tuple`, and :eql:type:`enum` types. This syntax is used in
:ref:`property <ref_eql_sdl_props>`, :ref:`scalar
<ref_eql_sdl_scalars>`, or :ref:`function <ref_eql_sdl_functions>`
declarations as well as in type expressions involving :eql:op:`IS`
or a :eql:op:`cast <CAST>`.


.. _ref_eql_types_array:

Array
-----

An array type can be explicitly defined in an expression or schema
declaration using the following syntax:

.. eql:synopsis::

    array "<" <element_type> ">"

With the exception of other array types, any :ref:`scalar
<ref_datamodel_scalar_types>` or :ref:`collection
<ref_datamodel_collection_types>` type can be used as an array element
type.

Here's an example of using this syntax in a schema definition:

.. code-block:: sdl

    type User {
        required property name -> str;
        property favorites -> array<str>;
    }

Here's a few examples of using array types in EdgeQL queries:

.. code-block:: edgeql-repl

    db> SELECT <array<int64>>['1', '2', '3'];
    {[1, 2, 3]}
    db> SELECT [1, 2, 3] IS (array<int64>);
    {true}
    db> SELECT [(1, 'a')] IS (array<tuple<int64, str>>);
    {true}


.. _ref_eql_types_tuple:

Tuple
-----

A tuple type can be explicitly declared in an expression or schema
declaration using the following syntax:

.. eql:synopsis::

    tuple "<" <element-type>, [<element-type>, ...] ">"

A named tuple:

.. eql:synopsis::

    tuple "<" <element-name> : <element-type> [, ... ] ">"

Any type can be used as a tuple element type.

Here's an example of using this syntax in a schema definition:

.. code-block:: sdl

    type GameElement {
        required property name -> str;
        required property position -> tuple<x: int64, y: int64>;
    }

Here's a few examples of using tuple types in EdgeQL queries:

.. code-block:: edgeql-repl

    db> SELECT <tuple<int64, str>>('1', 3);
    {(1, '3')}
    db> SELECT <tuple<x: int64, y: int64>>(1, 2);
    {(x := 1, y := 2)}
    db> SELECT (1, '3') IS (tuple<int64, str>);
    {true}
    db> SELECT ([1, 2], 'a') IS (tuple<array<int64>, str>);
    {true}


.. _ref_eql_types_enum:

Enum
----

An enumerated type can be declared in a schema declaration using
the following syntax:

.. eql:synopsis::

    enum "<" <enum-values> ">"

Where :eql:synopsis:`<enum-values>` is a comma-separated list of
quoted string constants comprising the enum type.  Currently, the
only valid application of the enum declaration is to define an
enumerated scalar type:

.. code-block:: sdl

    scalar type Color extending enum<'red', 'green', 'blue'>;
