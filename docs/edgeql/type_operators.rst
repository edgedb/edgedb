.. _ref_eql_types:


==============
Type Operators
==============

- Names of types
- Array and tuples syntax
- Casting
- Type filter operator
- IS boolean

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

    scalar type Color extending enum<Red, Green, Blue>;


.. _ref_eql_expr_index_typecast:

Type Casts
----------

A type cast expression converts the specified value to another value of
the specified type:

.. eql:synopsis::

    "<" <type> ">" <expression>

The :eql:synopsis:`<type>` must be a valid type expression denoting a non-abstract scalar or a collection type.

For example, the following expression casts an integer value into a string:

.. code-block:: edgeql-repl

    db> SELECT <str>10;
    {"10"}

See the :eql:op:`type cast operator <CAST>` section for more
information on type casting rules.
