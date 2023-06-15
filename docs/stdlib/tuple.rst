.. _ref_std_tuple:

======
Tuples
======

A tuple type is a heterogeneous sequence of other types. Tuples can be either
*named* or *unnamed* (the default).

Constructing tuples
-------------------

A tuple constructor is an expression that consists of a sequence of
comma-separated expressions enclosed in parentheses.  It produces a
tuple value:

.. eql:synopsis::

    "(" <expr> [, ... ] ")"

Declare a *named tuple*:

.. eql:synopsis::

    "(" <identifier> := <expr> [, ... ] ")"

*All* elements in a named tuple must have a name.

A tuple constructor automatically creates a corresponding
:ref:`tuple type <ref_eql_types_tuple>`.


.. _ref_std_tuple_accessor:

Accessing elements
------------------

An element of a tuple can be referenced in the form:

.. eql:synopsis::

    <expr>.<element-index>

Here, :eql:synopsis:`<expr>` is any expression that has a tuple type,
and :eql:synopsis:`<element-index>` is either the *zero-based index*
of the element or the name of an element in a named tuple.

Examples:

.. code-block:: edgeql-repl

    db> select (1, 'EdgeDB').0;
    {1}

    db> select (number := 1, name := 'EdgeDB').name;
    {"EdgeDB"}

    db> select (number := 1, name := 'EdgeDB').1;
    {"EdgeDB"}

Nesting tuples
--------------

Tuples can be nested:

.. code-block:: edgeql-repl

    db> select (nested_tuple := (1, 2)).nested_tuple.0;
    {1}

Referencing a non-existent tuple element will result in an error:

.. code-block:: edgeql-repl

    db> select (1, 2).5;
    EdgeQLError: 5 is not a member of a tuple

    ---- query context ----

        line 1
            > select (1, 2).3;


.. _ref_eql_types_tuple:

Type syntax
-----------

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
    :version-lt: 3.0

    type GameElement {
        required property name -> str;
        required property position -> tuple<x: int64, y: int64>;
    }

.. code-block:: sdl

    type GameElement {
        required name: str;
        required position: tuple<x: int64, y: int64>;
    }

Here's a few examples of using tuple types in EdgeQL queries:

.. code-block:: edgeql-repl

    db> select <tuple<int64, str>>('1', 3);
    {(1, '3')}
    db> select <tuple<x: int64, y: int64>>(1, 2);
    {(x := 1, y := 2)}
    db> select (1, '3') is (tuple<int64, str>);
    {true}
    db> select ([1, 2], 'a') is (tuple<array<int64>, str>);
    {true}


.. eql:type:: std::tuple

    :index: tuple

    A tuple type is a heterogeneous sequence of other types.

    Tuple elements can optionally have names,
    in which case the tuple is called a *named tuple*.

    Any type can be used as a tuple element type.

    A tuple type is created implicitly when a :ref:`tuple constructor
    <ref_std_tuple>` is used:

    .. code-block:: edgeql-repl

        db> select ('foo', 42);
        {('foo', 42)}

    Two tuples are equal if all of their elements are equal and in the same
    order.  Note that element names in named tuples are not significant for
    comparison:

    .. code-block:: edgeql-repl

        db> select (1, 2, 3) = (a := 1, b := 2, c := 3);
        {true}

    The syntax of a tuple type declaration can be found in :ref:`this
    section <ref_eql_types_tuple>`.



