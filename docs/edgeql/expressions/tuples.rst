:orphan:

Tuples
======

.. _ref_eql_expr_tuple_ctor:

Tuple Constructor
-----------------

A tuple constructor is an expression that consists of a sequence of
comma-separated expressions enclosed in parentheses.  It produces a
tuple value:

.. eql:synopsis::

    "(" <expr> [, ... ] ")"

Named tuples are created using the following syntax:

.. eql:synopsis::

    "(" <identifier> := <expr> [, ... ] ")"

Note that *all* elements in a named tuple must have a name.

A tuple constructor automatically creates a corresponding
:eql:type:`std::tuple` type:

.. code-block:: edgeql-repl

    db> SELECT ('foo', 42).__type__.name;
    std::tuple<std::str, std::int64>


.. _ref_eql_expr_tuple_elref:

Tuple Element Reference
-----------------------

An element of a tuple can be referenced in the form:

.. eql:synopsis::

    <expr>.<element-index>

Here, *expr* is any expression that has a tuple type, and *element-name* is
either the *zero-based index* of the element, if the tuple is unnamed, or
the name of an element in a named tuple.

Examples:

.. code-block:: edgeql-repl

    db> SELECT (1, 'EdgeDB').0;
    {1}

    db> SELECT (number := 1, name := 'EdgeDB').name;
    {"EdgeDB"}

Tuples can be nested:

.. code-block:: edgeql-repl

    db> SELECT (nested_tuple := (1, 2),).nested_tuple.0;
    {1}

Referencing a non-existent tuple element will result in an error:

.. code-block:: edgeql-repl

    db> SELECT (1, 2).5;
    EdgeQLError: 5 is not a member of a tuple

    ---- query context ----

        line 1
            > SELECT (1, 2).3;
