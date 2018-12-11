:orphan:

Arrays
======

.. _ref_eql_expr_array_ctor:

Array Constructor
-----------------

An array constructor is an expression that consists of a sequence of
comma-separated expressions *of the same type* enclosed in square brackets.
It produces an array value:

.. eql:synopsis::

    "[" <expr> [, ...] "]"

For example:

.. code-block:: edgeql-repl

    db> SELECT [1, 2, 3];
    {
      [1, 2, 3]
    }

An empty array can also be created, but it must be used together with
a type case, since EdgeDB cannot determine the type of an array without
having elements in it:

.. code-block:: edgeql-repl

    db> SELECT [];
    EdgeQLError: could not determine the type of empty array

    db> SELECT <array<int64>>[];
    {[]}
