.. _ref_eql_expr_array_ctor:


Arrays
======

An array constructor is an expression that consists of a sequence of
comma-separated expressions *of the same type* enclosed in square brackets.
It produces an array value:

.. eql:synopsis::

    "[" <expr> [, ...] "]"

For example:

.. code-block:: edgeql-repl

    db> SELECT [1, 2, 3];
    {[1, 2, 3]}
    db> SELECT [('a', 1), ('b', 2), ('c', 3)];
    {[('a', 1), ('b', 2), ('c', 3)]}


An empty array can also be created, but it must be used together with
a type cast, since EdgeDB cannot determine the type of an array without
having elements in it:

.. code-block:: edgeql-repl

    db> SELECT [];
    QueryError: expression returns value of indeterminate type
    Hint: Consider using an explicit type cast.
    ### SELECT [];
    ###        ^

    db> SELECT <array<int64>>[];
    {[]}


See also the list of array
:ref:`functions and operators <ref_std_array>`.
