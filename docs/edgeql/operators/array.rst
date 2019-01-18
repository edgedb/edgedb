.. _ref_eql_operators_array:


=====
Array
=====

:index: array

.. _ref_eql_expr_array_elref:

Accessing Array Elements
========================

An element of an array can be referenced in the following form:

.. eql:synopsis::

    <expr> "[" <index-expr> "]"

Here, :eql:synopsis:`<expr>` is any expression of array type,
and :eql:synopsis:`<index-expr>` is any integer expression.

Example:

.. code-block:: edgeql-repl

    db> SELECT [1, 2, 3][0];
    {1}

Negative indexing is supported:

.. code-block:: edgeql-repl

    db> SELECT [1, 2, 3][-1];
    {3}

Referencing a non-existent array element will result in an
exception "array index is out of bounds":

.. code-block:: edgeql-repl

    db> SELECT [1, 2, 3][4];


.. _ref_eql_expr_array_slice:

Slicing Arrays
==============

An array slice can be referenced in the following form:

.. eql:synopsis::

    <expr> "[" <lower-bound> : <upper-bound> "]"

Here, :eql:synopsis:`<expr>` is any expression of array type,
and :eql:synopsis:`<lower-bound>` and
:eql:synopsis:`<upper-bound>` are arbitrary integer expressions.
Both :eql:synopsis:`<lower-bound>`, and
:eql:synopsis:`<upper-bound>` are optional.
An omitted :eql:synopsis:`<lower-bound>` default to zero,
and an omitted :eql:synopsis:`<upper-bound>` defaults to the
size of the array.  The upper bound is non-inclusive.

Examples:

.. code-block:: edgeql-repl

    db> SELECT [1, 2, 3][0:2];
    {[1, 2]}

    db> SELECT [1, 2, 3][2:];
    {[3]}

    db> SELECT [1, 2, 3][:1];
    {[1]}

    db> SELECT [1, 2, 3][:-2];
    {[1]}

Referencing an array slice beyond the array boundaries will result in
an empty array (unlike the direct reference to a specific index):

.. code-block:: edgeql-repl

    db> SELECT [1, 2, 3][10:20];
    {[]}
