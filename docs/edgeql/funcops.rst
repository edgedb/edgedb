.. _ref_eql_funcops:


Functions and operators
-----------------------

All functions and operators in EdgeDB are either *element-wise* or *aggregate*.
Element-wise operations are applied to each item in a set. Aggregate operations
operate on sets *as a whole*.

.. _ref_eql_funcops_aggregate:

Aggregate operations
^^^^^^^^^^^^^^^^^^^^

An example of an aggregate function is :eql:func:`count`. It returns the number
of elements in a given set. Regardless of the size of the input set, the result
is a singleton integer.

.. code-block:: edgeql-repl

  db> select count('hello');
  {1}
  db> select count({'this', 'is', 'a', 'set'});
  {4}
  db> select count(<str>{});
  {0}

Another example is :eql:func:`array_agg`, which converts a *set* of elements
into a singleton array.

.. code-block:: edgeql-repl

  db> select array_agg({1,2,3});
  {[1, 2, 3]}


.. _ref_eql_funcops_elementwise:

Element-wise operations
^^^^^^^^^^^^^^^^^^^^^^^

By contrast, the :eql:func:`len` function is element-wise; it computes the
length of each string inside a set of strings; as such, it converts a set
of :eql:type:`str` into an equally-sized set of :eql:type:`int64`.

.. code-block:: edgeql-repl

  db> select len('hello');
  {5}
  db> select len({'hello', 'world'});
  {5, 5}


.. _ref_eql_funcops_cartesian:

Cartesian products
^^^^^^^^^^^^^^^^^^

In case of element-wise operations that
accept multiple arguments, the operation is applied to a cartesian product
cross-product of all the input sets.

.. code-block:: edgeql-repl

  db> select {'aaa', 'bbb'} ++ {'ccc', 'ddd'};
  {'aaaccc', 'aaaddd', 'bbbccc', 'bbbddd'}
  db> select {true, false} or {true, false};
  {true, true, true, false}

By extension, if any of the input sets are empty, the result of applying an
element-wise function is also empty. In effect, when EdgeDB detects an empty
set, it "short-circuits" and returns an empty set without applying the
operation.

.. code-block:: edgeql-repl

  db> select {} ++ {'ccc', 'ddd'};
  {}
  db> select {} or {true, false};
  {}

.. note::

  Certain functions and operators avoid this "short-circuit" behavior by
  marking their inputs as :ref:`optional <ref_eql_sdl_functions_syntax>`. A
  notable example of an operator with optional inputs is the :eql:op:`??
  <COALESCE>` operator.

  .. code-block:: edgeql-repl

    db> select <str>{} ?? 'default';
    {'default'}

