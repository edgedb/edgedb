.. _ref_reference_cardinality:

Cardinality
===========



It's often useful to think of EdgeDB functions/operators as either
*element-wise* or *aggregate*. Element-wise operations are applied to *each
item* in a set. Aggregate operations operate on sets *as a whole*.

.. note::

  This is a simplification, but it's a useful mental model when getting
  started with EdgeDB.

.. _ref_reference_cardinality_aggregate:

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


.. _ref_reference_cardinality_elementwise:

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

.. _ref_reference_cardinality_cartesian:

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


Per-input cardinality
=====================

Ultimately, the distinction between "aggregate vs element-wise" operations is
a false one. Consider the :eql:op:`IN` operation.

.. code-block:: edgeql-repl

  db> select {1, 4} in {1, 2, 3};
  {true, false}

This operator takes two inputs. If it was "element-wise" we would expect the
cardinality of the above operation to the cartesian product of the input
cardinalities: ``2 x 3 = 6``. It it was aggregate, we'd expect a singleton
output.

Instead, the cardinality is ``2``. This operator is element-wise with respect
to the first input and aggregate with respect to the second. The "element-wise
vs aggregate" concept isn't determined on a per-function/per-operator basis;
it determined on a per-input basis.


Type qualifiers
^^^^^^^^^^^^^^^

When defining custom functions, all inputs  are element-wise by default. The
``set of`` :ref:`type qualifier  <ref_sdl_function_typequal>` is used to
designate an input as aggregate. The ``optional`` qualifier marks the input as
optional; an operation will be executed is an optional input is empty or
omitted, whereas passing an empty set for a "standard" (non-optional)
element-wise input will always result in an empty set.


Cardinality computation
^^^^^^^^^^^^^^^^^^^^^^^

To compute the final cardinality of a function/operator call, take the
cardinality of each input and apply the following transformations, based on
the type qualifier (or lack thereof) for each:

.. code-block::

  element-wise:  N -> N
  optional:      N -> N == 0 ? 1 : N
  aggregate:     N -> 1

The cardinality of the resulting set is the product of the resulting numbers.

