.. _ref_datamodel_functions:

=========
Functions
=========


Functions are ways to transform one set of data into another.

User-defined Functions
----------------------

It is also possible to define custom functions. For example, consider
a function that adds an exclamation mark ``'!'`` at the end of the
string:

.. code-block:: sdl

    function exclamation(word: str) -> str
        using (word ++ '!');

This function accepts a :eql:type:`str` as an argument and produces a
:eql:type:`str` as output as well.

.. code-block:: edgeql-repl

    test> select exclamation({'Hello', 'World'});
    {'Hello!', 'World!'}



.. _ref_eql_fundamentals_aggregates:

Aggregate vs element-wise operations
------------------------------------

Consider the :ref:`function <ref_std>`
:eql:func:`len` used to transform a set of :eql:type:`str` into a set
of :eql:type:`int64`:

.. code-block:: edgeql-repl

  db> select len({'hello', 'world'});
  {5, 5}


This behavior is known as an *element-wise* operation: the ``len`` function is applied to each element of the input set.

In case of operations that accept multiple arguments, the operation is applied to a cartesian product cross-product of all the input sets.

.. code-block:: edgeql-repl

  db> select {'aaa', 'bbb'} ++ {'ccc', 'ddd'};
  {'aaaccc', 'aaaddd', 'bbbccc', 'bbbddd'}
  db> select {true, false} or {true, false};
  {true, true, true, false}

By extension, if any of the input sets are empty, the result of applying an element-wise function is also empty. In effect, when EdgeDB detects an empty set, it "short-circuits" and returns an empty set without applying the operation.

.. code-block:: edgeql-repl

  db> select {} ++ {'ccc', 'ddd'};
  {}
  db> select {} or {true, false};
  {}



.. _ref_eql_fundamentals_optional:

Optional parameters
-------------------

Sometimes, it may be desirable to override this "short-circuit" behavior and allow the operation to be applied on the empty set. This requires marking the input with the :ref:`optional <ref_eql_sdl_functions_syntax>` keyword. A notable example of a function that gets called even when one input is empty is the :eql:op:`coalescing <COALESCE>` operator.

.. code-block:: edgeql-repl

  test> select <str>{} ?? 'default'
  {'default'}

You can also provide a default value for optional arguements. argument is omitted entirely. Here are some results this
function produces:

.. code-block:: edgeql-repl

    test> SELECT exclamation({'Hello', 'World'});
    {'Hello!', 'World!'}
    test> SELECT exclamation(<str>{});
    {'!!!'}
    test> SELECT exclamation();
    {'!!!'}


Aggregate operations
--------------------

See Also
--------

Function
:ref:`SDL <ref_eql_sdl_functions>`,
:ref:`DDL <ref_eql_ddl_functions>`,
and :ref:`introspection <ref_eql_introspection_functions>`.



