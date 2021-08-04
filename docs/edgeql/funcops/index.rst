.. _ref_eql_funcops:

=======================
Functions and Operators
=======================

EdgeDB provides a number of functions for the standard data types.
Custom functions can be created using
:ref:`function declarations <ref_datamodel_functions>` or
:eql:stmt:`CREATE FUNCTION` DDL statements.

The syntax for a function call is as follows:

.. eql:synopsis::

    <function_name> "(" [<argument> [, <argument>, ...]] ")"

Here :eql:synopsis:`<function_name>` is a possibly qualified name of a
function, and :eql:synopsis:`<argument>` is an *expression* optionally
prefixed with an argument name and the assignment operator (``:=``).

For example, the following computes the length of a string ``'foo'``:

.. code-block:: edgeql-repl

    db> SELECT len('foo');
    {3}

Many built-in and user-defined functions operate on elements,
so they are element-wise operations. This implies that if any of the
input sets are empty, the result of applying an element function
is also empty.

.. _ref_eql_functions_agg:

Aggregate functions are *set functions* mapping arbitrary sets onto
singletons. Examples of aggregate functions include built-ins such as
:eql:func:`count` and :eql:func:`array_agg`.

.. code-block:: edgeql

    # count maps a set to an integer, specifically it returns the
    # number of elements in a set
    SELECT count(example::Issue);

    # array_agg maps a set to an array of the same type, specifically
    # it returns the array made from all of the set elements (which
    # can also be ordered)
    WITH MODULE example
    SELECT array_agg(Issue ORDER BY Issue.number);


.. toctree::
    :maxdepth: 3
    :hidden:

    generic
    set
    type
    bool
    numerics
    array
    string
    datetime
    json
    bytes
    math
    sequence
    sys
    uuid
    deprecated
