.. _ref_eql_expr_func_call:


Functions
=========


EdgeDB provides a number of functions in the :ref:`standard library
<ref_std>`. It is also possible for users to :ref:`define their own
<ref_eql_sdl_functions>` functions.

.. _ref_eql_expr_index_function_call:

The syntax for a function call is as follows:

.. eql:synopsis::

    <function_name> "(" [<argument> [, <argument>, ...]] ")"

    # where <argument> is:

    <expr> | <identifier> := <expr>



Here :eql:synopsis:`<function_name>` is a possibly qualified name of a
function, and :eql:synopsis:`<argument>` is an *expression* optionally
prefixed with an argument name and the assignment operator (``:=``)
for :ref:`named only <ref_eql_sdl_functions_syntax>` arguments.

For example, the following computes the length of a string ``'foo'``:

.. code-block:: edgeql-repl

    db> SELECT len('foo');
    {3}

And here's an example of using a *named only* argument to provide a
default value:

.. code-block:: edgeql-repl

    db> SELECT array_get(['hello', 'world'], 10, default := 'n/a');
    {'n/a'}




.. _ref_eql_fundamentals_aggregates:

Aggregate vs element-wise
-------------------------

A function parameter or an operand of an operator can be declared as an
*aggregate parameter*.  An aggregate parameter means that the function or
operator are called *once* on an entire set passed as a corresponding
argument, rather than being called sequentially on each element of an
argument set.  A function or an operator with an aggregate parameter is
called an *aggregate*.  Non-aggregate functions and operators are
*regular* functions and operators.

For example, basic arithmetic :ref:`operators <ref_std_math>`
are regular operators, while the :eql:func:`sum` function and the
:eql:op:`DISTINCT` operator are aggregates.

An aggregate parameter is specified using the ``SET OF`` modifier
in the function or operator declaration.  See :eql:stmt:`CREATE FUNCTION`
for details.



.. _ref_eql_fundamentals_optional:

OPTIONAL
--------

Normally, if a non-aggregate argument of a function or an operator is empty,
then the function will not be called and the result will be empty.

A function parameter or an operand of an operator can be declared as
``OPTIONAL``, in which case the function is called normally when the
corresponding argument is empty.

A notable example of a function that gets called on empty input
is the :eql:op:`coalescing <COALESCE>` operator.
