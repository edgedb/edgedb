.. _ref_reference_function_call:


Function calls
==============


EdgeDB provides a number of functions in the :ref:`standard library
<ref_std>`. It is also possible for users to :ref:`define their own
<ref_eql_sdl_functions>` functions.


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

    db> select len('foo');
    {3}

And here's an example of using a *named only* argument to provide a
default value:

.. code-block:: edgeql-repl

    db> select array_get(['hello', 'world'], 10, default := 'n/a');
    {'n/a'}



.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Functions <ref_datamodel_functions>`
  * - :ref:`SDL > Functions <ref_eql_sdl_functions>`
  * - :ref:`DDL > Functions <ref_eql_ddl_functions>`
  * - :ref:`Introspection > Functions <ref_datamodel_introspection_functions>`
  * - :ref:`Cheatsheets > Functions <ref_cheatsheet_functions>`
  * - `Tutorial > Advanced EdgeQL > User-Defined Functions
      </tutorial/advanced-edgeql/user-def-functions>`_

