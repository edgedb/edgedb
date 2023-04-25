.. _ref_datamodel_functions:

=========
Functions
=========

.. note::

  This page documents how to define custom functions, however EdgeDB provides a
  large library of built-in functions and operators. These are documented in
  :ref:`Standard Library <ref_std>`.

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


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Functions <ref_eql_sdl_functions>`
  * - :ref:`DDL > Functions <ref_eql_ddl_functions>`
  * - :ref:`Reference > Function calls <ref_reference_function_call>`
  * - :ref:`Introspection > Functions <ref_datamodel_introspection_functions>`
  * - :ref:`Cheatsheets > Functions <ref_cheatsheet_functions>`
  * - `Tutorial > Advanced EdgeQL > User-Defined Functions
      </tutorial/advanced-edgeql/user-def-functions>`_

