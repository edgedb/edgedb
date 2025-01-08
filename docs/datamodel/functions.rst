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

.. _ref_datamodel_functions_modifying:

Modifying Functions
^^^^^^^^^^^^^^^^^^^

.. versionadded:: 6.0

User-defined functions can contain DML (i.e.,
:ref:`insert <ref_eql_insert>`, :ref:`update <ref_eql_update>`,
:ref:`delete <ref_eql_delete>`) to make changes to existing data. These
functions have a :ref:`modifying <_ref_reference_volatility>` volatility.

.. code-block:: sdl

  function add_user(name: str) -> User
    using (
      insert User {
        name := name,
        joined_at := std::datetime_current(),
      }
    );

.. code-block:: edgeql-repl

    db> select add_user('Jan') {name, joined_at};
    {default::User {name: 'Jan', joined_at: <datetime>'2024-12-11T11:49:47Z'}}

Unlike other functions, the arguments of modifying functions **must** have a
:ref:`cardinality <_ref_reference_cardinality>` of ``One``.

.. code-block:: edgeql-repl

    db> select add_user({'Feb','Mar'});
    edgedb error: QueryError: possibly more than one element passed into
    modifying function
    db> select add_user(<str>{});
    edgedb error: QueryError: possibly an empty set passed as non-optional
    argument into modifying function

Optional arguments can still accept empty sets. For example, if ``add_user``
was defined as:

.. code-block:: sdl

  function add_user(name: str, joined_at: optional datetime) -> User
    using (
      insert User {
        name := name,
        joined_at := joined_at ?? std::datetime_current(),
      }
    );

then the following queries are valid:

.. code-block:: edgeql-repl

    db> select add_user('Apr', <datetime>{}) {name, joined_at};
    {default::User {name: 'Apr', joined_at: <datetime>'2024-12-11T11:50:51Z'}}
    db> select add_user('May', <datetime>'2024-12-11T12:00:00-07:00') {name, joined_at};
    {default::User {name: 'May', joined_at: <datetime>'2024-12-11T12:00:00Z'}}


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

