.. _ref_cheatsheet_functions:

Declaring functions
===================

Define a function for counting reviews given a user name:

.. code-block:: edgeql

    CREATE FUNCTION review_count(name: str) -> int64
    USING EdgeQL $$
        with module default
        select count(
            (
                select Review
                filter .author.name = name
            )
        )
    $$


----------


Drop a user-defined function:

.. code-block:: edgeql

    DROP FUNCTION review_count(name: str);


----------


Define and use polymorphic function:

.. code-block:: edgeql-repl

    db> CREATE FUNCTION make_name(name: str) -> str
    ... USING EdgeQL $$ select 'my_name_' ++ name $$;
    CREATE
    db> CREATE FUNCTION make_name(name: int64) -> str
    ... USING EdgeQL $$ select 'my_name_' ++ <str>name $$;
    CREATE
    q> select make_name('Alice');
    {'my_name_Alice'}
    q> select make_name(42);
    {'my_name_42'}

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Functions <ref_datamodel_functions>`
  * - :ref:`SDL > Functions <ref_eql_sdl_functions>`
  * - :ref:`DDL > Functions <ref_eql_ddl_functions>`
  * - :ref:`Reference > Function calls <ref_reference_function_call>`
  * - :ref:`Introspection > Functions <ref_eql_introspection_functions>`

