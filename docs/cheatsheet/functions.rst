.. _ref_cheatsheet_functions:

Functions
=========

Define a function for counting reviews given a user name:

.. code-block:: edgeql

    CREATE FUNCTION review_count(name: str) -> int64
    USING EdgeQL $$
        WITH MODULE default
        SELECT count(
            (
                SELECT Review
                FILTER .author.name = name
            )
        )
    $$

Drop a user-defined function:

.. code-block:: edgeql

    DROP FUNCTION review_count(name: str);

Define and use polymorphic function:

.. code-block:: edgeql-repl

    db> CREATE FUNCTION make_name(name: str) -> str
    ... USING EdgeQL $$ SELECT 'my_name_' ++ name $$;
    CREATE
    db> CREATE FUNCTION make_name(name: int64) -> str
    ... USING EdgeQL $$ SELECT 'my_name_' ++ <str>name $$;
    CREATE
    q> SELECT make_name('Alice');
    {'my_name_Alice'}
    q> SELECT make_name(42);
    {'my_name_42'}
