.. _ref_cheatsheet_repl:

Using the REPL
==============

Execute a query. To execute a query in the REPL, terminate the statement with
a semicolon and press "ENTER".

.. code-block:: edgeql-repl

    db> select 5;
    {5}

Alternatively, you can run the query without a semicolon by hitting Alt-Enter
on Windows/Linux, or Esc+Return on macOS.

.. code-block:: edgeql-repl

    db> select 5
    {5}


Type ``Alt-Enter`` to run the query without having the cursor to the end of
the query.

.. note::

    This doesn't work by default on macOS, however it's possible to enable it
    with a quick fix.

    * in Terminal.app: Settings → Profiles → Keyboard → Check
      "Use Option as Meta key"
    * in iTerm: Settings → Profiles → Keys → Let Option key: ESC+

    Alternatively you can use the ``esc+return`` shortcut.


----------

Use query parameters. If your query contains a parameter, you will be prompted
for a value.

.. code-block:: edgeql-repl

    db> select 5 + <int64>$num;
    Parameter <int64>$num: 6
    {11}

----------

Commands
^^^^^^^^


.. list-table::

    * - Options
      - ``-v`` = verbose

        ``-s`` = show system objects

        ``-I`` = case-sensitive match

    * - ``\d [-v] NAME``
      - Describe a schema object.

    * - ``\ds, \describe schema``
      - Describe the entire schema.

    * - ``\list databases``

        ``alias: \l``
      - List databases.
    * - ``\list scalars [-sI] [pattern]``

        ``alias: \ls``
      - List scalar types.
    * - ``\list types [-sI] [pattern]``

        ``alias: \lt``
      - List object types.
    * - ``\list roles [-I]``

        ``alias: \lr``
      - List roles.
    * - ``\list modules [-I]``

        ``alias: \lm``
      - List modules.
    * - ``\list aliases [-Isv] [pattern]``

        ``alias: \la``
      - List expression aliases.
    * - ``\list casts [-I] [pattern]``

        ``alias: \lc``
      - List casts.
    * - ``\list indexes [-Isv] [pattern]``

        ``alias: \li``
      - List indexes.

    * - ``\dump <filename>``
      - Dump the current database to file.

    * - ``\restore <filename>``
      - Restore the database from a dump file.

    * - ``\s, \history``
      - Show query history

    * - ``\e, \edit [N]``
      - Spawn $EDITOR to edit history entry N.

        Then use the output as the input.

    * - ``\set [<option> [<value>]]``
      - View/change a setting.

        Type ``\set`` to see all available settings.

    * - ``\c, \connect [<dbname>]``
      - Connect to a particular database.


Sample usage
^^^^^^^^^^^^

List databases:

.. code-block:: edgeql-repl

    db> \ls
    List of databases:
      db
      tutorial



----------


Connect to a database:

.. code-block:: edgeql-repl

    db> \c my_new_project
    my_new_project>


----------


Describe an object type:

.. code-block:: edgeql-repl

    db> \d object Object
    abstract type std::Object extending std::BaseObject {
        required single link __type__ -> schema::Type {
            readonly := true;
        };
        required single property id -> std::uuid {
            readonly := true;
        };
    };


----------


Describe a scalar type:

.. code-block:: edgeql-repl

    db> \d object decimal
    scalar type std::decimal extending std::anynumeric;


----------


Describe a function:

.. code-block:: edgeql-repl

    db> \d object sum
    function std::sum(s: set of std::bigint) ->  std::bigint {
        volatility := 'Immutable';
        annotation std::description := 'Return the sum of the set of numbers.';
        using sql function 'sum'
    ;};
    function std::sum(s: set of std::int32) ->  std::int64 {
        volatility := 'Immutable';
        annotation std::description := 'Return the sum of the set of numbers.';
        using sql function 'sum'
    ;};
    function std::sum(s: set of std::decimal) ->  std::decimal {
        volatility := 'Immutable';
        annotation std::description := 'Return the sum of the set of numbers.';
        using sql function 'sum'
    ;};
    function std::sum(s: set of std::float32) ->  std::float32 {
        volatility := 'Immutable';
        annotation std::description := 'Return the sum of the set of numbers.';
        using sql function 'sum'
    ;};
    function std::sum(s: set of std::int64) ->  std::int64 {
        volatility := 'Immutable';
        annotation std::description := 'Return the sum of the set of numbers.';
        using sql function 'sum'
    ;};
    function std::sum(s: set of std::float64) ->  std::float64 {
        volatility := 'Immutable';
        annotation std::description := 'Return the sum of the set of numbers.';
        using sql function 'sum'
    ;};
