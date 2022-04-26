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

.. tip::

    The Alt-Enter / ESC+Return trick is also very useful when your cursor is in
    the middle of a long query and you want to run it directly without moving 
    the cursor to the end of the query.
    
    macOS terminals allow you to configure the left Option key to act as ESC+,
    which makes Alt-Enter also work on macOS:
    
    * in iTerm: Settings → Profiles → Keys → Let Option key: ESC+
    * in Terminal.app: Settings → Profiles → Keyboard → Use Option as Meta key


----------

Use query parameters. If your query contains a parameter, you will be prompted
for a value.

.. code-block:: edgeql-repl

    db> select 5 + <int64>$num;
    Parameter <int64>$num: 6
    {11}

----------


List databases:

.. code-block:: edgeql-repl

    db> \l
    List of databases:
      db
      tutorial


----------


Connect to a database:

.. code-block:: edgeql-repl

    db> \c my_new_project

    my_new_project>


----------


List modules:

.. code-block:: edgeql-repl

    db> \lm


----------


List object types:

.. code-block:: edgeql-repl

    db> \lt


----------


List scalar types:

.. code-block:: edgeql-repl

    db> \ls


----------


List expression aliases (the ``-v`` includes the expression value in
the listing):

.. code-block:: edgeql-repl

    db> \la -v


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
