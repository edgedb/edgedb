.. _ref_cheatsheet_repl:

REPL
====

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

    db> \lT


----------


List expression aliases (the ``-v`` includes the expression value in
the listing):

.. code-block:: edgeql-repl

    db> \la -v


----------


Describe an object type:

.. code-block:: edgeql-repl

    db> \d Object
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

    db> \d decimal
    scalar type std::decimal extending std::anynumeric;


----------


Describe a function:

.. code-block:: edgeql-repl

    db> \d sum
    function std::sum(s: SET OF std::float64) ->  std::float64 {
        volatility := 'Immutable';
        using sql
    ;};
    function std::sum(s: SET OF std::decimal) ->  std::decimal {
        volatility := 'Immutable';
        using sql
    ;};
    function std::sum(s: SET OF std::float32) ->  std::float32 {
        volatility := 'Immutable';
        using sql
    ;};
    function std::sum(s: SET OF std::bigint) ->  std::bigint {
        volatility := 'Immutable';
        using sql
    ;};
    function std::sum(s: SET OF std::int64) ->  std::int64 {
        volatility := 'Immutable';
        using sql
    ;};
    function std::sum(s: SET OF std::int32) ->  std::int64 {
        volatility := 'Immutable';
        using sql
    ;};
