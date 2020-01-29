.. _ref_cheatsheet_syntax:

Special Syntax
==============

There are several kinds of strings:

.. code-block:: edgeql-repl

    db> SELECT 'A simple string';
    {'A simple string'}
    db> SELECT "Also a simple string";
    {'Also a simple string'}
    db> SELECT '\'escaped quotes\'';
    {"'escaped quotes'"}
    db> SELECT r"Raw string where escapes aren\'t special";
    {"Raw string where escapes aren\'t special"}
    db> SELECT 'A
    ...     multi line
    ...     string';
    {'A
        multi line
        string'}
    db> SELECT 'A \
    ...     string with \
    ...     line continuation';
    {'A string with line continuation'}

The literals for limited range/precision numbers (like
:eql:type:`std::int64` and :eql:type:`std::float64`):

.. code-block:: edgeql-repl

    db> SELECT 42;
    {42}
    db> SELECT 12.3;
    {12.3}

The literals for unlimited range/precision numbers (like
:eql:type:`std::bigint` and :eql:type:`std::decimal`):

.. code-block:: edgeql-repl

    db> SELECT 42n;
    {42n}
    db> SELECT 1000000000000000000000000000000000000n;
    {1000000000000000000000000000000000000n}
    db> SELECT 12.3n;
    {12.3n}
    db> SELECT 12.300000000000000000000000000000045n;
    {12.300000000000000000000000000000045n}

It's possible to quote odd identifiers:

.. code-block:: edgeql

    # If a reserved keyword needs to be used
    # as identifier it can be quoted.
    SELECT `Union`.content;

    # Identifiers containing symbols other than
    # typical alphanumerics can also be created by
    # quoting.
    SELECT `Odd-Type-Name`.value;

Link properties are accessed by using the ``@``:

.. code-block:: edgeql

    # This will just select all the link properties "list_order"
    # (if they were defined on the actors link). By itself this
    # is not a practical query, but it can be more meaningful as
    # a sub-query for a specific movie.
    SELECT Movie.actors@list_order;

    # Here's a more practical use of querying link properties
    # in a shape.
    SELECT Movie {
        title,
        actors: {
            full_name,
            @list_order,
        } ORDER BY Movie.actors@list_order
    };
