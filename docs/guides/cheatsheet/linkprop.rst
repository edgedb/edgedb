.. _ref_cheatsheet_syntax:

Link Properties
===============

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
