.. _ref_cheatsheet_update:

Update
======

.. note::

    The types used in these queries are defined :ref:`here
    <ref_cheatsheet_types>`.

Flag all reviews to a specific movie:

.. code-block:: edgeql

    UPDATE Review
    FILTER
        Review.movie.title = 'Dune'
        AND
        Review.movie.director.last_name = 'Villeneuve'
    SET {
        flag := True
    }

Using a ``FOR`` query to set a specific ordering of the actors list:

.. code-block:: edgeql

    UPDATE Movie
    FILTER
        .title = 'Dune'
        AND
        .directors.last_name = 'Villeneuve'
    SET {
        actors := (
            FOR x IN {
                ('Timothee Chalamet', 1),
                ('Zendaya', 2),
                ('Rebecca Ferguson', 3),
                ('Jason Momoa', 4),
            }
            UNION (
                SELECT Person {@list_order := x.1}
                FILTER .full_name = x.0
            )
        )
    }

Updating a multi link by adding one more item:

.. code-block:: edgeql

    UPDATE Movie
    FILTER
        .title = 'Dune'
        AND
        .directors.last_name = 'Villeneuve'
    SET {
        actors := .actors UNION (
            INSERT Person {
                first_name := 'Dave',
                last_name := 'Bautista',
                image := 'dbautista.jpg',
            }
        )
    }
