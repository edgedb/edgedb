.. _ref_cheatsheet_update:

Update
======

.. note::

    The types used in these queries are defined :ref:`here
    <ref_cheatsheet_types>`.


----------


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


----------


Add an actor with a specific ``list_order`` link property to a movie:

.. code-block:: edgeql

    UPDATE Movie
    FILTER
        .title = 'Dune'
        AND
        .directors.last_name = 'Villeneuve'
    SET {
        actors := (
            INSERT Person {
                first_name := 'Timothee',
                last_name := 'Chalamet',
                image := 'tchalamet.jpg',
                @list_order := 1,
            }
        )
    }


----------


Using a ``FOR`` query to set a specific ``list_order`` link property
for the actors list:

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


----------


Updating a multi link by adding one more item:

.. code-block:: edgeql

    UPDATE Movie
    FILTER
        .title = 'Dune'
        AND
        .directors.last_name = 'Villeneuve'
    SET {
        actors += (
            INSERT Person {
                first_name := 'Dave',
                last_name := 'Bautista',
                image := 'dbautista.jpg',
            }
        )
    }


----------


Updating a multi link by removing an item:

.. code-block:: edgeql

    UPDATE Movie
    FILTER
        .title = 'Dune'
        AND
        .directors.last_name = 'Villeneuve'
    SET {
        actors -= (
            SELECT Person
            FILTER
                .full_name = 'Jason Momoa'
        )
    }


----------


Update the ``list_order`` link property for a specific link:

.. code-block:: edgeql

    UPDATE Movie
    FILTER
        .title = 'Dune'
        AND
        .directors.last_name = 'Villeneuve'
    SET {
        # The += operator will allow updating only the
        # specified actor link.
        actors += (
            SELECT Person {
                @list_order := 5,
            }
            FILTER .full_name = 'Jason Momoa'
        )
    }
