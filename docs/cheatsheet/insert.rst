.. _ref_cheatsheet_insert:

Insert
======

.. note::

    The types used in these queries are defined :ref:`here
    <ref_cheatsheet_types>`.

Insert basic movie stub:

.. code-block:: edgeql

    INSERT Movie {
        title := 'Dune',
        year := 2020,
        image := 'dune2020.jpg',
        directors := (
            SELECT Person
            FILTER
                .last_name = 'Villeneuve'
        )
    }

Insert several nested objects at once:

.. code-block:: edgeql

    # Create a new review and a new user in one step.
    INSERT Review {
        body := 'Dune is cool',
        rating := 5,
        # The movie record already exists, so SELECT it.
        movie := (
            SELECT Movie
            FILTER
                .title = 'Dune'
                AND
                .year = 2020
            # the LIMIT is needed to satisfy the single
            # link requirement validation
            LIMIT 1
        ),
        # This is a new user, so INSERT one.
        author := (
            INSERT User {
                name := 'dune_fan_2020',
                image := 'default_avatar.jpg',
            }
        )
    }
