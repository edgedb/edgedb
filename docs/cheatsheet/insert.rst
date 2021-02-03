.. _ref_cheatsheet_insert:

Insert
======

.. note::

    The types used in these queries are defined :ref:`here
    <ref_cheatsheet_types>`.


----------


Insert basic movie stub:

.. code-block:: edgeql

    INSERT Movie {
        title := 'Dune',
        year := 2020,
        image := 'dune2020.jpg',
        directors := (
            SELECT Person
            FILTER
                .full_name = 'Denis Villeneuve'
        )
    }


----------


Alternatively, insert a movie using JSON input value:

.. code-block:: edgeql

    WITH
        # Cast the JSON $input into a tuple, which we will
        # use to populate the Person record.
        data := <tuple<
            title: str,
            year: int64,
            image: str,
            directors: array<str>,
            actors: array<str>
        >> <json>$input
    INSERT Movie {
        title := data.title,
        year := data.year,
        image := data.image,
        directors := (
            SELECT Person
            FILTER
                .full_name IN array_unpack(data.directors)
        ),
        actors := (
            SELECT Person
            FILTER
                .full_name IN array_unpack(data.actors)
        )
    }


----------


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


----------


"Upserts" as well as and other combinations of :eql:stmt:`INSERT` and
some alternative operation are possible:

.. code-block:: edgeql

    WITH MODULE people
    SELECT (
        # Try to create a new Person,
        INSERT Person {
            name := "Łukasz Langa",
            is_admin := true
        }

        # but if a Person with this name already exists,
        UNLESS CONFLICT ON .name
        ELSE (
            # update that Person's record instead.
            UPDATE Person
            SET {
                is_admin := true
            }
        )
    ) {
        name,
        is_admin
    };
