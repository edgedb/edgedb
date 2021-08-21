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


Sometimes it's necessary to check whether some object exists and
create it if it doesn't. If this type of object has an exclusive
property, the ``UNLESS CONFLICT`` clause can make the ``INSERT``
command indempotent. So running such a command would guarantee that a
copy of the object exists without the need for more complex logic:

.. code-block:: edgeql

    # Try to create a new User
    INSERT User {
        name := "Alice",
        image := "default_avatar.jpg",
    }
    # and do nothing if a User with this name already exists
    UNLESS CONFLICT

If more than one property is exclusive, it is possible to specify
which one of them is considered when a conflict is detected:

.. code-block:: edgeql

    # Try to create a new User
    INSERT User {
        name := "Alice",
        image := "default_avatar.jpg",
    }
    # and do nothing if a User with this name already exists
    UNLESS CONFLICT ON .name


----------


"Upserts" can be performed by using the ``UNLESS CONFLICT`` clause and
specifying what needs to be updated:

.. code-block:: edgeql

    SELECT (
        # Try to create a new User,
        INSERT User {
            name := "Alice",
            image := "my_face.jpg",
        }

        # but if a User with this name already exists,
        UNLESS CONFLICT ON .name
        ELSE (
            # update that User's record instead.
            UPDATE User
            SET {
                image := "my_face.jpg"
            }
        )
    ) {
        name,
        image
    }


----------


Rather than acting as an "upsert", the ``UNLESS CONFLICT`` clause can
be used to insert or select an existing record, which is handy for
inserting nested structures:

.. code-block:: edgeql

    # Create a new review and a new user in one step.
    INSERT Review {
        body := 'Loved it!!!',
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

        # This might be a new user or an existing user. Some
        # other part of the app handles authentication, this
        # endpoint is used as a generic way to post a review.
        author := (
            # Try to create a new User,
            INSERT User {
                name := "dune_fan_2020",
                image := "default_avatar.jpg",
            }

            # but if a User with this name already exists,
            UNLESS CONFLICT ON .name
            # just pick that existing User as the author.
            ELSE User
        )
    }
