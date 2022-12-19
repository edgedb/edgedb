.. _ref_cheatsheet_insert:

Inserting data
==============

.. note::

    The types used in these queries are defined :ref:`here
    <ref_cheatsheet_object_types>`.


----------


Insert basic movie stub:

.. code-block:: edgeql

    insert Movie {
        title := 'Dune',
        year := 2020,
        image := 'dune2020.jpg',
        directors := (
            select Person
            filter
                .full_name = 'Denis Villeneuve'
        )
    }


----------


Alternatively, insert a movie using JSON input value:

.. code-block:: edgeql

    with
        # Cast the JSON $input into a tuple, which we will
        # use to populate the Person record.
        data := <tuple<
            title: str,
            year: int64,
            image: str,
            directors: array<str>,
            actors: array<str>
        >> <json>$input
    insert Movie {
        title := data.title,
        year := data.year,
        image := data.image,
        directors := (
            select Person
            filter
                .full_name in array_unpack(data.directors)
        ),
        actors := (
            select Person
            filter
                .full_name in array_unpack(data.actors)
        )
    }


----------


Insert several nested objects at once:

.. code-block:: edgeql

    # Create a new review and a new user in one step.
    insert Review {
        body := 'Dune is cool',
        rating := 5,
        # The movie record already exists, so select it.
        movie := (
            select Movie
            filter
                .title = 'Dune'
                and
                .year = 2020
            # the limit is needed to satisfy the single
            # link requirement validation
            limit 1
        ),
        # This is a new user, so insert one.
        author := (
            insert User {
                name := 'dune_fan_2020',
                image := 'default_avatar.jpg',
            }
        )
    }


----------


Sometimes it's necessary to check whether some object exists and
create it if it doesn't. If this type of object has an exclusive
property, the ``unless conflict`` clause can make the ``insert``
command indempotent. So running such a command would guarantee that a
copy of the object exists without the need for more complex logic:

.. code-block:: edgeql

    # Try to create a new User
    insert User {
        name := "Alice",
        image := "default_avatar.jpg",
    }
    # and do nothing if a User with this name already exists
    unless conflict

If more than one property is exclusive, it is possible to specify
which one of them is considered when a conflict is detected:

.. code-block:: edgeql

    # Try to create a new User
    insert User {
        name := "Alice",
        image := "default_avatar.jpg",
    }
    # and do nothing if a User with this name already exists
    unless conflict on .name


----------


"Upserts" can be performed by using the ``unless conflict`` clause and
specifying what needs to be updated:

.. code-block:: edgeql

    select (
        # Try to create a new User,
        insert User {
            name := "Alice",
            image := "my_face.jpg",
        }

        # but if a User with this name already exists,
        unless conflict on .name
        else (
            # update that User's record instead.
            update User
            set {
                image := "my_face.jpg"
            }
        )
    ) {
        name,
        image
    }


----------


Rather than acting as an "upsert", the ``unless conflict`` clause can
be used to insert or select an existing record, which is handy for
inserting nested structures:

.. code-block:: edgeql

    # Create a new review and a new user in one step.
    insert Review {
        body := 'Loved it!!!',
        rating := 5,
        # The movie record already exists, so select it.
        movie := (
            select Movie
            filter
                .title = 'Dune'
                and
                .year = 2020
            # the limit is needed to satisfy the single
            # link requirement validation
            limit 1
        ),

        # This might be a new user or an existing user. Some
        # other part of the app handles authentication, this
        # endpoint is used as a generic way to post a review.
        author := (
            # Try to create a new User,
            insert User {
                name := "dune_fan_2020",
                image := "default_avatar.jpg",
            }

            # but if a User with this name already exists,
            unless conflict on .name
            # just pick that existing User as the author.
            else User
        )
    }


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`EdgeQL > Insert <ref_eql_insert>`
  * - :ref:`Reference > Commands > Insert <ref_eql_statements_insert>`
  * - `Tutorial > Data Mutations > Insert
      </tutorial/data-mutations/insert>`_
  * - `Tutorial > Data Mutations > Upsert
      </tutorial/data-mutations/upsert>`_
