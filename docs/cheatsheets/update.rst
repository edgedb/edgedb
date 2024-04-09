.. _ref_cheatsheet_update:

Updating data
=============

.. note::

    The types used in these queries are defined :ref:`here
    <ref_cheatsheet_object_types>`.


----------


Flag all reviews to a specific movie:

.. code-block:: edgeql

    update Review
    filter
        Review.movie.title = 'Dune'
        and
        Review.movie.director.last_name = 'Villeneuve'
    set {
        flag := True
    }


----------


Add an actor with a specific ``list_order`` link property to a movie:

.. code-block:: edgeql

    update Movie
    filter
        .title = 'Dune'
        and
        .directors.last_name = 'Villeneuve'
    set {
        actors := (
            insert Person {
                first_name := 'Timothee',
                last_name := 'Chalamet',
                image := 'tchalamet.jpg',
                @list_order := 1,
            }
        )
    }


----------


Using a ``for`` query to set a specific ``list_order`` link property
for the actors list:

.. code-block:: edgeql

    update Movie
    filter
        .title = 'Dune'
        and
        .directors.last_name = 'Villeneuve'
    set {
        actors := (
            for x in {
                ('Timothee Chalamet', 1),
                ('Zendaya', 2),
                ('Rebecca Ferguson', 3),
                ('Jason Momoa', 4),
            }
            union (
                select Person {@list_order := x.1}
                filter .full_name = x.0
            )
        )
    }


----------


Updating a multi link by adding one more item:

.. code-block:: edgeql

    update Movie
    filter
        .title = 'Dune'
        and
        .directors.last_name = 'Villeneuve'
    set {
        actors += (
            insert Person {
                first_name := 'Dave',
                last_name := 'Bautista',
                image := 'dbautista.jpg',
            }
        )
    }


----------


Updating a multi link by removing an item:

.. code-block:: edgeql

    update Movie
    filter
        .title = 'Dune'
        and
        .directors.last_name = 'Villeneuve'
    set {
        actors -= (
            select Person
            filter
                .full_name = 'Jason Momoa'
        )
    }


----------


Update the ``list_order`` link property for a specific link:

.. code-block:: edgeql

    update Movie
    filter
        .title = 'Dune'
        and
        .directors.last_name = 'Villeneuve'
    set {
        # The += operator will allow updating only the
        # specified actor link.
        actors += (
            select Person {
                @list_order := 5,
            }
            filter .full_name = 'Jason Momoa'
        )
    }


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`EdgeQL > Update <ref_eql_update>`
  * - :ref:`Reference > Commands > Update <ref_eql_statements_update>`
  * - `Tutorial > Data Mutations > Update
      </tutorial/data-mutations/update>`_
