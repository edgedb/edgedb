.. _ref_cheatsheet_select:

Selecting data
==============

.. note::

    The types used in these queries are defined :ref:`here
    <ref_cheatsheet_object_types>`.


----------


Select a Movie with associated actors and reviews with their authors:

.. code-block:: edgeql

    select Movie {
        id,
        title,
        year,
        description,

        actors: {
            id,
            full_name,
        },

        reviews := .<movie[is Review] {
            id,
            body,
            rating,
            author: {
                id,
                name,
            }
        },
    }
    filter .id = <uuid>'09c34154-4148-11ea-9c68-5375ca908326'


----------


Select movies with Keanu Reeves:

.. code-block:: edgeql

    select Movie {
        id,
        title,
        year,
        description,
    }
    filter .actors.full_name = 'Keanu Reeves'


----------


Select all actors that share the last name with other actors and
include the same-last-name actor list as well:

.. code-block:: edgeql

    select Person {
        id,
        full_name,
        same_last_name := (
            with
                P := detached Person
            select P {
                id,
                full_name,
            }
            filter
                # same last name
                P.last_name = Person.last_name
                and
                # not the same person
                P != Person
        ),
    }
    filter exists .same_last_name


----------


The same query can be refactored moving the ``with`` block to the
top-level:

.. code-block:: edgeql

    with
        # don't need detached at top-level
        P := Person
    select Person {
        id,
        full_name,
        same_last_name := (
            select P {
                id,
                full_name,
            }
            filter
                # same last name
                P.last_name = Person.last_name
                and
                # not the same person
                P != Person
        ),
    }
    filter exists .same_last_name


----------


Select user names and the number of reviews they have:

.. code-block:: edgeql

    select (
        User.name,
        count(User.<author[is Review])
    )


----------


For every user and movie combination, select whether the user has
reviewed the movie (beware, in practice this maybe a very large
result):

.. code-block:: edgeql

    select (
        User.name,
        Movie.title,
        Movie in User.<author[is Review].movie
    )


----------


Perform a set intersection of all actors with all directors:

.. code-block:: edgeql

    with
        # get the set of actors and set of directors
        Actor := Movie.actors,
        Director := Movie.director,
    # set intersection is done via the filter clause
    select Actor filter Actor in Director;


----------


To order a set of scalars first assign the set to a variable and use the
variable in the order by clause.

.. code-block:: edgeql

    select numbers := {3, 1, 2} order by numbers;

    # alternatively
    with numbers := {3, 1, 2}
    select numbers order by numbers;


----------


.. _ref_datamodel_objects_free:

Selecting free objects.

It is also possible to package data into a *free object*.
*Free objects* are meant to be transient and used either to more
efficiently store some intermediate results in a query or for
re-shaping the output. The advantage of using *free objects* over
:eql:type:`tuples <tuple>` is that it is easier to package data that
potentially contains empty sets as links or properties of the
*free object*. The underlying type of a *free object* is
``std::FreeObject``.

Consider the following query:

.. code-block:: edgeql

    with U := (select User filter .name like '%user%')
    select {
        matches := U {name},
        total := count(U),
        total_users := count(User),
    };

The ``matches`` are potentially ``{}``, yet the query will always
return a single *free object* with ``results``, ``total``, and
``total_users``. To achieve the same using a :eql:type:`named tuple
<tuple>`, the query would have to be modified like this:

.. code-block:: edgeql

    with U := (select User filter .name like '%user%')
    select (
        matches := array_agg(U {name}),
        total := count(U),
        total_users := count(User),
    );

Without the :eql:func:`array_agg` the above query would return ``{}``
instead of the named tuple if no ``matches`` are found.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`EdgeQL > Select <ref_eql_select>`
  * - :ref:`Reference > Commands > Select <ref_eql_statements_select>`
  * - `Tutorial > Basic Queries > Objects
      </tutorial/basic-queries/objects>`_
  * - `Tutorial > Basic Queries > Filters
      </tutorial/basic-queries/config>`_
  * - `Tutorial > Basic Queries > Aggregates
      </tutorial/basic-queries/aggregate-functions>`_
  * - `Tutorial > Nested Structures > Shapes
      </tutorial/nested-structures/shapes>`_
  * - `Tutorial > Nested Structures > Polymorphism
      </tutorial/nested-structures/polymorphism>`_
