.. _ref_cheatsheet_select:

Select
======

.. note::

    The types used in these queries are defined :ref:`here
    <ref_cheatsheet_types>`.

Select a Movie with associated actors and reviews with their authors:

.. code-block:: edgeql

    SELECT Movie {
        id,
        title,
        year,
        description,

        actors: {
            id,
            full_name,
        },

        reviews := .<movie[IS Review] {
            id,
            body,
            rating,
            author: {
                id,
                name,
            }
        },
    }
    FILTER .id = <uuid>'09c34154-4148-11ea-9c68-5375ca908326'

Select movies with Keanu Reeves:

.. code-block:: edgeql

    SELECT Movie {
        id,
        title,
        year,
        description,
    }
    FILTER .cast.full_name = 'Keanu Reeves'

Select all actors that share the last name with other actors and
include the same-last-name actor list as well:

.. code-block:: edgeql

    SELECT Person {
        id,
        full_name,
        same_last_name := (
            WITH
                P := DETACHED Person
            SELECT P {
                id,
                full_name,
            }
            FILTER
                # same last name
                P.last_name = Person.last_name
                AND
                # not the same person
                P != Person
        ),
    }
    FILTER EXISTS .same_last_name

The same query can be refactored moving the ``WITH`` block tot he top-level:

.. code-block:: edgeql

    WITH
        # don't need DETACHED at top-level
        P := Person
    SELECT Person {
        id,
        full_name,
        same_last_name := (
            SELECT P {
                id,
                full_name,
            }
            FILTER
                # same last name
                P.last_name = Person.last_name
                AND
                # not the same person
                P != Person
        ),
    }
    FILTER EXISTS .same_last_name

Select user names and the number of reviews they have:

.. code-block:: edgeql

    SELECT (
        User.name,
        count(User.<author[IS Review])
    )

For every user and movie combination, select whether the user has
reviewed the movie (beware, in practice this maybe a very large
result):

.. code-block:: edgeql

    SELECT (
        User.name,
        Movie.title,
        Movie IN User.<author[IS Review].movie
    )
