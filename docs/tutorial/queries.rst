.. _ref_tutorial_queries:

3. EdgeQL
=========

After the database and schema are set up, we can add some actual data.
For example, let's add "Blade Runner 2049" to the database. It's possible to
add movie, director and cast data all at once:

.. code-block:: edgeql-repl

    tutorial> INSERT Movie {
    .........     title := 'Blade Runner 2049',
    .........     year := 2017,
    .........     director := (
    .........         INSERT Person {
    .........             first_name := 'Denis',
    .........             last_name := 'Villeneuve',
    .........         }
    .........     ),
    .........     cast := {
    .........         (INSERT Person {
    .........             first_name := 'Harrison',
    .........             last_name := 'Ford',
    .........         }),
    .........         (INSERT Person {
    .........             first_name := 'Ryan',
    .........             last_name := 'Gosling',
    .........         }),
    .........         (INSERT Person {
    .........             first_name := 'Ana',
    .........             last_name := 'de Armas',
    .........         }),
    .........     }
    ......... };
    {Object { id: <uuid>'4d0c8ddc-54d4-11e9-8c54-7776f6130e05' }}

.. note::

    The specific ``id`` values will be different. They are shown
    explicitly here so that the tutorial can refer to the Movie
    objects by their ``id``.

In principle, we could have first used individual :ref:`INSERT
<ref_eql_statements_insert>` statements to create all the people
records and then refer to them using :ref:`SELECT
<ref_eql_statements_select>` when creating a ``Movie``. To show how
existing data can be combined with new data let's add another movie
directed by Denis Villeneuve - "Dune". Since the movie is still not
finished, we can omit the ``year`` and ``cast``, populating only the
``title`` and ``director``:

.. code-block:: edgeql-repl

    tutorial> INSERT Movie {
    .........     title := 'Dune',
    .........     director := (
    .........         SELECT Person
    .........         FILTER
    .........             # the last name is sufficient
    .........             # to identify the right person
    .........             .last_name = 'Villeneuve'
    .........         # the LIMIT is needed to satisfy the single
    .........         # link requirement validation
    .........         LIMIT 1
    .........     )
    ......... };
    {Object { id: <uuid>'64d024dc-54d5-11e9-8c54-a3f59e1d995e' }}

Let's write some basic queries:

.. code-block:: edgeql-repl

    tutorial> SELECT Movie;
    {
        Object { id: <uuid>'4d0c8ddc-54d4-11e9-8c54-7776f6130e05' },
        Object { id: <uuid>'64d024dc-54d5-11e9-8c54-a3f59e1d995e' }
    }

The above query simply returned all the ``Movie`` objects. Since we
didn't specify any details, the results only referred to the objects
by their ``id``. Let's add some more data to the result by describing
the :ref:`shape <ref_eql_expr_shapes>` of the data we want to fetch:

.. code-block:: edgeql-repl

    tutorial> SELECT Movie {
    .........     title,
    .........     year
    ......... };
    {
        Object { title: 'Blade Runner 2049', year: 2017 },
        Object { title: 'Dune', year: {} }
    }

This time, the results contain ``title`` and ``year`` as requested in
the query shape. The year for the movie "Dune" is given as ``{}`` (the
empty set) since no ``year`` is set for that object.

Let's narrow down the ``Movie`` search to "blade runner" using
:eql:op:`ILIKE` (simple case-insensitive pattern matching):

.. code-block:: edgeql-repl

    tutorial> SELECT Movie {
    .........     title,
    .........     year
    ......... }
    ......... FILTER .title ILIKE 'blade runner%';
    {
        Object { title: 'Blade Runner 2049', year: 2017 },
    }

Let's get more details about the ``Movie``:

.. code-block:: edgeql-repl

    tutorial> SELECT Movie {
    .........     title,
    .........     year,
    .........     director: {
    .........         first_name,
    .........         last_name
    .........     },
    .........     cast: {
    .........         first_name,
    .........         last_name
    .........     }
    ......... }
    ......... FILTER .title ILIKE 'blade runner%';
    {
        Object {
            title: 'Blade Runner 2049',
            year: 2017,
            director: Object {
                first_name: 'Denis',
                last_name: 'Villeneuve'
            },
            cast: {
                Object { first_name: 'Harrison', last_name: 'Ford' },
                Object { first_name: 'Ryan', last_name: 'Gosling' },
                Object { first_name: 'Ana', last_name: 'de Armas' }
            }
        }
    }

Instead of listing the ``cast`` let's just count how many people are
there in the ``cast`` by using a :ref:`computable
<ref_datamodel_computables>`:

.. code-block:: edgeql-repl

    tutorial> SELECT Movie {
    .........     title,
    .........     num_actors := count(Movie.cast)
    ......... };
    {
        Object { title: 'Blade Runner 2049', num_actors: 3 },
        Object { title: 'Dune', num_actors: 0 }
    }

Let's add some more information about "Dune". For example, we can add
some of the cast members, like Jason Momoa, Zendaya and Oscar Isaac:

.. code-block:: edgeql-repl

    tutorial> INSERT Person {
    .........     first_name := 'Jason',
    .........     last_name := 'Momoa'
    ......... };
    Object { id: <uuid>'618d4cd6-54db-11e9-8c54-67c38dbbba18' },
    tutorial> INSERT Person {
    .........     first_name := 'Oscar',
    .........     last_name := 'Isaac'
    ......... };
    Object { id: <uuid>'618d5a64-54db-11e9-8c54-9393cfcd9598' },

Unfortunately, adding Zendaya is not possible with the current schema
since both ``first_name`` and ``last_name`` are required. So let's
:ref:`alter <ref_eql_ddl_object_types_alter>` our schema to make
``first_name`` optional (we'll use :ref:`DDL <ref_eql_ddl>` here
for brevity):

.. code-block:: edgeql-repl

    tutorial> ALTER TYPE Person {
    .........     ALTER PROPERTY first_name {
    .........         DROP REQUIRED;
    .........     }
    ......... };
    ALTER

Now we can add Zendaya:

.. code-block:: edgeql-repl

    tutorial> INSERT Person {
    .........     last_name := 'Zendaya'
    ......... };
    {Object { id: <uuid>'65fce84c-54dd-11e9-8c54-5f000ca496c9' }}

And we can update "Dune":

.. code-block:: edgeql-repl

    tutorial> UPDATE Movie
    ......... FILTER Movie.title = 'Dune'
    ......... SET {
    .........     cast := (
    .........         SELECT Person
    .........         FILTER .last_name IN {
    .........             'Momoa',
    .........             'Zendaya',
    .........             'Isaac'
    .........         }
    .........     )
    ......... };
    {Object { id: <uuid>'4d0c8ddc-54d4-11e9-8c54-7776f6130e05' }}

Finally, let's update the schema so that a ``Person`` will also have a
:ref:`computable <ref_datamodel_computables>` ``name`` that combines
the ``first_name`` and ``last_name`` properties. This time we will use
:ref:`SDL <ref_eql_sdl>` to make the final state of the schema clear:

.. code-block:: edgeql-repl

    tutorial> START TRANSACTION;
    START TRANSACTION
    tutorial> CREATE MIGRATION movies TO {
    .........     module default {
    .........         type Movie {
    .........             required property title -> str;
    .........             # the year of release
    .........             property year -> int64;
    .........             required link director -> Person;
    .........             multi link cast -> Person;
    .........         }
    .........         type Person {
    .........             property first_name -> str;
    .........             required property last_name -> str;
    .........             property name :=
    .........                 .first_name ++ ' ' ++ .last_name
    .........                 IF EXISTS .first_name
    .........                 ELSE .last_name;
    .........         }
    .........     }
    ......... };
    CREATE MIGRATION
    tutorial> COMMIT MIGRATION movies;
    COMMIT MIGRATION
    tutorial> COMMIT;
    COMMIT TRANSACTION

Let's try out the new schema with the "Dune" ``Movie``:

.. code-block:: edgeql-repl

    tutorial> SELECT Movie {
    .........     title,
    .........     year,
    .........     director: { name },
    .........     cast: { name }
    ......... }
    ......... FILTER .title = 'Dune';
    {
        Object {
            title: 'Dune',
            year: {},
            director: Object { name: 'Denis Villeneuve' },
            cast: {
                Object { name: 'Jason Momoa' },
                Object { name: 'Zendaya' },
                Object { name: 'Oscar Isaac' }
            }
        }
    }

Next, we can expose this data via a :ref:`GraphQL API <ref_tutorial_graphql>`.
