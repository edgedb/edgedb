.. _ref_tutorial_queries:

3. EdgeQL
=========

After the database and schema are set up, we can add some actual data.
For this tutorial we will use the command-line REPL tool to do that,
so let's start it up:

.. code-block:: bash

    $ edgedb -I tutorial

Now, let's add "Blade Runner 2049" to the database. It's
possible to add movie, director and actor data all at once:

.. code-block:: edgeql-repl

    edgedb> INSERT Movie {
    .......     title := 'Blade Runner 2049',
    .......     year := 2017,
    .......     director := (
    .......         INSERT Person {
    .......             first_name := 'Denis',
    .......             last_name := 'Villeneuve',
    .......         }
    .......     ),
    .......     actors := {
    .......         (INSERT Person {
    .......             first_name := 'Harrison',
    .......             last_name := 'Ford',
    .......         }),
    .......         (INSERT Person {
    .......             first_name := 'Ryan',
    .......             last_name := 'Gosling',
    .......         }),
    .......         (INSERT Person {
    .......             first_name := 'Ana',
    .......             last_name := 'de Armas',
    .......         }),
    .......     }
    ....... };
    {default::Movie {id: 4d0c8ddc-54d4-11e9-8c54-7776f6130e05}}

.. note::

    The specific ``id`` values will be different from the one
    above. They are shown explicitly here so that the tutorial
    can refer to the Movie objects by their ``id``.

In principle, we could have first used individual :ref:`INSERT
<ref_eql_statements_insert>` statements to create all the people
records and then refer to them using :ref:`SELECT
<ref_eql_statements_select>` when creating a ``Movie``. To show how
existing data can be combined with new data let's add another movie
directed by Denis Villeneuve - "Dune":

.. code-block:: edgeql-repl

    edgedb> INSERT Movie {
    .......     title := 'Dune',
    .......     director := (
    .......         SELECT Person
    .......         FILTER
    .......             # the last name is sufficient
    .......             # to identify the right person
    .......             .last_name = 'Villeneuve'
    .......         # the LIMIT is needed to satisfy the single
    .......         # link requirement validation
    .......         LIMIT 1
    .......     )
    ....... };
    {default::Movie {id: 64d024dc-54d5-11e9-8c54-a3f59e1d995e}}

Let's write some basic queries:

.. code-block:: edgeql-repl

    edgedb> SELECT Movie;
    {
      default::Movie {id: 4d0c8ddc-54d4-11e9-8c54-7776f6130e05},
      default::Movie {id: 64d024dc-54d5-11e9-8c54-a3f59e1d995e}
    }

The above query simply returned all the ``Movie`` objects. Since we
didn't specify any details, the results only referred to the objects
by their ``id``. Let's add some more data to the result by describing
the :ref:`shape <ref_eql_expr_shapes>` of the data we want to fetch:

.. code-block:: edgeql-repl

    edgedb> SELECT Movie {
    .......     title,
    .......     year
    ....... };
    {
      default::Movie {title: 'Blade Runner 2049', year: 2017},
      default::Movie {title: 'Dune', year: {}},
    }

This time, the results contain ``title`` and ``year`` as requested in
the query shape. The year for the movie "Dune" is given as ``{}`` (the
empty set) since no ``year`` is set for that object.

Let's narrow down the ``Movie`` search to "blade runner" using
:eql:op:`ILIKE` (simple case-insensitive pattern matching). With the %
at the end anything after ``blade runner`` will match (Blade Runner,
Blade Runner 2049, BLaDE runnER the Video Game...).

.. code-block:: edgeql-repl

    edgedb> SELECT Movie {
    .......     title,
    .......     year
    ....... }
    ....... FILTER .title ILIKE 'blade runner%';
    {default::Movie {title: 'Blade Runner 2049', year: 2017}}

Let's get more details about the ``Movie``:

.. code-block:: edgeql-repl

    edgedb> SELECT Movie {
    .......     title,
    .......     year,
    .......     director: {
    .......         first_name,
    .......         last_name
    .......     },
    .......     actors: {
    .......         first_name,
    .......         last_name
    .......     }
    ....... }
    ....... FILTER .title ILIKE 'blade runner%';
    {
      default::Movie {
        title: 'Blade Runner 2049',
        year: 2017,
        director: default::Person {
          first_name: 'Denis',
          last_name: 'Villeneuve'
        },
        actors: {
          default::Person {
            first_name: 'Harrison',
            last_name: 'Ford'
          },
          default::Person {
            first_name: 'Ryan',
            last_name: 'Gosling'
          },
          default::Person {
            first_name: 'Ana',
            last_name: 'de Armas',
          },
        },
      },
    }

Instead of listing the ``actors`` let's just count how many people are
there in the ``actors`` by using a :ref:`computable
<ref_datamodel_computables>`:

.. code-block:: edgeql-repl

    edgedb> SELECT Movie {
    .......     title,
    .......     num_actors := count(Movie.actors)
    ....... };
    {
      default::Movie {title: 'Blade Runner 2049', num_actors: 3},
      default::Movie {title: 'Dune', num_actors: 0},
    }

Let's add some more information about "Dune". For example, we can add
some of the actors, like Jason Momoa, Zendaya and Oscar Isaac:

.. code-block:: edgeql-repl

    edgedb> INSERT Person {
    .......     first_name := 'Jason',
    .......     last_name := 'Momoa'
    ....... };
    default::Person {id: 618d4cd6-54db-11e9-8c54-67c38dbbba18}
    edgedb> INSERT Person {
    .......     first_name := 'Oscar',
    .......     last_name := 'Isaac'
    ....... };
    default::Person {id: 618d5a64-54db-11e9-8c54-9393cfcd9598}

Unfortunately, adding Zendaya is not possible with the current schema
since both ``first_name`` and ``last_name`` are required. So let's
migrate our schema to make ``last_name`` optional.

First, we'll update the ``dbschema/schema.esdl``:

.. code-block:: sdl

    module default {
        type Person {
            required property first_name -> str;
            property last_name -> str;
        }
        type Movie {
            required property title -> str;
            # the year of release
            property year -> int64;
            required link director -> Person;
            multi link actors -> Person;
        }
    };

Second, let's create a new migration to this new schema state:

.. code-block:: bash

    $ edgedb -I tutorial create-migration
    did you make property 'last_name' of object type
    'default::Person' optional? [y,n,l,c,b,s,q,?]
    y
    Created ./dbschema/migrations/00002.edgeql, id:
    m1k62y4xkmxbeer4rsrfysxhgibw7kjiedqcz6dxusces7ekx7g4ta

Third and final step in this sequence is to apply the migration:

.. code-block:: bash

    $ edgedb -I tutorial migrate
    Applied m1k62y4xkmxbeer4rsrfysxhgibw7kjiedqcz6dxusces7ekx7g4ta
    (00002.edgeql)

Now back in our REPL we can add Zendaya:

.. code-block:: edgeql-repl

    edgeql> INSERT Person {
    .......     first_name := 'Zendaya'
    ....... };
    {default::Person {id: 65fce84c-54dd-11e9-8c54-5f000ca496c9}}

And we can update "Dune":

.. code-block:: edgeql-repl

    edgeql> UPDATE Movie
    ....... FILTER Movie.title = 'Dune'
    ....... SET {
    .......     actors := (
    .......         SELECT Person
    .......         FILTER .first_name IN {
    .......             'Jason',
    .......             'Zendaya',
    .......             'Oscar'
    .......         }
    .......     )
    ....... };
    {default::Movie {id: 4d0c8ddc-54d4-11e9-8c54-7776f6130e05}}

For querying convenience let's update the schema so that a ``Person``
will also have a :ref:`computable <ref_datamodel_computables>`
``name`` that combines the ``first_name`` and ``last_name``
properties. The new ``dbschema/schema.esdl`` should look like
this:

.. code-block:: sdl

    module default {
        type Person {
            required property first_name -> str;
            property last_name -> str;
            property name :=
                .first_name ++ ' ' ++ .last_name
                IF EXISTS .last_name
                ELSE .first_name;
        }
        type Movie {
            required property title -> str;
            # the year of release
            property year -> int64;
            required link director -> Person;
            multi link actors -> Person;
        }
    };

Create the migration to the updated schema and then apply it:

.. code-block:: bash

    $ edgedb -I tutorial create-migration
    did you create property 'name' of object type
    'default::Person'? [y,n,l,c,b,s,q,?]
    y
    Created ./dbschema/migrations/00003.edgeql, id:
    m1gd3vxwz3oopur6ljgg7kzrin3jh65xhhjbj6de2xaou6i7owyhaq
    $ edgedb -I tutorial migrate
    Applied m1gd3vxwz3oopur6ljgg7kzrin3jh65xhhjbj6de2xaou6i7owyhaq
    (00003.edgeql)

Let's get back to EdgeDB REPL to try out the new schema with the
"Dune" ``Movie``:

.. code-block:: edgeql-repl

    edgeql> SELECT Movie {
    .......     title,
    .......     year,
    .......     director: { name },
    .......     actors: { name }
    ....... }
    ....... FILTER .title = 'Dune';
    {
        default::Movie {
            title: 'Dune',
            year: {},
            director: default::Person {name: 'Denis Villeneuve'},
            actors: {
                default::Person {name: 'Jason Momoa'},
                default::Person {name: 'Zendaya'},
                default::Person {name: 'Oscar Isaac'},
            }
        }
    }

Next, we can expose this data via a :ref:`GraphQL API
<ref_tutorial_graphql>`.
