.. _ref_tutorial_graphql:

4. GraphQL
==========

In order to set up GraphQL access to the database we need to update the
schema:

.. code-block:: sdl

    using extension graphql;

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

After the schema is updated, we use the migration tools to create a
new migration and apply it:

.. code-block:: bash

    $ edgedb -I tutorial create-migration
    did you create extension 'graphql'? [y,n,l,c,b,s,q,?]
    y
    Created ./dbschema/migrations/00004.edgeql, id:
    m12exapirxxcs227zb2sruf7byvupbt6klkyl5ib6nyklyg3xo5s7a
    $ edgedb -I tutorial migrate
    Applied m12exapirxxcs227zb2sruf7byvupbt6klkyl5ib6nyklyg3xo5s7a
    (00004.edgeql)

This will expose :ref:`GraphQL API <ref_graphql_index>` on
``http://127.0.0.1:5656/db/edgedb/graphql``. Pointing your browser to
``http://127.0.0.1:5656/db/edgedb/graphql/explore`` will bring up a
`GraphiQL`_ interface to EdgeDB. This interface can be used to try out
queries and explore the GraphQL capabilities.

.. _`GraphiQL`: https://github.com/graphql/graphiql

Let's look at a basic ``Movie`` query:

.. code-block:: graphql

    {
        Movie {
            title
            year
        }
    }

Which results in:

.. code-block:: json

    {
      "data": {
        "Movie": [
          {
            "title": "Blade Runner 2049",
            "year": 2017
          },
          {
            "title": "Dune",
            "year": null
          }
        ]
      }
    }

It's possible to apply a :ref:`filter <ref_graphql_overview_filter>` to
get a specific ``Movie``:

.. code-block:: graphql

    {
        Movie(filter: {title: {eq: "Dune"}}) {
            title
            year
            director { name }
            actors { name }
        }
    }

Which results in:

.. code-block:: json

    {
      "data": {
        "Movie": [
          {
            "title": "Dune",
            "year": null,
            "director": {
              "name": "Denis Villeneuve"
            },
            "actors": [
              {
                "name": "Jason Momoa"
              },
              {
                "name": "Zendaya"
              },
              {
                "name": "Oscar Isaac"
              }
            ]
          }
        ]
      }
    }

If we wanted to provide some customized information, like which
``Movie`` a ``Person`` acted in without altering the existing types,
we could do that by creating an :ref:`alias <ref_datamodel_aliases>`
instead. Let's add that alias to the schema:

.. code-block:: sdl

    using extension graphql;

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
        alias PersonAlias := Person {
            acted_in := Person.<actors[IS Movie]
        };
    };

Then we create a new migration and apply it:

.. code-block:: bash

    $ edgedb -I tutorial create-migration
    did you create alias 'default::PersonAlias'? [y,n,l,c,b,s,q,?]
    y
    Created ./dbschema/migrations/00005.edgeql, id:
    m1td3ogdzqhztdaivw5bem4sjl3otxfx6fmqngzayymqfwtwbolroa
    $ edgedb -I tutorial migrate
    Applied m1td3ogdzqhztdaivw5bem4sjl3otxfx6fmqngzayymqfwtwbolroa
    (00005.edgeql)


Now, after reloading the GraphiQL page, we will be able to access the
``PersonAlias``:

.. code-block:: graphql

    {
        PersonAlias(order: {first_name: {dir: ASC}}) {
            name
            acted_in { title }
        }
    }

Which results in:

.. code-block:: json

    {
      "data": {
        "PersonAlias": [
          {
            "name": "Ana de Armas",
            "acted_in": [
              {
                "title": "Blade Runner 2049"
              }
            ]
          },
          {
            "name": "Denis Villeneuve",
            "acted_in": []
          },
          {
            "name": "Harrison Ford",
            "acted_in": [
              {
                "title": "Blade Runner 2049"
              }
            ]
          },
          {
            "name": "Jason Momoa",
            "acted_in": [
              {
                "title": "Dune"
              }
            ]
          },
          {
            "name": "Oscar Isaac",
            "acted_in": [
              {
                "title": "Dune"
              }
            ]
          },
          {
            "name": "Ryan Gosling",
            "acted_in": [
              {
                "title": "Blade Runner 2049"
              }
            ]
          },
          {
            "name": "Zendaya",
            "acted_in": [
              {
                "title": "Dune"
              }
            ]
          }
        ]
      }
    }
