.. _ref_tutorial_graphql:

4. GraphQL
==========

In order to set up GraphQL access to the database we need to update the
configuration:

.. code-block:: edgeql-repl

    tutorial> CONFIGURE SYSTEM INSERT Port {
    .........     protocol := "graphql+http",
    .........     database := "tutorial",
    .........     address := "127.0.0.1",
    .........     port := 8888,
    .........     user := "http",
    .........     concurrency := 4,
    ......... };
    CONFIGURE SYSTEM

.. note::

    If you are using Docker to run the EdgeDB server, replace the
    "address" value above with "0.0.0.0" to make sure the GraphQL port
    is proxied to the host.

This will expose :ref:`GraphQL API <ref_graphql_index>` on port 8888
(you can also specify any other port that you want). Pointing your
browser to ``http://127.0.0.1:8888/explore`` will bring up a
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
we could do that by creating a :ref:`view <ref_datamodel_views>`
instead. Let's add that view to the schema via EdgeDB :ref:`DDL
<ref_eql_ddl>`:

.. code-block:: edgeql-repl

    tutorial> CREATE VIEW PersonView := Person {
    .........     acted_in := Person.<actors[IS Movie]
    ......... };
    CREATE VIEW

Now, after reloading the GraphiQL page, we will be able to access the
``PersonView``:

.. code-block:: graphql

    {
        PersonView(order: {last_name: {dir: ASC}}) {
            name
            acted_in { title }
        }
    }

Which results in:

.. code-block:: json

    {
      "data": {
        "PersonView": [
          {
            "name": "Harrison Ford",
            "acted_in": [
              {
                "title": "Blade Runner 2049"
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
            "name": "Oscar Isaac",
            "acted_in": [
              {
                "title": "Dune"
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
            "name": "Denis Villeneuve",
            "acted_in": []
          },
          {
            "name": "Zendaya",
            "acted_in": [
              {
                "title": "Dune"
              }
            ]
          },
          {
            "name": "Ana de Armas",
            "acted_in": [
              {
                "title": "Blade Runner 2049"
              }
            ]
          }
        ]
      }
    }
