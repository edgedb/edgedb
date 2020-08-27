.. _ref_graphql_index:

=================
GraphQL over HTTP
=================

EdgeDB supports `GraphQL queries`__ natively out of the box. Not
everything that can be expressed in EdgeQL can easily be queried using
GraphQL, but generally for complex queries it is useful to set up
expression aliases and use GraphQL to query them.

EdgeDB exposes the Types and :ref:`ref_datamodel_aliases` for GraphQL
querying. Types and expression aliases from the ``default`` module are
exposed using their short names, whereas items from another module use
the module name as a prefix.

Here's an example of configuration that will set up GraphQL access to
the database:

.. code-block:: edgeql-repl

    tutorial> CONFIGURE SYSTEM INSERT Port {
    .........     protocol := "graphql+http",
    .........     database := "your_database_name",
    .........     address := "127.0.0.1",
    .........     port := 8888,
    .........     user := "http",
    .........     concurrency := 4,
    ......... };
    CONFIGURE SYSTEM

This will expose GraphQL API for the ``"your_database_name"`` on port 8888
(or any other port that was specified).

Pointing your browser to ``http://127.0.0.1:8888/explore``
will bring up a `GraphiQL`_ interface to EdgeDB. This interface can be
used to try out queries and explore the GraphQL capabilities.

.. toctree::
    :maxdepth: 2
    :hidden:

    graphql
    mutations
    introspection
    protocol
    limitations


.. __: http://graphql.org/docs/queries/

.. _`GraphiQL`: https://github.com/graphql/graphiql
