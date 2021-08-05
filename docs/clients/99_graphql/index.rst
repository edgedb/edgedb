.. _ref_graphql_index:

=================
GraphQL over HTTP
=================

EdgeDB supports `GraphQL queries`__ natively out of the box. Not
everything that can be expressed in EdgeQL can easily be queried using
GraphQL, but generally setting up :ref:`aliases <ref_datamodel_aliases>`
for complex expressions makes it possible to then use use GraphQL to
query them.

EdgeDB exposes the Types and :ref:`ref_datamodel_aliases` for GraphQL
querying. Types and expression aliases from the ``default`` module are
exposed using their short names, whereas items from another module use
the module name as a prefix.

In order to set up GraphQL access to the database add the following to
the schema:

.. code-block:: sdl

    using extension graphql;

Then create a new migration and apply it using
:ref:`ref_cli_edgedb_migration_create` and
:ref:`ref_cli_edgedb_migrate`, respectively.

``http://127.0.0.1:<instance-port>/db/<database-name>/graphql`` will
expose GraphQL API. Check the credentials file for your instance at
``<edgedb_config_dir>/credentials`` to find out which port the instance is
using. Run ``edgedb info`` to see the path to ``<edgedb_config_dir>`` on your
machine.

``http://127.0.0.1:<instance-port>/db/<database-name>/graphql/explore``
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
