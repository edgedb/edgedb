.. _ref_graphql_index:


=======
GraphQL
=======

EdgeBD supports `GraphQL queries`__ natively out of the box. Not
everything that can be expressed in EdgeQL can easily be queried using
GraphQL, but generally for complex queries it is useful to set up
Views and use GraphQL to query them.

EdgeDB exposes the Types and Views from the ``default`` module for
GraphQL querying. To expose something from another module a View in
the default module can be used.

.. toctree::
    :maxdepth: 3

    graphql
    introspection


.. __: http://graphql.org/docs/queries/
