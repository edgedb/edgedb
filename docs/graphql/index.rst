.. _ref_graphql_index:


=======
GraphQL
=======

EdgeBD supports `GraphQL queries`__ natively out of the box. Not
everything that can be expressed in EdgeQL can easily be queried using
GraphQL, but generally for complex queries it is useful to set up
Views and use GraphQL to query them.

EdgeDB exposes the Types and :ref:`ref_datamodel_views` for GraphQL
querying. Types and Views from thee ``default`` module are exposed
using their short names, whereas items from another module use the
module name as a prefix.

.. toctree::
    :maxdepth: 3

    graphql
    introspection


.. __: http://graphql.org/docs/queries/
