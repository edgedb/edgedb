.. _ref_datamodel_extensions:

==========
Extensions
==========

Extensions are the way EdgeDB adds more functionality. In principle,
extensions could add new types, scalars, functions, etc., but, more
importantly, they can add new ways of interacting with the database.

At the moment there are only two extensions available:

- ``edgeql_http``: enables :ref:`EdgeQL over HTTP <ref_edgeql_http>`
- ``graphql``: enables :ref:`GraphQL <ref_graphql_index>`


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Extensions <ref_eql_sdl_extensions>`
  * - :eql:stmt:`DDL > CREATE EXTENSION <create extension>`
  * - :eql:stmt:`DDL > DROP EXTENSION <drop extension>`
