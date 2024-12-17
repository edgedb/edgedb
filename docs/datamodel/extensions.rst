.. _ref_datamodel_extensions:

==========
Extensions
==========

Extensions are the way EdgeDB adds more functionality. In principle,
extensions could add new types, scalars, functions, etc., but, more
importantly, they can add new ways of interacting with the database.

Built-in extensions
-------------------

There are a few built-in extensions available:

- ``edgeql_http``: enables :ref:`EdgeQL over HTTP <ref_edgeql_http>`
- ``graphql``: enables :ref:`GraphQL <ref_graphql_index>`
- ``auth``: enables :ref:`EdgeDB Auth <ref_guide_auth>`

.. _ref_datamodel_using_extension:

To enable these extensions, add a ``using`` statement at the top level of your schema:

.. code:: sdl

    using extension auth;
    


Standalone extensions
---------------------

Additionally, standalone extension packages can be installed via the CLI:

.. code:: bash

    $ edgedb extension list -I my_instance
    ┌─────────┬─────────┐
    │ Name    │ Version │
    └─────────┴─────────┘

    $ edgedb extension list-available -I my_instance
    ┌─────────┬───────────────┐
    │ Name    │ Version       │
    │ postgis │ 3.4.3+6b82d77 │
    └─────────┴───────────────┘

    $ edgedb extension install -I my_instance -E postgis
    Found extension package: postgis version 3.4.3+6b82d77
    00:00:03 [====================] 22.49 MiB/22.49 MiB
    Extension 'postgis' installed successfully.

    $ edgedb extension list -I my_instance
    ┌─────────┬───────────────┐
    │ Name    │ Version       │
    │ postgis │ 3.4.3+6b82d77 │
    └─────────┴───────────────┘

After installing extensions, make sure to restart your instance:

.. code:: bash

    $ edgedb instance restart -I my_instance

Standalone extensions can now be declared in the schema, same as :ref:`built-in
extensions <ref_datamodel_using_extension>`.

To restore a dump that uses a standalone extension, that extension must be installed
before the restore process.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Extensions <ref_eql_sdl_extensions>`
  * - :eql:stmt:`DDL > CREATE EXTENSION <create extension>`
  * - :eql:stmt:`DDL > DROP EXTENSION <drop extension>`
