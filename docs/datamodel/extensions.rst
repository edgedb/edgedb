.. _ref_datamodel_extensions:

==========
Extensions
==========

.. index:: using extension

Extensions are the way |Gel| can be extended with more functionality.
They can add new types, scalars, functions, etc., but, more
importantly, they can add new ways of interacting with the database.


Built-in extensions
===================

.. index:: edgeql_http, graphql, auth, ai, pg_trgm, pg_unaccent, pgcrypto,
           pgvector

There are a few built-in extensions available:

- ``edgeql_http``: enables :ref:`EdgeQL over HTTP <ref_edgeql_http>`,
- ``graphql``: enables :ref:`GraphQL <ref_graphql_index>`,
- ``auth``: enables :ref:`Gel Auth <ref_guide_auth>`,
- ``ai``: enables :ref:`ext::ai module <ref_ai_reference>`,

- ``pg_trgm``: enables ``ext::pg_trgm``, which re-exports
  `pgtrgm <https://www.postgresql.org/docs/current/pgtrgm.html>`__,

- ``pg_unaccent``: enables ``ext::pg_unaccent``, which re-exports
  `unaccent <https://www.postgresql.org/docs/current/unaccent.html>`__,

- ``pgcrypto``: enables ``ext::pgcrypto``, which re-exports
  `pgcrypto <https://www.postgresql.org/docs/current/pgcrypto.html>`__,

- ``pgvector``: enables ``ext::pgvector``, which re-exports
  `pgvector <https://github.com/pgvector/pgvector/>`__,

.. _ref_datamodel_using_extension:

To enable these extensions, add a ``using`` statement at the top level of
your schema:

.. code-block:: sdl

   using extension auth;
   # or / and
   using extension ai;


Standalone extensions
=====================

.. index:: postgis

Additionally, standalone extension packages can be installed via the CLI,
with ``postgis`` being a notable example.

List installed extensions:

.. code-block:: bash

  $ gel extension list
  ┌─────────┬─────────┐
  │ Name    │ Version │
  └─────────┴─────────┘

List available extensions:

.. code-block:: bash

  $ gel extension list-available
  ┌─────────┬───────────────┐
  │ Name    │ Version       │
  │ postgis │ 3.4.3+6b82d77 │
  └─────────┴───────────────┘

Install the ``postgis`` extension:

.. code-block:: bash

  $ gel extension install -E postgis
  Found extension package: postgis version 3.4.3+6b82d77
  00:00:03 [====================] 22.49 MiB/22.49 MiB
  Extension 'postgis' installed successfully.

Check that extension is installed:

.. code-block:: bash

  $ gel extension list
  ┌─────────┬───────────────┐
  │ Name    │ Version       │
  │ postgis │ 3.4.3+6b82d77 │
  └─────────┴───────────────┘

After installing extensions, make sure to restart your instance:

.. code-block:: bash

  $ gel instance restart

Standalone extensions can now be declared in the schema, same as
built-in extensions:

.. code-block:: sdl

  using extension postgis;

.. note::
   To restore a dump that uses a standalone extension, that extension must
   be installed before the restore process.

.. _ref_eql_sdl_extensions:

Using extensions
================

Syntax
------

.. sdl:synopsis::

  using extension <ExtensionName> ";"


Extension declaration must be outside any :ref:`module block
<ref_eql_sdl_modules>` since extensions affect the entire database and
not a specific module.



.. _ref_eql_ddl_extensions:

DDL commands
============

This section describes the low-level DDL commands for creating and
dropping extensions. You typically don't need to use these commands directly,
but knowing about them is useful for reviewing migrations.


create extension
----------------

:eql-statement:

Enable a particular extension for the current schema.

.. eql:synopsis::

  create extension <ExtensionName> ";"


Description
^^^^^^^^^^^

The command ``create extension`` enables the specified extension for
the current :versionreplace:`database;5.0:branch`.

Examples
^^^^^^^^

Enable :ref:`GraphQL <ref_graphql_index>` extension for the current
schema:

.. code-block:: edgeql

  create extension graphql;

Enable :ref:`EdgeQL over HTTP <ref_edgeql_http>` extension for the
current :versionreplace:`database;5.0:branch`:

.. code-block:: edgeql

  create extension edgeql_http;


drop extension
--------------

:eql-statement:

Disable an extension.

.. eql:synopsis::

  drop extension <ExtensionName> ";"


The command ``drop extension`` disables a currently active extension for
the current |branch|.

Examples
^^^^^^^^

Disable :ref:`GraphQL <ref_graphql_index>` extension for the current
schema:

.. code-block:: edgeql

  drop extension graphql;

Disable :ref:`EdgeQL over HTTP <ref_edgeql_http>` extension for the
current :versionreplace:`database;5.0:branch`:

.. code-block:: edgeql

  drop extension edgeql_http;
