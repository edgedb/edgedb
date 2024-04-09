.. _ref_cli_edgedb_migration:


================
edgedb migration
================

EdgeDB provides schema migration tools as server-side tools. This means that,
from the point of view of the application, migrations are language- and
platform-agnostic and don't require additional libraries.

Using the migration tools is the recommended way to make schema changes.

.. toctree::
    :maxdepth: 3
    :hidden:

    edgedb_migration_apply
    edgedb_migration_create
    edgedb_migration_edit
    edgedb_migration_extract
    edgedb_migration_log
    edgedb_migration_status
    edgedb_migration_upgrade_check

Setup
=====

First of all, the migration tools need a place to store the schema and
migration information. By default they will look in the ``dbschema``
directory, but it's also possible to specify any other location by
using the :cli:synopsis:`schema-dir` option.

Inside this directory, you will find an ``.esdl`` file with an :ref:`SDL
<ref_eql_sdl>` schema description. You may split your schema across multiple
``.esdl`` files. The migration tools will read all of them and treat them as a
single SDL document.

.. list-table::
    :class: funcoptable

    * - :ref:`ref_cli_edgedb_migration_apply`
      - Bring current branch (or database pre-v5) to the latest or a specified revision
    * - :ref:`ref_cli_edgedb_migration_create`
      - Create a migration script
    * - :ref:`ref_cli_edgedb_migration_edit`
      - Edit migration file
    * - :ref:`ref_cli_edgedb_migration_extract`
      - Extract migration history and write it to ``/migrations``.
    * - :ref:`ref_cli_edgedb_migration_log`
      - Show all migration versions
    * - :ref:`ref_cli_edgedb_migration_status`
      - Show current migration state
    * - :ref:`ref_cli_edgedb_migration_upgrade_check`
      - Checks your schema against a different EdgeDB version.
