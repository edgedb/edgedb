.. _ref_cli_gel_migration:


=============
gel migration
=============

|Gel| provides schema migration tools as server-side tools. This means that,
from the point of view of the application, migrations are language- and
platform-agnostic and don't require additional libraries.

Using the migration tools is the recommended way to make schema changes.

.. toctree::
    :maxdepth: 3
    :hidden:

    gel_migration_apply
    gel_migration_create
    gel_migration_edit
    gel_migration_extract
    gel_migration_log
    gel_migration_status
    gel_migration_upgrade_check

Setup
=====

First of all, the migration tools need a place to store the schema and
migration information. By default they will look in the ``dbschema``
directory, but it's also possible to specify any other location by
using the :cli:synopsis:`schema-dir` option.

Inside this directory, you will find an |.gel| file with an :ref:`SDL
<ref_eql_sdl>` schema description. You may split your schema across multiple
|.gel| files. The migration tools will read all of them and treat them as a
single SDL document.

.. list-table::
    :class: funcoptable

    * - :ref:`ref_cli_gel_migration_apply`
      - Bring current |branch| to the latest or a specified revision
    * - :ref:`ref_cli_gel_migration_create`
      - Create a migration script
    * - :ref:`ref_cli_gel_migration_edit`
      - Edit migration file
    * - :ref:`ref_cli_gel_migration_extract`
      - Extract migration history and write it to ``/migrations``.
    * - :ref:`ref_cli_gel_migration_log`
      - Show all migration versions
    * - :ref:`ref_cli_gel_migration_status`
      - Show current migration state
    * - :ref:`ref_cli_gel_migration_upgrade_check`
      - Checks your schema against a different |Gel| version.
