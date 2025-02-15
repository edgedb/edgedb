.. _ref_cli_edgedb_watch:


=========
gel watch
=========

Start a long-running process that watches for changes in schema files in your
project's ``dbschema`` directory and applies those changes to your database in
real time. Starting it is as simple as running this command:

.. cli:synopsis::

	gel watch

.. note::

    If a schema change cannot be applied, you will see an error in the
    :gelcmd:`watch` console. You will also receive the error when you
    try to run a query with any |Gel| client binding.

To learn about our recommended development migration workflow using
:gelcmd:`watch`, read our :ref:`intro to migrations <ref_intro_migrations>`.

.. note::

    If you want to apply a migration in the same manner as ``watch`` but
    without the long-running process, use :gelcmd:`migrate --dev-mode`. See
    :ref:`ref_cli_edgedb_migration_apply` for more details.

Demo
====

.. edb:youtube-embed:: _IUSPBm2xEA
