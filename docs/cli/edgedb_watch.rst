.. _ref_cli_edgedb_watch:


============
edgedb watch
============

.. note::

    This CLI feature is compatible with EdgeDB server 3.0 and above.

Start a long-running process that watches for changes in schema files in your
project's ``dbschema`` directory and applies those changes to your database in
real time. Starting it is as simple as running this command:

.. cli:synopsis::

	edgedb watch

.. note::

    If a schema change cannot be applied, you will see an error in the ``edgedb
    watch`` console. You will also receive the error when you try to run a
    query with any EdgeDB client binding.

To learn about our recommended development migration workflow using ``edgedb
watch``, read our :ref:`intro to migrations <ref_intro_migrations>`.

.. note::

    If you want to apply a migration in the same manner as ``watch`` but
    without the long-running process, use ``edgedb migrate --dev-mode``. See
    :ref:`ref_cli_edgedb_migration_apply` for more details.

Demo
====

.. edb:youtube-embed:: _IUSPBm2xEA
