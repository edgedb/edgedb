.. _ref_cli_edgedb_watch:


============
edgedb watch
============

.. TODO: 3.0 release
.. Remove this note

.. note::

    This CLI feature is compatible with EdgeDB server 3.0 and above.

Start a long-running process that watches for changes in schema files in your
project's ``dbschema`` directory and applies those changes to your database in
real time.

.. cli:synopsis::

	edgedb watch

.. note::

    If a schema change cannot be applied, you will see an error in the ``edgedb
    watch`` console. You will also receive the error when you try to run a
    query with any EdgeDB client binding.

To learn about our recommended development migration workflow using ``edgedb
watch``, read our :ref:`intro to migrations <ref_intro_migrations>`.
