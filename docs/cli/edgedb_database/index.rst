.. _ref_cli_edgedb_database:


===============
edgedb database
===============

The ``edgedb database`` group of commands contains various database
manipulation tools.

.. note::

    EdgeDB 5.0 introduced :ref:`branches <ref_datamodel_branches>` to
    replace databases. These commands work on instances running versions
    prior to EdgeDB 5.0. If you are running a newer version of
    EdgeDB, you will instead use the :ref:`ref_cli_edgedb_branch` suite of
    commands.

.. toctree::
    :maxdepth: 3
    :hidden:

    edgedb_database_create
    edgedb_database_drop
    edgedb_database_wipe

.. list-table::
    :class: funcoptable

    * - :ref:`ref_cli_edgedb_database_create`
      - Create a new database
    * - :ref:`ref_cli_edgedb_database_drop`
      - Drop a database
    * - :ref:`ref_cli_edgedb_database_wipe`
      - Destroy the contents of a database
