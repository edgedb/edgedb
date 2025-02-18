.. _ref_cli_gel_database:


============
gel database
============

The :gelcmd:`database` group of commands contains various database
manipulation tools.

.. note::

    |EdgeDB| 5.0 introduced :ref:`branches <ref_datamodel_branches>` to
    replace databases. These commands work on instances running versions
    prior to |EdgeDB| 5.0. If you are running a newer version of
    Gel, you will instead use the :ref:`ref_cli_gel_branch` suite of
    commands.

.. toctree::
    :maxdepth: 3
    :hidden:

    gel_database_create
    gel_database_drop
    gel_database_wipe

.. list-table::
    :class: funcoptable

    * - :ref:`ref_cli_gel_database_create`
      - Create a new database
    * - :ref:`ref_cli_gel_database_drop`
      - Drop a database
    * - :ref:`ref_cli_gel_database_wipe`
      - Destroy the contents of a database
