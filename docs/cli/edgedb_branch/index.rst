.. _ref_cli_edgedb_branch:


=============
edgedb branch
=============

.. note::

    These CLI commands require CLI version 4.3.0 or later and EdgeDB version
    5.0 or later. If you are running an earlier version of EdgeDB, you will
    instead use the :ref:`ref_cli_edgedb_database` command suite to manage
    databases, which branches replaced in EdgeDB 5.0.

The ``edgedb branch`` group of commands contains various branch management
tools.

.. toctree::
    :maxdepth: 3
    :hidden:

    edgedb_branch_create
    edgedb_branch_drop
    edgedb_branch_list
    edgedb_branch_merge
    edgedb_branch_rebase
    edgedb_branch_rename
    edgedb_branch_switch
    edgedb_branch_wipe

.. list-table::
    :class: funcoptable

    * - :ref:`ref_cli_edgedb_branch_create`
      - Create a new branch
    * - :ref:`ref_cli_edgedb_branch_drop`
      - Drop a branch
    * - :ref:`ref_cli_edgedb_branch_list`
      - List all branches
    * - :ref:`ref_cli_edgedb_branch_merge`
      - Merge a branch into the current branch
    * - :ref:`ref_cli_edgedb_branch_rebase`
      - Create a branch based on a target branch
    * - :ref:`ref_cli_edgedb_branch_rename`
      - Rename a branch
    * - :ref:`ref_cli_edgedb_branch_switch`
      - Change the currently active branch
    * - :ref:`ref_cli_edgedb_branch_wipe`
      - Destroy the contents of a branch
