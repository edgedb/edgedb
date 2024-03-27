.. _ref_cli_edgedb_branch_rename:


====================
edgedb branch rename
====================

.. note::

    This CLI command requires CLI version 4.3.0 or later and EdgeDB version 5.0
    or later. If you are running an earlier version of EdgeDB, you will instead
    use the :ref:`ref_cli_edgedb_database` command suite to manage databases,
    which branches replaced in EdgeDB 5.0.

Rename a :ref:`branch <ref_datamodel_branches>`

.. cli:synopsis::

    edgedb branch rename [<options>] <old-name> <new-name>


Options
=======

The ``branch rename`` command runs in the EdgeDB instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<old-name>`
    The current name of the branch to rename.

:cli:synopsis:`<new-name>`
    The new name of the branch.

:cli:synopsis:`--force`
    Close any existing connections to the branch before renaming it.
