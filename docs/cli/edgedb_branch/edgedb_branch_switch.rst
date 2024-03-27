.. _ref_cli_edgedb_branch_switch:


====================
edgedb branch switch
====================

.. note::

    This CLI command requires CLI version 4.3.0 or later and EdgeDB version 5.0
    or later. If you are running an earlier version of EdgeDB, you will instead
    use the :ref:`ref_cli_edgedb_database` command suite to manage databases,
    which branches replaced in EdgeDB 5.0.

Change the currently active :ref:`branch <ref_datamodel_branches>`

.. cli:synopsis::

    edgedb branch switch [<options>] <name>


Options
=======

The ``branch switch`` command runs in the EdgeDB instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<name>`
    The name of the new branch.

:cli:synopsis:`-c, --create`
    Create the branch if it doesn't exist.

:cli:synopsis:`-e, --empty`
    If creating a new branch: create the branch with no schema or data.

:cli:synopsis:`--from <FROM>`
    If creating a new branch: the optional base branch to create the new branch
    from.

:cli:synopsis:`--copy-data`
    If creating a new branch: copy data from the base branch to the new branch.
