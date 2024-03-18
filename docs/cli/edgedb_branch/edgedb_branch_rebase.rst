.. _ref_cli_edgedb_branch_rebase:


====================
edgedb branch rebase
====================

.. note::

    This CLI command requires CLI version 4.0 or later and EdgeDB version 5.0
    or later. If you are running an earlier version of EdgeDB, you will instead
    use the :ref:`ref_cli_edgedb_database` command suite to manage databases,
    which branches replaced in EdgeDB 5.0.

Create a :ref:`branch <ref_datamodel_branches>` based on the target branch but
including new migrations on the current branch.

.. cli:synopsis::

    edgedb branch rebase [<options>] <name>


Description
===========

Creates a new branch that is based on the target branch, but also contains any new migrations on the
current branch.


Options
=======

The ``branch rebase`` command runs in the EdgeDB instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<name>`
    The name of the target branch.

:cli:synopsis:`-c, --create`
    Create the branch if it doesn't exist.

:cli:synopsis:`-e, --empty`
    If creating a new branch: create the branch with no schema or data.

:cli:synopsis:`--from <FROM>`
    If creating a new branch: the optional base branch to create the new branch
    from.

:cli:synopsis:`--copy-data`
    If creating a new branch: copy data from the base branch to the new branch.
