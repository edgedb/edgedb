.. _ref_cli_edgedb_branch_merge:


===================
edgedb branch merge
===================

.. note::

    This CLI command requires CLI version 4.0 or later and EdgeDB version 5.0
    or later. If you are running an earlier version of EdgeDB, you will instead
    use the :ref:`ref_cli_edgedb_database` command suite to manage databases,
    which branches replaced in EdgeDB 5.0.

Merge a :ref:`branch <ref_datamodel_branches>` into the current branch.

.. cli:synopsis::

    edgedb branch merge [<options>] <name>


Description
===========

Merges the target branch with the current branch using a fast-forward strategy,
applying any new migrations from the target branch on the current branch.

.. note::

    This is a fast-forward merge, so no conflict resolution will be applied to
    the new migrations. If you want to merge but may have conflicts, you should
    first use :ref:`ref_cli_edgedb_branch_rebase` from the target branch before
    merging.

.. note::

    When merging, the data of the current branch is preserved. This means that
    if you switch to a branch ``main`` and run ``edgedb branch merge feature``,
    you will end up with a branch with the schema from ``main`` and any
    new migrations from ``feature`` and the data from ``main``.


Options
=======

The ``branch merge`` command runs in the EdgeDB instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<name>`
    The name of the branch to merge into the current branch.
:cli:synopsis:`--no-apply`
    Skip applying migrations generated from the merge
