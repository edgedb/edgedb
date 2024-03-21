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

Creates a new branch that is based on the target branch, but also contains any new migrations on the
current branch.


Options
=======

The ``branch merge`` command runs in the EdgeDB instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<name>`
    The name of the branch to merge into the current branch.
:cli:synopsis:`--no-apply`
    Skip applying migrations generated from the merge
