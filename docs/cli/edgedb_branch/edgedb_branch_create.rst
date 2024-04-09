.. _ref_cli_edgedb_branch_create:


====================
edgedb branch create
====================

Create a new :ref:`branch <ref_datamodel_branches>`.

.. cli:synopsis::

    edgedb branch create [<options>] <name>

.. note::

    This CLI command requires CLI version 4.3.0 or later and EdgeDB version 5.0
    or later. If you are running an earlier version of EdgeDB, you will instead
    use the :ref:`ref_cli_edgedb_database_create` command to create a database,
    which branches replaced in EdgeDB 5.0.


Description
===========

``edgedb branch create`` creates a new branch with the same schema as the
current branch specified in ``$CONFIG/credentials``. Without any options, it is
equivalent to :eql:stmt:`create schema branch`.


Options
=======

The ``branch create`` command runs in the EdgeDB instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<name>`
    The name of the new branch.

:cli:synopsis:`--from <oldbranch>`
    The optional base branch to create the new branch from. Defaults to the
    current branch specified in ``$CONFIG/credentials``.

:cli:synopsis:`-e, --empty`
    Create a branch with no schema or data.

:cli:synopsis:`--copy-data`
    Copy data from the base branch to the new branch.
