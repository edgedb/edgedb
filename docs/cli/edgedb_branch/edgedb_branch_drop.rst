.. _ref_cli_edgedb_branch_drop:


==================
edgedb branch drop
==================

Remove an existing :ref:`branch <ref_datamodel_branches>`.

.. cli:synopsis::

    edgedb branch drop [<options>] <name>

.. note::

    This CLI command requires CLI version 4.3.0 or later and EdgeDB version 5.0
    or later. If you are running an earlier version of EdgeDB, you will instead
    use the :ref:`ref_cli_edgedb_database_drop` command to drop a database,
    which branches replaced in EdgeDB 5.0.


Options
=======

The ``branch drop`` command runs in the EdgeDB instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<name>`
    The name of the branch to drop.

:cli:synopsis:`--non-interactive`
    Drop the branch without asking for confirmation.

:cli:synopsis:`--force`
    Close any existing connections to the branch before dropping it.
