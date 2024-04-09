.. _ref_cli_edgedb_branch_rename:


====================
edgedb branch rename
====================

Rename a :ref:`branch <ref_datamodel_branches>`

.. cli:synopsis::

    edgedb branch rename [<options>] <old-name> <new-name>

.. note::

    This CLI command requires CLI version 4.0 or later and EdgeDB version 5.0
    or later. Earlier versions did not feature branches and instead featured
    databases. Databases offered no analog to the ``rename`` command. To
    rename a database, you could :ref:`dump <ref_cli_edgedb_dump>` your
    database, :ref:`create <ref_cli_edgedb_database_create>` a new database
    with the desired name, and :ref:`restore <ref_cli_edgedb_restore>` the dump
    to that new database. See the :ref:`ref_cli_edgedb_database` command suite
    for other database management commands.


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
