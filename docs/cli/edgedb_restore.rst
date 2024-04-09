.. _ref_cli_edgedb_restore:


==============
edgedb restore
==============

Restore an EdgeDB branch (or database pre-v5) from a backup file.

.. cli:synopsis::

    edgedb restore [<options>] <path>


Description
===========

``edgedb restore`` is a terminal command used to restore an EdgeDB database
branch (or database pre-v5) from a backup file. The backup is restored to the
currently active branch (or to the currently connected database pre-v5).

.. note::

    The backup cannot be restored to a branch (or database pre-v5) with any
    existing schema. As a result, you should restore to one of these targets:

    - a new empty branch which can be created using
      :ref:`ref_cli_edgedb_branch_create` with the ``--empty`` option
    - a new empty database if your instance is running EdgeDB versions prior to
      5
    - an existing branch or database that has been wiped with the appropriate
      ``wipe`` command (either :ref:`ref_cli_edgedb_branch_wipe` or
      :ref:`ref_cli_edgedb_database_wipe`; note that this will destroy all data
      and schema currently in that branch/database)


Options
=======

The ``restore`` command restores the backup file into the active branch or, in
pre-v5 instance, the currently connected database. For specifying the
connection target see :ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<path>`
    The name of the backup file to restore the database branch from.

:cli:synopsis:`--all`
    Restore all branches (or databases pre-v5) and the server configuration
    using the directory specified by the :cli:synopsis:`<path>`.

:cli:synopsis:`-v, --verbose`
    Verbose output.
