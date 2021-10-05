.. _ref_cli_edgedb_migration_status:


=======================
edgedb migration status
=======================

Show current migration state.

.. cli:synopsis::

    edgedb migration show [<options>]

The tool will show how the state of the schema in the EdgeDB instance
compares to the migrations stored in the schema directory.

Options
=======

The ``migration status`` command runs on the database it is connected
to. For specifying the connection target see :ref:`connection options
<ref_cli_edgedb_connopts>`.

:cli:synopsis:`--quiet`
    Do not print any messages, only indicate success by exit status.

:cli:synopsis:`--schema-dir=<schema-dir>`
    Directory where the schema files are located. Defaults to
    ``./dbschema``.
