.. _ref_cli_gel_migration_status:


====================
gel migration status
====================

Show current migration state.

.. cli:synopsis::

    gel migration status [<options>]

The tool will show how the state of the schema in the |Gel| instance
compares to the migrations stored in the schema directory.

Options
=======

The ``migration status`` command runs on the database it is connected
to. For specifying the connection target see :ref:`connection options
<ref_cli_gel_connopts>`.

:cli:synopsis:`--quiet`
    Do not print any messages, only indicate success by exit status.

:cli:synopsis:`--schema-dir=<schema-dir>`
    Directory where the schema files are located. Defaults to
    ``./dbschema``.
