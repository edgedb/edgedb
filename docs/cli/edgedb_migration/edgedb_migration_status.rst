.. _ref_cli_edgedb_migration_status:


=====================
edgedb migration show
=====================

Show current migration state.

.. cli:synopsis::

    edgedb [<connection-option>...] migration show [OPTIONS]

The tool will show how the state of the schema in the EdgeDB instance
compares to the migrations stored in the schema directory.

Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The ``show-status``
    command runs on the database it is connected to.

:cli:synopsis:`--schema-dir=<schema-dir>`
    Directory where the schema files are located. Defaults to
    ``./dbschema``.

:cli:synopsis:`--quiet`
    Do not print any messages, only indicate success by exit status.
