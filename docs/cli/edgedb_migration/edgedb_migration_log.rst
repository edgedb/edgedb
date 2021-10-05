.. _ref_cli_edgedb_migration_log:


====================
edgedb migration log
====================

Show all migration versions.

.. cli:synopsis::

    edgedb migration log [<options>]

The tool will display the migration history either by reading it from
the EdgeDB instance or from the schema directory.

Options
=======

The ``migration log`` command runs on the database it is connected
to. For specifying the connection target see :ref:`connection options
<ref_cli_edgedb_connopts>`.

:cli:synopsis:`--from-fs`
    Print revisions from the schema directory (no database connection
    required). At least one of :cli:synopsis:`--from-db` or
    :cli:synopsis:`--from-fs` is required for ``migration log``
    command.

:cli:synopsis:`--from-db`
    Print revisions from the database (no schema files required). At
    least one of :cli:synopsis:`--from-db` or
    :cli:synopsis:`--from-fs` is required for ``migration log``
    command.

:cli:synopsis:`--newest-first`
    Sort migrations starting from newer to older, by default older
    revisions go first.

:cli:synopsis:`--schema-dir=<schema-dir>`
    Directory where the schema files are located. Defaults to
    ``./dbschema``.

:cli:synopsis:`--limit=<N>`
    Show maximum of :cli:synopsis:`N` revisions (default is unlimited).
