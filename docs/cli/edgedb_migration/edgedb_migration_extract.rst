.. _ref_cli_edgedb_migration_extract:


========================
edgedb migration extract
========================

Extract migration history from the database and write it to
``/dbschema/migrations``. Useful when a direct DDL command has been used to
change the schema and now ``edgedb migrate`` will not comply because the
database migration history is ahead of the migration history inside
``/dbschema/migrations``.

This can also be useful if the migrations on the file system have been lost or
deleted.

Options
=======

The ``migration extract`` command runs on the database it is connected
to. For specifying the connection target see :ref:`connection options
<ref_cli_edgedb_connopts>`.


:cli:synopsis:`--tls-server-name <TLS_SERVER_NAME>`
    Override server name used for TLS connections and certificate verification.

    Useful when the server hostname cannot be used as it does not resolve, or
    resolves to a wrong IP address, and a different name or IP address is used
    in ``--host``.

:cli:synopsis:`--non-interactive`
    Don't ask questions, only add missing files, abort if mismatching

:cli:synopsis`--force`
    Force overwrite existing migration files

:cli:synopsis:`--schema-dir=<schema-dir>`
    Directory where the schema files are located. Defaults to
    ``./dbschema``.
