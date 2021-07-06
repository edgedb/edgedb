.. _ref_cli_edgedb_migration_apply:


======================
edgedb migration apply
======================

Once the migration scripts are in place the changes can be applied to
the database by this command:

.. cli:synopsis::

    edgedb [<connection-option>...] migration apply [OPTIONS]

The tool will find all the unapplied migrations in
``dbschema/migrations/`` directory and sequentially run them on the
target instance.

Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The ``migrate``
    command runs on the database it is connected to.

:cli:synopsis:`--schema-dir=<schema-dir>`
    Directory where the schema files are located. Defaults to
    ``./dbschema``.

:cli:synopsis:`--to-revision=<to-revision>`
    Upgrade to a specified revision.

    Unique prefix of the revision can be specified instead of full
    revision name.

    If this revision is applied, the command is no-op. The command
    ensures that this revision present, but it's not an error if more
    revisions are applied on top.
