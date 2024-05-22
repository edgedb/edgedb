.. _ref_cli_edgedb_migration_apply:


======================
edgedb migration apply
======================

Once the migration scripts are in place the changes can be applied to
the database by this command:

.. cli:synopsis::

    edgedb migration apply [<options>]

The tool will find all the unapplied migrations in
``dbschema/migrations/`` directory and sequentially run them on the
target instance.

.. warning:: EdgeDB Cloud CI users and scripters

    When scripting a ``migrate``/``migration apply`` for an EdgeDB Cloud
    instance, do not use ``edgedb login`` to authenticate. Instead, you should
    generate a secret key in the EdgeDB Cloud UI or by running
    :ref:`ref_cli_edgedb_cloud_secretkey_create` and set the
    ``EDGEDB_SECRET_KEY`` environment variable to your secret key. Once this
    variable is set to your secret key, logging in is no longer required.

Options
=======

The ``migration apply`` command runs on the database it is connected
to. For specifying the connection target see :ref:`connection options
<ref_cli_edgedb_connopts>`.

:cli:synopsis:`--quiet`
    Do not print any messages, only indicate success by exit status.

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

:cli:synopsis:`--dev-mode`
    .. note::

        The ``--dev-mod`` option is compatible with EdgeDB server 3.0 and
        above.

    Apply the current schema changes on top of the current migration history,
    without having created a new migration. This works the same way as
    :ref:`ref_cli_edgedb_watch` but without starting a long-running watch
    task.
