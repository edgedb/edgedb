.. _ref_cli_edgedb_migration:


===============
Migration tools
===============

EdgeDB provides schema migration tools as server-side tools. This
means that from the point of view of the application migrations are
language- and platform-agnostic and don't require additional
libraries.

Using the migration tools is the recommended way to make schema changes.

Setup
=====

First of all, the migration tools need a place to store the schema and
migration information. By default they will look in ``dbschema``
directory, but it's also possible to specify any other location by
using :cli:synopsis:`schema-dir` option. Inside this directory there
should be an ``.esdl`` file with :ref:`SDL <ref_eql_sdl>` schema
description. It's also possible to split the schema definition across
multiple ``.esdl`` files. The migration tools will read all of them
and treat them as a single SDL document.

.. _ref_cli_edgedb_create_migration:

edgedb create-migration
=======================

The next step after setting up the desired target schema is creating a
migration script. This is done by invoking the following command:

.. cli:synopsis::

    edgedb [<connection-option>...] create-migration [OPTIONS]

This will start an interactive tool that will provide the user with
suggestions based on the differences between the current database and
the schema file. The prompts will look something like this:

.. code-block::

    did you create object type 'default::User'? [y,n,l,c,b,s,q,?]
    ?

    y - confirm the prompt, use the DDL statements
    n - reject the prompt
    l - list the DDL statements associated with prompt
    c - list already confirmed EdgeQL statements
    b - revert back to previous save point, perhaps previous question
    s - stop and save changes (splits migration into multiple)
    q - quit without saving changes
    h or ? - print help

Options
-------

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The ``create-migration``
    command runs on the database it is connected to.

:cli:synopsis:`--schema-dir=<schema-dir>`
    Directory where the schema files are located. Defaults to
    ``./dbschema``.

:cli:synopsis:`--allow-empty`
    Create a new migration even if there are no changes. This is
    useful for creating migration stubs for data-only migrations.

:cli:synopsis:`--non-interactive`
    Do not prompts user for input. By default this works only if there
    are only "safe" changes to be done unless
    :cli:synopsis:`--allow-unsafe` is also specified.

:cli:synopsis:`--allow-unsafe`
    Apply the most probable unsafe changes in case there are any.
    This is only useful in non-interactive mode.


.. _ref_cli_edgedb_migrate:

edgedb migrate
==============

Once the migration scripts are in place the changes can be applied to
the database by this command:

.. cli:synopsis::

    edgedb [<connection-option>...] migrate [OPTIONS]

The tool will find all the unapplied migrations in
``dbschema/migrations/`` directory and sequentially run them on the
target instance.

Options
-------

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


.. _ref_cli_edgedb_migration_log:

edgedb migration-log
====================

Show all migration versions.

.. cli:synopsis::

    edgedb [<connection-option>...] migration-log [OPTIONS]

The tool will display the migration history either by reading it from
the EdgeDB instance or from the schema directory.

Options
-------

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The ``migration-log``
    command runs on the database it is connected to.

:cli:synopsis:`--schema-dir=<schema-dir>`
    Directory where the schema files are located. Defaults to
    ``./dbschema``.

:cli:synopsis:`--from-db`
    Print revisions from the database (no schema files required). At
    least one of :cli:synopsis:`--from-db` or
    :cli:synopsis:`--from-fs` is required for ``migration-log``
    command.

:cli:synopsis:`--from-fs`
    Print revisions from the schema directory (no database connection
    required). At least one of :cli:synopsis:`--from-db` or
    :cli:synopsis:`--from-fs` is required for ``migration-log``
    command.

:cli:synopsis:`--newest-first`
    Sort migrations starting from newer to older, by default older
    revisions go first.

:cli:synopsis:`--limit=<N>`
    Show maximum of :cli:synopsis:`N` revisions (default is unlimited).


.. _ref_cli_edgedb_show_status:

edgedb show-status
==================

Show current migration state.

.. cli:synopsis::

    edgedb [<connection-option>...] show-status [OPTIONS]

The tool will show how the state of the schema in the EdgeDB instance
compares to the migrations stored in the schema directory.

Options
-------

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The ``show-status``
    command runs on the database it is connected to.

:cli:synopsis:`--schema-dir=<schema-dir>`
    Directory where the schema files are located. Defaults to
    ``./dbschema``.

:cli:synopsis:`--quiet`
    Do not print any messages, only indicate success by exit status.
