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

Create migration script
=======================

The next step after setting up the desired target schema is creating a
migration script. This is done by invoking the following command:

.. cli:synopsis::

    edgedb -I <instance-name> create-migration [migration-option...]

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

.. _ref_cli_edgedb_migrate:

Apply migrations
================

Once the migration scripts are in place the changes can be applied to
the database by this command:

.. cli:synopsis::

    edgedb -I <instance-name> migrate [migration-option...]

The tool will find all the unapplied migrations in
``dbschema/migrations/`` directory and sequentially run them on the
target instance.

Options
=======

:cli:synopsis:`schema-dir`
    The directory that contains the ``.esdl`` schema files and
    ``migrations`` sub-directory for individual scripts.
