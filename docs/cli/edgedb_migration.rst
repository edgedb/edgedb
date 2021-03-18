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

Here is typical file tree::

    $ tree dbschema
    dbschema
    ├── default.esdl
    └── migrations
        ├── 00001.edgeql
        ├── 00002.edgeql
        ├── 00003.edgeql
        ├── ....
        └── 00019.edgeql

In the tree:

* ``dbschema/*.esdl`` is a set of files that contains current database schema
* ``default.esdl`` is named after ``module default`` that is usually used for
  the initial application schema. There is no requirement to use a single
  module, but this is a good start.
* ``dbschema/migrations/*.edgeql`` is a sequence of migration files that when
  applied bring database schema to the latest version. These files are mostly
  managed by command-line tools, but have to be commited to source code
  repository, and occasionally may need to be edited manually (common example
  is when merging a branch).


.. _ref_cli_edgedb_migration_workflow:

General Workflow
================

1. Create or edit schema in ``dbschema/*.esdl`` files
2. Before applying schema, create a migration file:

   .. code-block:: bash

      edgedb create-migration

   It runs through changes interactively and puts
   ``dbschema/migrations/<revision-number>.edgeql`` file into the file system
   (later you want to check it into a version control)

3. To apply schema run:

   .. code-block:: bash

      edgedb migrate


It's worth mentioning that after code checkout (e.g. ``git pull``) it makes
sense to run ``edgedb migrate`` again. Server has the latest migration id
recorded so will only apply new migrations.

At any time ``edgedb show-status`` describes if there are any pending
migrations or schema changes.


.. _ref_cli_edgedb_create_migration:

Create migration script
=======================

After editing schema files, migration script must be created.
This is done by invoking the following command:

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


.. _ref_cli_edgedb_show-status:

Show Status
===========

To figure out the status of the schema and migrations use the respective
command:

.. cli:synopsis::

    edgedb -I <instance-name> show-status [migration-option...]

This might result in few different scenarios:

.. code-block:: bash

   $ edgedb -Imyapp show-status
   Database is up to date.
   Last migration: m1dcrpvcmyooykcbbgixwajmlqimkhfgpuu5xnyp4ziedpd64akxpa.

This means everything up to date. If you've edited the schema:

.. code-block:: bash

   $ edgedb -Imyapp show-status
   Detected differences between the database schema and the schema source,
   in particular:
       CREATE TYPE default::NewType;
   Some migrations are missing, use `edgedb create-migration`

And after creating migration or ``git pull``:

.. code-block:: bash

   Database is
   at migration "m1dcrpvcmyooykcbbgixwajmlqimkhfgpuu5xnyp4ziedpd64akxpa"
   while sources contain 1 migrations ahead,
   starting from "m1b3lvddqzkcw3wxw7cckdhrkgnr7uwjyh7cge5amak52ahg4z6hqq"
   (./dbschema/migrations/00020.edgeql)

Which suggests to run ``edgedb migrate``.


Options
=======

:cli:synopsis:`schema-dir`
    The directory that contains the ``.esdl`` schema files and
    ``migrations`` sub-directory for individual scripts.
