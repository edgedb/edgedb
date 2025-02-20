.. _ref_datamodel_migrations:

==========
Migrations
==========

|Gel's| baked-in migration system lets you painlessly evolve your schema over
time. Just update the contents of your |.gel| file(s) and use the |Gel| CLI
to *create* and *apply* migrations.

.. code-block:: bash

  $ gel migration create
  Created dbschema/migrations/00001.edgeql

  $ gel migrate
  Applied dbschema/migrations/00001.edgeql

Refer to the :ref:`creating and applying migrations <ref_intro_migrations>`
guide for more information on how to use the migration system.

This document describes how migrations are implemented.


The migrations flow
===================

The migration flow is as follows:

1. The user edits the |.gel| files in the ``dbschema`` directory.

   This makes the schema described in the |.gel| files **different** from the
   actual schema in the database.

2. The user runs the :gelcmd:`migration create` command to create a new
   migration (a sequence of low-level DDL commands).

   * The CLI reads the |.gel| files and sends them to the |Gel| server, to
     analyze the changes.

   * The |Gel| server generates a migration plan and sends it back to the CLI.

   * The migration plan might require clarification from the user.

     If so, the CLI and the |Gel| server will go back and forth presenting
     the user with a sequence of questions, until the migration plan is
     clear and approved by the user.

3. The CLI writes the migration plan to a new file in the ``dbschema/migrations``
   directory.

4. The user runs the :gelcmd:`migrate` command to apply the migration to the
   database.

5. The user checks in the updated |.gel| files and the new
   ``dbschema/migrations`` migration file (created by :gelcmd:`migration create`)
   into version control.


Command line tools
==================

The two most important commands are:

* :gelcmd:`migration create`
* :gelcmd:`migrate`


Automatic migrations
====================

Sometimes when you're prototyping something new you don't want to spend
time worrying about migrations. There's no data to lose and not much code
that depends on the schema just yet.

For this use case you can use the :gelcmd:`watch` command, which will
monitor your |.gel| files and automatically create and apply migrations
for you in the background.

.. _ref_eql_ddl:

Data definition language (DDL)
==============================

The migration plan is a sequence of DDL commands. DDL commands are low-level
instructions that describe the changes to the schema.

SDL and your |.gel| files are like a 3D printer: you design the final shape,
and the system puts a database together for you. Using DDL is like building a
house the traditional way: to add a window, you first need a frame; to have a
frame, you need a wall; and so on.

If your schema looks like this:

.. code-block:: sdl

  type User {
    required name: str;
  }

then the corresponding DDL might look like this:

.. code-block:: edgeql

  create type User {
    create required property name: str;
  }

There are some circumstances where users might want to use DDL directly.
But in most cases you just need to learn how to read them to understand
the migration plan. Luckily, the DDL and SDL syntaxes were designed in tandem
and are very similar.

Most documentation pages on Gel's schema have a section about DDL commands,
e.g. :ref:`object types DDL <ref_eql_ddl_object_types>`.


.. _ref_eql_ddl_migrations:

Migration DDL commands
======================

Migrations themselves are a sequence of special DDL commands.

Like all DDL commands, ``start migration`` and other migration commands are
considered low-level. Users are encouraged to use the built-in
:ref:`migration tools <ref_cli_gel_migration>` instead.

However, if you want to implement your own migration tools, this section
will give you a good understanding of how Gel migrations work under the hood.


Start migration
---------------

:eql-statement:

Start a migration block.

.. eql:synopsis::

    start migration to "{"
        <sdl-declaration> ;
        [ ... ]
    "}" ;

Parameters
^^^^^^^^^^

:eql:synopsis:`<sdl-declaration>`
    Complete schema text (content of all |.gel| files) defined with
    the declarative :ref:`Gel schema definition language <ref_eql_sdl>`.

Description
^^^^^^^^^^^

The command ``start migration`` defines a migration of the schema to a
new state. The target schema state is described using :ref:`SDL
<ref_eql_sdl>` and describes the entire schema. This is important to
remember when creating a migration to add a few more things to an
existing schema as all the existing schema objects and the new ones
must be included in the ``start migration`` command. Objects that
aren't included in the command will be removed from the new schema
(which may result in data loss).

This command also starts a transaction block if not inside a
transaction already.

While inside a migration block, all issued EdgeQL statements are not executed
immediately and are instead recorded to be part of the migration script. Aside
from normal EdgeQL commands the following special migration commands are
available:

* :eql:stmt:`describe current migration` -- return a list of statements
  currently recorded as part of the migration;

* :eql:stmt:`populate migration` -- auto-populate the migration with
  system-generated DDL statements to achieve the target schema state;

* :eql:stmt:`abort migration` -- abort the migration block and discard the
  migration;

* :eql:stmt:`commit migration` -- commit the migration by executing the
  migration script statements and recording the migration into the system
  migration log.

Example
^^^^^^^

Create a new migration to a target schema specified by the Gel Schema
syntax:

.. code-block:: edgeql

    start migration to {
        module default {
            type User {
                property username: str;
            };
        };
    };


.. _ref_eql_ddl_migrations_create:

create migration
----------------

:eql-statement:

Create a new migration using an explicit EdgeQL script.

.. eql:synopsis::

    create migration "{"
        <edgeql-statement> ;
        [ ... ]
    "}" ;

Parameters
^^^^^^^^^^

:eql:synopsis:`<edgeql-statement>`
    Any valid EdgeQL statement, except ``database``, ``branch``, ``role``,
    ``configure``, ``migration``, or ``transaction`` statements.

Description
^^^^^^^^^^^

The command ``create migration`` executes all the nested EdgeQL commands
and records the migration into the system migration log.

Example
^^^^^^^

Create a new migration to a target schema specified by the Gel Schema
syntax:

.. code-block:: edgeql

    create migration {
        create type default::User {
            create property username: str;
        }
    };


Abort migration
---------------

:eql-statement:

Abort the current migration block and discard the migration.

.. eql:synopsis::

    abort migration ;

Description
^^^^^^^^^^^

The command ``abort migration`` is used to abort a migration block started by
:eql:stmt:`start migration`. Issuing ``abort migration`` outside of a
migration block is an error.

Example
^^^^^^^

Start a migration block and then abort it:

.. code-block:: edgeql

    start migration to {
        module default {
            type User;
        };
    };

    abort migration;


Populate migration
------------------

:eql-statement:

Populate the current migration with system-generated statements.

.. eql:synopsis::

    populate migration ;

Description
^^^^^^^^^^^

The command ``populate migration`` is used within a migration block started by
:eql:stmt:`start migration` to automatically fill the migration with
system-generated statements to achieve the desired target schema state. If
the system is unable to automatically find a satisfactory sequence of
statements to perform the migration, an error is returned. Issuing ``populate
migration`` outside of a migration block is also an error.

.. warning::

    The statements generated by ``populate migration`` may drop schema objects,
    which may result in data loss.  Make sure to inspect the generated
    migration using :eql:stmt:`describe current migration` before running
    :eql:stmt:`commit migration`!

Example
^^^^^^^

Start a migration block and populate it with auto-generated statements.

.. code-block:: edgeql

    start migration to {
        module default {
            type User;
        };
    };

    populate migration;


Describe current migration
--------------------------

:eql-statement:

Describe the migration in the current migration block.

.. eql:synopsis::

    describe current migration [ as {ddl | json} ];


Description
^^^^^^^^^^^

The command ``describe current migration`` generates a description of
the migration in the current migration block in the specified output
format:

:eql:synopsis:`as ddl`
    Show a sequence of statements currently recorded as part of the migration
    using valid :ref:`DDL <ref_eql_ddl>` syntax. The output will indicate
    if the current migration is fully defined, i.e. the recorded statements
    bring the schema to the state specified by :eql:stmt:`start migration`.

:eql:synopsis:`as json`
    Provide a machine-readable description of the migration using the following
    JSON format:

    .. code-block::

        {
          // Name of the parent migration
          "parent": "<parent-migration-name>",

          // Whether the confirmed DDL makes the migration complete,
          // i.e. there are no more statements to issue.
          "complete": {true|false},

          // List of confirmed migration statements
          "confirmed": [
            "<stmt text>",
            ...
          ],

          // The variants of the next statement
          // suggested by the system to advance
          // the migration script.
          "proposed": {
            "statements": [{
              "text": "<stmt text template>"
            }],
            "required-user-input": [
              {
                "placeholder": "<placeholder variable>",
                "prompt": "<statement prompt>"
              },
              ...
            ],
            "confidence": (0..1), // confidence coefficient
            "prompt": "<operation prompt>",
            "prompt_id": "<prompt id>",
            // Whether the operation is considered to be non-destructive.
            "data_safe": {true|false}
          }
        }

    Where:

    :eql:synopsis:`<stmt text>`
        Regular statement text.

    :eql:synopsis:`<stmt text template>`
        Statement text template with interpolation points using the ``\(name)``
        syntax.

    :eql:synopsis:`<placeholder variable>`
        The name of an interpolation variable in the statement text template
        for which the user prompt is given.

    :eql:synopsis:`<statement prompt>`
        The text of a user prompt for an interpolation variable.

    :eql:synopsis:`<operation prompt>`
        Prompt for the proposed migration step.

    :eql:synopsis:`<prompt id>`
        An opaque string identifier for a particular operation prompt.
        The client should not repeat prompts with the same prompt id.


Commit migration
----------------

:eql-statement:

Commit the current migration to the database.

.. eql:synopsis::

    commit migration ;

Description
^^^^^^^^^^^

The command ``commit migration`` executes all the commands defined by
the current migration and records the migration as the most recent
migration in the database.

Issuing ``commit migration`` outside of a migration block initiated
by :eql:stmt:`start migration` is an error.

Example
^^^^^^^

Create and execute the current migration:

.. code-block:: edgeql

    commit migration;


Reset schema to initial
-----------------------

:eql-statement:

Reset the database schema to its initial state.

.. eql:synopsis::

    reset schema to initial ;

.. warning::

    This command will drop all entities and, as a consequence, all data. You
    won't want to use this statement on a production instance unless you want
    to lose all that instance's data.


Migration rewrites DDL commands
===============================

Migration rewrites allow you to change the migration history as long as your
final schema matches the current database schema.

Start migration rewrite
-----------------------

Start a migration rewrite.

.. eql:synopsis::

    start migration rewrite ;

Once the migration rewrite is started, you can run any arbitrary DDL until you
are ready to :ref:`commit <ref_eql_ddl_migrations_rewrites_commit>` your new
migration history. The most useful DDL in this context will be :ref:`create
migration <ref_eql_ddl_migrations_create>` statements, which will allow you to
create a sequence of migrations that will become your new migration history.

Declare savepoint
-----------------

Establish a new savepoint within the current migration rewrite.

.. eql:synopsis::

    declare savepoint <savepoint-name> ;

Parameters
^^^^^^^^^^

:eql:synopsis:`<savepoint-name>`
    The name which will be used to identify the new savepoint if you need to
    later release it or roll back to it.

Release savepoint
-----------------

Destroys a savepoint previously defined in the current migration rewrite.

.. eql:synopsis::

    release savepoint <savepoint-name> ;

Parameters
^^^^^^^^^^

:eql:synopsis:`<savepoint-name>`
    The name of the savepoint to be released.

Rollback to savepoint
---------------------

Rollback to the named savepoint.

.. eql:synopsis::

    rollback to savepoint <savepoint-name> ;

All changes made after the savepoint are discarded. The savepoint remains valid
and can be rolled back to again later, if needed.

Parameters
^^^^^^^^^^

:eql:synopsis:`<savepoint-name>`
    The name of the savepoint to roll back to.

Rollback
--------

Rollback the entire migration rewrite.

.. eql:synopsis::

    rollback ;

All updates made within the transaction are discarded.

.. _ref_eql_ddl_migrations_rewrites_commit:

Commit migration rewrite
------------------------

Commit a migration rewrite.

.. eql:synopsis::

    commit migration rewrite ;
