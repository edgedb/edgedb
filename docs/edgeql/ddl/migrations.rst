.. _ref_eql_ddl_migrations:

==========
Migrations
==========

This section describes the DDL commands pertaining to migrations.


START MIGRATION
===============

:eql-statement:

Start a migration block.

.. eql:synopsis::

    START MIGRATION TO "{"
        <sdl-declaration> ;
        [ ... ]
    "}" ;

Parameters
----------

:eql:synopsis:`<sdl-declaration>`
    Complete schema defined with the declarative :ref:`EdgeDB schema
    definition language<ref_eql_sdl>`.

Description
-----------

``START MIGRATION`` defines a migration of the schema to a new state. The
target schema state is described using :ref:`SDL <ref_eql_sdl>` and describes
the entire schema. This is important to remember when creating a migration to
add a few more things to an existing schema as all the existing schema
objects and the new ones must be included in the ``START MIGRATION`` command.
Objects that aren't included in the command will be removed from the new
schema (which may result in data loss).

The ``START MIGRATION`` command also starts a transaction block if not inside
a transaction already.

While inside a migration block, all issued EdgeQL statements are not executed
immediately and are instead recorded to be part of the migration script.  Aside
from normal EdgeQL commands the following special migration commands are
available:

* :eql:stmt:`DESCRIBE CURRENT MIGRATION` -- return a list of statements
  currently recorded as part of the migration;

* :eql:stmt:`POPULATE MIGRATION` -- auto-populate the migration with
  system-generated DDL statements to achieve the target schema state;

* :eql:stmt:`ABORT MIGRATION` -- abort the migration block and discard the
  migration;

* :eql:stmt:`COMMIT MIGRATION` -- commit the migration by executing the
  migration script statements and recording the migration into the system
  migration log.

Examples
--------

Create a new migration to a target schema specified by the EdgeDB Schema
syntax:

.. code-block:: edgeql

    START MIGRATION TO {
        module default {
            type User {
                property username -> str;
            };
        };
    };


CREATE MIGRATION
================

:eql-statement:

Create a new migration using an explicit EdgeQL script.

.. eql:synopsis::

    CREATE MIGRATION "{"
        <edgeql-statement> ;
        [ ... ]
    "}" ;

Parameters
----------

:eql:synopsis:`<edgeql-statement>`
    Any valid EdgeQL statement, except ``DATABASE``, ``ROLE``, ``CONFIGURE``,
    ``MIGRATION``, or ``TRANSACTION`` statements.


Description
-----------

``CREATE MIGRATION`` runs the specified EdgeQL commands and records the
migration into the system migration log.


Examples
--------

Create a new migration to a target schema specified by the EdgeDB Schema
syntax:

.. code-block:: edgeql

    CREATE MIGRATION {
        CREATE TYPE default::User {
            CREATE PROPERTY username -> str;
        }
    };


ABORT MIGRATION
===============

:eql-statement:

Abort the current migration block and discard the migration.

.. eql:synopsis::

    ABORT MIGRATION ;

Description
-----------

``ABORT MIGRATION`` is used to abort a migration block started by
:eql:stmt:`START MIGRATION`.  Issuing ``ABORT MIGRATION`` outside of a
migration block is an error.

Examples
--------

Start a migration block and then abort it:

.. code-block:: edgeql

    START MIGRATION TO {
        module default {
            type User;
        };
    };

    ABORT MIGRATION;


POPULATE MIGRATION
==================

:eql-statement:

Populate the current migration with system-generated statements.

.. eql:synopsis::

    POPULATE MIGRATION ;

Description
-----------

``POPULATE MIGRATION`` is used within a migration block started by
:eql:stmt:`START MIGRATION` to automatically fill the migration with
system-generated statements to achieve the desired target schema state. If
the system is unable to automatically find a satisfactory sequence of
statements to perform the migration, an error is returned. Issuing ``POPULATE
MIGRATION`` outside of a migration block is also an error.

.. warning::

    ``POPULATE MIGRATION`` may generate statements that drop schema objects,
    which may result in data loss.  Make sure to inspect the generated
    migration using :eql:stmt:`DESCRIBE CURRENT MIGRATION` before running
    :eql:stmt:`COMMIT MIGRATION`!

Examples
--------

Start a migration block and populate it with auto-generated statements.

.. code-block:: edgeql

    START MIGRATION TO {
        module default {
            type User;
        };
    };

    POPULATE MIGRATION;


DESCRIBE CURRENT MIGRATION
==========================

:eql-statement:

Describe the migration in the current migration block.

.. eql:synopsis::

    DESCRIBE CURRENT MIGRATION [ AS {DDL | JSON} ];


Description
-----------

``DESCRIBE CURRENT MIGRATION`` generates a description of the migration
in the current migration block in the specified output format:

:eql:synopsis:`AS DDL`
    Show a sequence of statements currently recorded as part of the migration
    using valid :ref:`DDL <ref_eql_ddl>` syntax.  The output will indicate
    if the current migration is fully defined, i.e. the recorded statements
    bring the schema to the state specified by :eql:stmt:`START MIGRATION`.

:eql:synopsis:`AS JSON`
    Provide a machine-readable description of the migration using the following
    JSON format:

    .. code-block::

        {
          // Name of the parent migration
          "parent": "<parent-migraiton-name>",

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
                "prompt": "<statement prompt>",
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


COMMIT MIGRATION
================

:eql-statement:

Commit the current migration to the database.

.. eql:synopsis::

    COMMIT MIGRATION ;


Description
-----------

``COMMIT MIGRATION`` runs the commands defined by the current migration and
records the migration as the most recent migration in the database.

Issuing ``COMMIT MIGRATION`` outside of a migration block initiated
by :eql:stmt:`START MIGRATION` is an error.


Example
-------

Create and execute the current migration:

.. code-block:: edgeql

    COMMIT MIGRATION;
