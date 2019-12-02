.. _ref_eql_ddl_migrations:

==========
Migrations
==========

This section describes the DDL commands pertaining to migrations.


CREATE MIGRATION
================

:eql-statement:

Create a new migration.

.. eql:synopsis::

    [ WITH [ <module-alias> := ] MODULE <module-name> ]
    CREATE MIGRATION <name> TO "{"
        <sdl-declaration> ;
        [ ... ]
    "}" ;

    [ WITH [ <module-alias> := ] MODULE <module-name> ]
    CREATE MIGRATION <name> "{"
        <ddl-command> ;
        [ ... ]
    "}" ;


Description
-----------

``CREATE MIGRATION`` defines a new schema migration for a specific module.
If *name* is qualified with a module name, then the migration is created
for that module, owtherwise it is created for the current module, as
determined by the session or the ``WITH`` block.

There are two forms of ``CREATE MIGRATION`` as shown in the synopsis above.
The first form uses a specific description of the target schema state and
generates the necessary DDL commands automatically based on the current and
the target state.  The second form uses explicit DDL command specifications
for the migration.

**Important:** ``CREATE MIGRATION`` and the follow-up
:eql:stmt:`COMMIT MIGRATION` must be executed in a transaction block.


Parameters
----------

:eql:synopsis:`[ <module-alias> := ] MODULE <module-name>`
    An optional list of module alias declarations to be used in the
    migration definition.  When *module-alias* is not specified,
    *module-name* becomes the effective current module and is used
    to resolve all unqualified names.

:eql:synopsis:`<name>`
    The name of the new migration.  If qualifed with a module name,
    the migration is created for that module, otherwise the effective
    current module is used.

:eql:synopsis:`<sdl-declaration>`
    Module contents defined using the declarative :ref:`EdgeDB schema
    definition language<ref_eql_sdl>`.

:eql:synopsis:`<ddl-command>`
    A list of arbitrary DDL commands.  :ref:`Database <ref_admin_databases>`,
    :ref:`module <ref_eql_ddl_modules>`, and migration commands cannot be
    used here.


Examples
--------

Create a new migration to a target schema specified by the EdgeDB Schema
syntax:

.. code-block:: edgeql

    CREATE MIGRATION init TO {
        module default {
            type User {
                property username -> str
            }
        }
    };

Create a new migration for the "payments" module using explicit DDL:

.. code-block:: edgeql

    START TRANSACTION;

    CREATE MIGRATION alter_tx {
        ALTER TYPE payments::Payment CREATE PROPERTY amount -> str;
        ALTER TYPE payments::CreditCard CREATE PROPERTY cvv -> str;
    };

    COMMIT MIGRATION alter_tx;

    COMMIT;


COMMIT MIGRATION
================

:eql-statement:

Apply the given migration to the database.

.. eql:synopsis::

    COMMIT MIGRATION <name> ;


Description
-----------

``COMMIT MIGRATION`` runs the DDL commands defined by the given migration.
Once the migration is committed, it cannot be dropped.

**Important:** ``COMMIT MIGRATION`` must be executed in a transaction block.


Parameters
----------

:eql:synopsis:`<name>`
    The name of the migration to commit.


Example
-------

Commit the "alter_tx" migration:

.. code-block:: edgeql

    COMMIT MIGRATION payments::alter_tx;


DROP MIGRATION
==============

:eql-statement:

Discard a migration.

.. eql:synopsis::

    DROP MIGRATION <name> ;


Description
-----------

``DROP MIGRATION`` discards the given migration.  Once a migration has
been applied using a ``COMMIT MIGRATION`` command, it cannot be discarded.


Parameters
----------

:eql:synopsis:`<name>`
    The name of the migration to discard.


Example
-------

Remove the "init" migration:

.. code-block:: edgeql

    DROP MIGRATION init;
