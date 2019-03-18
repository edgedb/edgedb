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
    CREATE MIGRATION <name> TO <schema-lang> <schema-content> ;

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

:eql:synopsis:`<schema-lang>`
    The name of the language used in *schema-content*.  Currently,
    only ``eschema`` is supported and specifies that the schema
    is described using the EdgeDB Schema language.

:eql:synopsis:`<schema-content>`
    The definition of the target schema for the module as a string constant.
    The format is determined by *schema-lang*.  It is often helpful to use
    :ref:`dollar quoting <ref_eql_lexical_dollar_quoting>` to write the
    schema definition string.

:eql:synopsis:`<ddl-command>`
    A list of arbitrary DDL commands.  :ref:`Database <ref_eql_ddl_databases>`,
    :ref:`module <ref_eql_ddl_modules>`, and migration commands cannot be
    used here.


Examples
--------

Create a new migration to a target schema specified by the EdgeDB Schema
syntax:

.. code-block:: edgeql

    CREATE MIGRATION init TO {
        type User {
            property username -> str
        }
    };

Create a new migration for the "payments" module using explicit DDL:

.. code-block:: edgeql

    START TRANSACTION;

    CREATE MIGRATION payments::alter_tx {
        ALTER TYPE Payment CREATE PROPERTY amount -> str;
        ALTER TYPE CreditCard CREATE PROPERTY cvv -> str;
    };

    COMMIT MIGRATION payments::alter_tx;

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


Examples
--------

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


Examples
--------

Remove the "init" migration:

.. code-block:: edgeql

    DROP MIGRATION init;
