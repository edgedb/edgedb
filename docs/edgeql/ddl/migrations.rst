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


Description
-----------

``CREATE MIGRATION`` defines a migration of the schema to a new state.
The target schema state is described using :ref:`SDL <ref_eql_sdl>`
and the migration generates the necessary :ref:`DDL <ref_eql_ddl>`
commands behind the scenes based on the current and the target state.

**Important:** ``CREATE MIGRATION`` and the follow-up
:eql:stmt:`COMMIT MIGRATION` must be executed in a transaction block.

.. note::

    The SDL declaration of the migration target describes the entire
    schema. This is important to remember when creating a migration to
    add a few more things to an existing schema as all the existing
    schema objects and the new ones must be included in the ``CREATE
    MIGRATION`` command. Objects that aren't included in the command
    will be removed from the new schema (which may result in data
    loss).

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

    COMMIT MIGRATION init;


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
