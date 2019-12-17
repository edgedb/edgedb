.. _ref_eql_sdl:

Schema Definition (SDL)
=======================

:edb-alt-title: Schema Definition Language


This section describes the high-level language used to define EdgeDB
schema.  It is called the EdgeDB *schema definition language* or
*SDL*.  There's a correspondence between this declarative high-level
language and the imperative low-level :ref:`DDL <ref_eql_ddl>`.

SDL is a declarative language optimized for human readability and
expressing the state of the EdgeDB schema without getting into the
details of how to arrive at that state.  Each *.esdl* file represents
a complete schema state for a particular
:ref:`module <ref_datamodel_modules>`.

Syntactically, an SDL declaration mirrors the ``CREATE`` DDL for the
corresponding entity, but with all of the ``CREATE`` and ``SET``
keywords omitted.

SDL is used to specify a :ref:`migration <ref_eql_ddl_migrations>` to a to a
specific schema state. For example:

.. code-block:: edgeql-repl

    db> START TRANSACTION;
    START TRANSACTION
    db> CREATE MIGRATION movies TO {
    ...     module default {
    ...         type Movie {
    ...             required property title -> str;
    ...             # the year of release
    ...             property year -> int64;
    ...             required link director -> Person;
    ...             required multi link actors -> Person;
    ...         }
    ...         type Person {
    ...             required property first_name -> str;
    ...             required property last_name -> str;
    ...         }
    ...     }
    ... };
    CREATE MIGRATION
    db> COMMIT MIGRATION movies;
    COMMIT MIGRATION
    db> COMMIT;
    COMMIT TRANSACTION


.. toctree::
    :maxdepth: 3
    :hidden:

    objects
    scalars
    links
    props
    views
    indexes
    constraints
    functions
    annotations
