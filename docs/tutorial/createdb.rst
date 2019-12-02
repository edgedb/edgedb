.. _ref_tutorial_createdb:

2. Database and Schema
======================

.. note::

    Syntax-highlighter packages are available for
    `Atom <https://atom.io/packages/edgedb>`_,
    `Visual Studio Code <https://marketplace.visualstudio.com/
    itemdetails?itemName=magicstack.edgedb>`_,
    `Sublime Text <https://packagecontrol.io/packages/EdgeDB>`_,
    and `Vim <https://github.com/edgedb/edgedb-vim>`_.


First step in a brand new project is to create the database for it:

.. code-block:: edgeql-repl

    edgedb> CREATE DATABASE tutorial;
    CREATE

The above :ref:`command <ref_admin_databases>` creates a new
:ref:`database <ref_datamodel_databases>` in the EdgeDB instance. Now
we should connect to it:

.. FIXME "\c" currently causes lexer errors in doc tests

.. code-block:: edgeql-repl

    edgedb> \c tutorial
    tutorial>

Now we need to set up the schema. Let's set up a basic schema for a
movies database. It will have 2 types of objects: movies and people
who directed and acted in them.

For the next step, there are two ways of setting this up. Please pick
one of the methods: :ref:`ref_tutorial_createdb_sdl` (recommended) or
:ref:`ref_tutorial_createdb_ddl`.


.. _ref_tutorial_createdb_sdl:

SDL
---

The :ref:`EdgeDB schema definition language <ref_eql_sdl>` provides a
way to describe a :ref:`migration <ref_eql_ddl_migrations>` to a
specific schema state. It is great for setting up a new database because it
focuses on expressing the final :ref:`types <ref_eql_sdl_object_types>` and
their :ref:`relationships <ref_eql_sdl_links>` without worrying about
the order of the definitions.

Migrations have to be done inside a :ref:`transaction
<ref_eql_statements_start_tx>`:

.. code-block:: edgeql-repl

    tutorial> START TRANSACTION;
    START TRANSACTION
    tutorial> CREATE MIGRATION movies TO {
    .........     module default {
    .........         type Movie {
    .........             required property title -> str;
    .........             # the year of release
    .........             property year -> int64;
    .........             required link director -> Person;
    .........             multi link cast -> Person;
    .........         }
    .........         type Person {
    .........             required property first_name -> str;
    .........             required property last_name -> str;
    .........         }
    .........     }
    ......... };
    CREATE MIGRATION
    tutorial> COMMIT MIGRATION movies;
    COMMIT MIGRATION
    tutorial> COMMIT;
    COMMIT TRANSACTION

The name of a migration doesn't matter much beyond providing a way to
specify the particular migration which must be committed. Once the
transaction is committed the schema is updated and we're ready to
:ref:`populate the database with data <ref_tutorial_queries>`.


.. _ref_tutorial_createdb_ddl:

DDL
---

.. important::

    The entire DDL section is an alternative to SDL. If you have
    completed the SDL steps you don't need to follow the steps in this
    section.


The :ref:`data definition language <ref_eql_ddl>` focuses on
transforming the current schema state into the desired target step by
step. This method is equally valid, but it is a lower level and more
explicit approach to altering the schema. It is also less transparent
in terms of giving a clear picture of the final resulting state.

In DDL the order of the commands matters, so the ``Person`` :ref:`type
<ref_eql_ddl_object_types>` must be created first:

.. code-block:: edgeql-repl

    tutorial> CREATE TYPE Person {
    .........     CREATE REQUIRED PROPERTY first_name -> str;
    .........     CREATE REQUIRED PROPERTY last_name -> str;
    ......... };
    CREATE

Now a ``Movie`` :ref:`type <ref_eql_ddl_object_types>` can be created with
:ref:`links <ref_eql_ddl_links>` referring to ``Person``:

.. code-block:: edgeql-repl

    tutorial> CREATE TYPE Movie {
    .........     CREATE REQUIRED PROPERTY title -> str;
    .........     # the year of release
    .........     CREATE PROPERTY year -> int64;
    .........     CREATE REQUIRED LINK director -> Person;
    .........     CREATE MULTI LINK cast -> Person;
    ......... };
    CREATE

Now that the schema is set up we're ready to
:ref:`populate the database with data <ref_tutorial_queries>`.
