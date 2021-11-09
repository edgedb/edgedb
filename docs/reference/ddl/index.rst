.. _ref_eql_ddl:

DDL
===

.. toctree::
    :maxdepth: 3
    :hidden:

    modules
    objects
    scalars
    links
    properties
    aliases
    indexes
    constraints
    functions
    annotations
    migrations
    extensions

:edb-alt-title: Data Definition Language

EdgeQL includes a set of *data definition language* (DDL) commands that
manipulate the database's schema. DDL is the low-level equivalent to
:ref:`EdgeDB schema definition language <ref_eql_sdl>`. You can execute DDL
commands against your database, just like any other EdgeQL query.

.. code-block:: edgeql-repl

    db> CREATE TYPE Person {
    ...     CREATE REQUIRED PROPERTY name -> str;
    ... };
    OK: CREATE
    db> CREATE TYPE Movie {
    ...     CREATE REQUIRED PROPERTY title -> str;
    ...     CREATE REQUIRED LINK director -> Person;
    ... };
    OK: CREATE

In DDL, the *order* of commands is important. In the example above, you
couldn't create ``Movie`` before ``Person``, because ``Movie`` contains a link
to ``Person``.

Under the hood, all migrations are represented as DDL scripts: a sequence of
imperative commands representing the migration. When you :ref:`create a
migration <ref_cli_edgedb_migrate>` with the CLI, EdgeDB produces a DDL script.


Comparison to SDL
-----------------

SDL is sort of like a 3D printer: you design the final shape and it puts
it together for you. DDL is like building a house with traditional
methods: to add a window, you first need a frame, to have a frame you
need a wall, and so on.

DDL lets you make quick changes to your schema without creating migrations. But
it can be dangerous too; some ``DDL`` commands can destroy user data
permanantly. In practice, we recommend most users stick with SDL until they get
comfortable, then start experimenting with DDL.

