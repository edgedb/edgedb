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
    annotations
    globals
    access_policies
    functions
    triggers
    mutation_rewrites
    extensions
    future
    migrations

:edb-alt-title: Data Definition Language

EdgeQL includes a set of *data definition language* (DDL) commands that
manipulate the database's schema. DDL is the low-level equivalent to
:ref:`EdgeDB schema definition language <ref_eql_sdl>`. You can execute DDL
commands against your database, just like any other EdgeQL query.

.. code-block:: edgeql-repl

    edgedb> create type Person {
    .......     create required property name -> str;
    ....... };
    OK: CREATE TYPE
    edgedb> create type Movie {
    .......     create required property title -> str;
    .......     create required link director -> Person;
    ....... };
    OK: CREATE TYPE

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

