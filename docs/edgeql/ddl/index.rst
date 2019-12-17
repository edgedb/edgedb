.. _ref_eql_ddl:

Data Definition (DDL)
=====================

:edb-alt-title: Data Definition Language


EdgeQL includes a set of commands to manipulate all aspects of the
database schema.  It is called the *data definition language* or *DDL*,
and is a low-level equivalent to :ref:`EdgeDB schema definition language
<ref_eql_sdl>`.

In DDL the order of the commands matters, so if one type refers to
another, the other type has to be created first. For example:

.. code-block:: edgeql-repl

    db> CREATE TYPE Person {
    ...     CREATE REQUIRED PROPERTY first_name -> str;
    ...     CREATE REQUIRED PROPERTY last_name -> str;
    ... };
    CREATE
    db> CREATE TYPE Movie {
    ...     CREATE REQUIRED PROPERTY title -> str;
    ...     # the year of release
    ...     CREATE PROPERTY year -> int64;
    ...     CREATE REQUIRED LINK director -> Person;
    ...     CREATE MULTI LINK actors -> Person;
    ... };
    CREATE


.. toctree::
    :maxdepth: 3
    :hidden:

    modules
    objects
    scalars
    links
    props
    views
    indexes
    constraints
    functions
    annotations
    migrations
