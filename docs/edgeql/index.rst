.. eql:section-intro-page:: edgeql

======
EdgeQL
======

EdgeQL is the primary language of EdgeDB. It is used to define, mutate,
and query data. EdgeQL encompasses four items in general:

**Querying language | SDL | DDL | Administration**

Most frequently used in EdgeQL is the **querying language**. EdgeQL is
a functional language in that every expression is a composition of
one or more queries. The querying language looks like this:

.. code-block:: edgeql

    SELECT Issue {
        number,
        body,
        due_date
    }
    FILTER
        EXISTS .due_date
    ORDER BY
        .due_date
    LIMIT 3;

The simplicity of EdgeQL queries might remind you of GraphQL a bit,
and EdgeQL does indeed have a similar straightforwardness while being
more powerful
(see some [comparisons of syntax here]() for details on that).


**SDL (schema definition language)**. SDL is used to describe migrations
to a specific schema state. In SDL you write the final types and
relationships without needing to worry about making declarations in any
particular order. Items in a schema written in SDL look like this:

.. code-block:: sdl

    type Movie {
        required property title -> str;
        required link director -> Person;
        multi link actors -> Person;
    }

    type Person {
        required property name -> str;
    }

**DDL (data definition language)**. DDL is the less frequently used
(but often convenient) cousin of SDL. DDL is used to transform schema
step by step instead of all at once. The order is important in DDL
because it involves issuing one command after another instead of
describing the final form.

So you can't create ``Movie`` first using DDL because it links
to ``Person``.  The two are created in this order:

.. code-block:: edgeql-repl

    db> CREATE TYPE Person {
    ...    CREATE REQUIRED PROPERTY name -> str;
    ... };
    CREATE: OK
    db> CREATE TYPE Movie {
    ...     CREATE REQUIRED PROPERTY title -> str;
    ...     CREATE REQUIRED LINK director -> Person;
    ...     CREATE MULTI LINK actors -> Person;
    ... };
    CREATE: OK

SDL is sort of like a 3D printer: you set the final shape and it puts
it together for you. DDL is like building a house with traditional
methods: to add a window you first need a frame, to have a frame you
need a wall, and so on. But DDL is great for making quick changes to
your schema without a new migration, in the same way that you can
replace a window without describing the whole house to do it.
In practice, most people stick to SDL until they get comfortable
and only then begin to experiment with DDL.

**The remainder:** this includes infrequently used but critical items
involved in database administration. Start with the
**Administration** link below to see how to configure a database,
set roles, passwords, and so on using EdgeDB.


.. toctree::
    :maxdepth: 3
    :hidden:

    overview
    expressions/__toc__
    statements/index
    funcops/index
    types
    sdl/index
    ddl/index
    introspection/index
    admin/index
    lexical
