.. _ref_guide_migrations:

===================
Creating migrations
===================

EdgeDB’s baked-in migration system lets you painlessly evolve your schema
throughout the development process.

Workflow
--------

1. Update your schema file
^^^^^^^^^^^^^^^^^^^^^^^^^^

In EdgeDB, your application schema is declaratively defined using EdgeDB's
schema definition language.

.. code-block:: sdl

  type User {
    required property name -> str;
  }

  type Post {
    required property title -> str;
    required link author -> User;
  }


By convention, this schema lives inside of ``.esdl`` files inside the
``dbschema`` directory of your project. You can keep your entire schema in one
file (typically called ``default.esdl``) or split it across several files. The
EdgeDB CLI will automatically deep-merge all declarations. Your ``.esdl``
file(s) can be directly modified to reflect changes to your schema.

.. code-block:: sdl-diff

    type User {
      required property name -> str;
    }

    type BlogPost {
  -   required property title -> str;
  +   property title -> str;

  +   property upvotes -> int64;

      required link author -> User;
    }

  + type Comment {
  +   required property content -> str;
  + }

2. Generate a migration
^^^^^^^^^^^^^^^^^^^^^^^

To modify your schema, make a change to your schema file and run ``edgedb
migration create``.

Your schema file(s) will be sent to the appropriate EdgeDB instance, which will
compare the files to the current schema state and determine a migration plan.
This plan is then presented to you interactively; every detected schema change
will be individually presented to you for approval:

.. code-block:: bash

    $ edgedb migration create
    Did you create object type 'default::Comment'?
    [y,n,l,c,b,s,q,?]
    > y
    Did you make property 'title' of object type
    'default::BlogPost' optional? [y,n,l,c,b,s,q,?]
    > y
    Did you create property 'upvotes' of object type
    'default::BlogPost'? [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00002.edgeql,
    id: m16f7cbc...


As you can see, you are presented with an exhaustive list of the detected
schema changes. This is a useful sanity check, and it provides a level of
visibility into the migration process that is sorely lacking from most
migration tools.

For each of these prompts, you have a variety of commands at your disposal.
Type ``?`` into the prompt for an explanation of these options.

.. code-block:: bash

  $ edgedb migration create
  Did you create property X...? [y,n,l,c,b,s,q,?]
  >?

  y - confirm the prompt, use the DDL statements
  n - reject the prompt
  l - list the DDL statements associated with prompt
  c - list already confirmed EdgeQL statements
  b - revert back to previous save point, perhaps previous question
  s - stop and save changes (splits migration into multiple)
  q - quit without saving changes
  h or ? - print help

The process of creating migrations is truly interactive. You can go back to
previous prompts, split the schema changes into several individual migrations,
or inspect the associated DDL commands (e.g. ``create type``, etc).

Running ``migration create`` simply generates a migration script, it doesn't
apply it! So you can safely quit at any time with ``q`` or ``Ctrl/Cmd-C``
without worrying about leaving your schema in an inconsistent state.

Once you’ve completed the prompts, the CLI will generate a ``.edgeql`` file
representing the migration inside your ``dbschema/migrations`` directory.

3. Apply the migration
^^^^^^^^^^^^^^^^^^^^^^

Simply run ``edgedb migrate`` to apply all unapplied migrations.

.. code-block:: bash

  $ edgedb migrate
  Applied m1virjowa... (00001.edgeql)

That's it! Now you know how to migrate an EdgeDB schema. To learn how
migrations work in greater detail, check out the :ref:`CLI reference
<ref_cli_edgedb_migration>` or the `Beta 1 blog post
</blog/edgedb-1-0-beta-1-sirius#built-in-database-migrations-in-use>`_, which
describes the design of the migration system.

