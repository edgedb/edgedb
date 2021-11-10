.. eql:section-intro-page:: datamodel

.. _ref_datamodel_index:

======
Schema
======

One of EdgeDB's foundational features is **declarative schema modeling**.

.. toctree::
    :maxdepth: 3
    :hidden:

    terminology
    modules
    primitives
    objects
    properties
    links
    computeds
    indexes
    constraints
    aliases
    annotations
    functions
    inheritance
    extensions
    comparison



EdgeDB schemas are declared using **SDL** (EdgeDB Schema Definition Language).
SDL's declarative, object-oriented syntax will look familiar to users of ORM
libraries.

.. code-block:: sdl

  type Movie {
    required property title -> str;
    required link director -> Person;
  }

  type Person {
    required property name -> str;
  }


SDL
---

SDL has two important properties. First, it's **declarative**; you can just
write your schema down exactly as you want it to be. It's easy to see the
entire state of your schema at a glance.

Secondly, it's **object-oriented**. There are no foreign keys; instead,
relationships between types are directly represented with :ref:`Links
<ref_datamodel_links>`; this is part of what makes deep EdgeQL queries so
concise. For example:

.. code-block:: edgeql

  SELECT Movie {
    title,
    director: {
      name
    }
  }


``.esdl`` files
---------------

Your schema should be defined in one or more ``.esdl`` files. These files
should be placed in a directory called ``dbschema`` in the root of your
project.

.. important::

  Syntax highlighter packages/extensions for ``.esdl`` files are available for
  `Visual Studio Code <https://marketplace.visualstudio.com/
  itemdetails?itemName=magicstack.edgedb>`_,
  `Sublime Text <https://packagecontrol.io/packages/EdgeDB>`_,
  `Atom <https://atom.io/packages/edgedb>`_, and `Vim <https://github.com/
  edgedb/edgedb-vim>`_.


Migrations
----------

EdgeDB’s baked-in migration system lets you painlessly evolve your schema
throughout the development process. After modifying your ``.esdl`` files, you
can *create* and *apply* a migration with the EdgeDB command-line tool. For a
full guide on how migrations work, reference the :ref:`Creating and applying
migrations <ref_guide_migrations>` guide.

.. important::

  A migration consists of a sequence of *imperative* schema-modifying commands
  like ``CREATE TYPE``, ``ALTER PROPERTY``, etc. Collectively these commands
  are known as DDL (*data definition language*). We recommend that most users
  use SDL and migrations when building applications. However, if you prefer
  SQL-style imperative schema modeling, you are free to use DDL directly;
  go to :ref:`Reference > DDL <ref_eql_ddl>` to learn more.


