.. eql:section-intro-page:: datamodel
.. _ref_datamodel_intro:

.. _ref_datamodel_index:

===============
Schema Modeling
===============

One of EdgeDB's foundational features is **declarative schema modeling**.

.. toctree::
    :maxdepth: 3
    :hidden:

    terminology
    modules
    objects
    props
    links
    linkprops
    computables
    indexes
    constraints
    aliases
    functions
    inheritance
    annotations
    extensions
    migrations
    comparison


With EdgeDB, you can define your schema with EdgeDB's schema definition
language, called **EdgeDB SDL** or simply **SDL**. SDL's declarative,
object-oriented syntax will look familiar to users or ORM libraries.

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
current state of your schema at a glance.

Secondly, it's **object-oriented**. There are no foreign keys; instead,
relationships between types are directly represented with :ref:`Links
<ref_datamodel_links>`; this is part of what makes EdgeQL queries so concise
and powerful.

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


See also
--------

**DDL**
  EdgeDB provides a migration tool to synchronize a database's with the latest
  SDL files. Migrations consist of a sequence of *imperative* commands like
  ``CREATE TYPE``, ``ALTER PROPERTY``, etc. Collectively these commands are
  known as EdgeDB Data Definition Language (**DDL** for short).

  We recommend that most users use SDL and migrations when building
  applications. However, if you prefer SQL-style imperative schema modeling,
  you are free to use DDL directly; reference the :ref:`DDL Reference
  <ref_eql_ddl>` to learn more.
