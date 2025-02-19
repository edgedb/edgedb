.. eql:section-intro-page:: datamodel
.. versioned-section::

.. _ref_datamodel_index:

======
Schema
======

.. toctree::
    :maxdepth: 3
    :hidden:

    primitives
    objects
    properties
    links
    computeds
    indexes
    constraints
    aliases
    annotations
    globals
    access_policies
    modules
    functions
    triggers
    mutation_rewrites
    inheritance
    linkprops
    extensions
    future
    comparison
    introspection/index


|Gel| schemas are declared using **SDL** (Gel's Schema Definition
Language).

SDL
---

Your schema is defined inside |.gel| files. It's common to define your
entire schema in a single file called :dotgel:`default`, but you can split it
across multiple files if you wish.

By convention, your schema files should live in a directory called ``dbschema``
in the root of your project.

.. code-block:: sdl

    # dbschema/default.gel

    type Movie {
      required title: str;
      required director: Person;
    }

    type Person {
      required name: str;
    }

.. important::

  Syntax highlighter packages/extensions for |.gel| files are available for
  `Visual Studio Code <https://marketplace.visualstudio.com/
  itemdetails?itemName=magicstack.edgedb>`_,
  `Sublime Text <https://packagecontrol.io/packages/EdgeDB>`_,
  `Atom <https://atom.io/packages/edgedb>`_, and `Vim <https://github.com/
  geldata/edgedb-vim>`_.

Migrations
----------

Gel's baked-in migration system lets you painlessly evolve your schema over
time. Just update the contents of your |.gel| file(s) and use the |Gel| CLI
to *create* and *apply* migrations.

.. code-block:: bash

  $ gel migration create
  Created dbschema/migrations/00001.edgeql
  $ gel migrate
  Applied dbschema/migrations/00001.edgeql

For a full guide on migrations, refer to the :ref:`Creating and applying
migrations <ref_intro_migrations>` guide.

.. important::

  A migration consists of a sequence of *imperative* schema-modifying commands
  like ``create type``, ``alter property``, etc. Collectively these commands
  are known as :ref:`DDL <ref_eql_ddl>` (*data definition language*). We
  recommend using SDL and the migration system when building applications,
  however you're free to use DDL directly if you prefer.

.. _ref_datamodel_terminology:

Terminology
-----------

.. _ref_datamodel_instances:

Instance
^^^^^^^^

A |Gel| **instance** is a running Gel process. Instances can be created,
started, stopped, and destroyed locally with the :ref:`Gel CLI
<ref_cli_overview>`.

.. _ref_datamodel_databases:
.. _ref_datamodel_branches:

Branch
^^^^^^

.. versionadded:: 5.0

Prior to |EdgeDB| 5 and Gel, *branches* were called "databases"
(and "databases" is what Gel branches map to in PostgreSQL).

Instances can be branched when working on new features, similar to branches in
your VCS. Each branch has its own schema and data.


Module
^^^^^^

Each |branch| has a schema consisting of several
**modules**, each with a unique name. Modules can be used to organize large
schemas into logical units. In practice, though, most users put their entire
schema inside a single module called ``default``.

Read more about modules in the :ref:`modules <ref_datamodel_modules>` section.
