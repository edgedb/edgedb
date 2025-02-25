.. versioned-section::

.. _ref_datamodel_index:

======
Schema
======

.. toctree::
    :maxdepth: 3
    :hidden:

    objects
    properties
    links
    computeds
    primitives
    indexes
    constraints
    inheritance
    aliases
    globals
    access_policies
    functions
    triggers
    mutation_rewrites
    linkprops
    modules
    migrations
    branches
    extensions
    annotations
    future
    comparison
    introspection/index


|Gel| schema is a high-level description of your application's data model.
In the schema, you define your types, links, access policies, functions,
triggers, constraints, indexes, and more.

Gel schema is strictly typed and is high-level enough to be mapped directly
to mainstream programming languages and back.


.. _ref_eql_sdl:

Schema Definition Language
==========================

Migrations are sequences of *data definition language* (DDL) commands.
DDL is a low-level language that tells the database exactly how to change
the schema. You typically won't need to write any DDL by hand; the Gel server
will generate it for you.

For a full guide on migrations, refer to the :ref:`Creating and applying
migrations <ref_intro_migrations>` guide or the
:ref:`migrations reference <ref_datamodel_migrations>` section.


Example:

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

Migrations and DDL
==================

Gel's baked-in migration system lets you painlessly evolve your schema over
time. Just update the contents of your |.gel| file(s) and use the |Gel| CLI
to *create* and *apply* migrations.

.. code-block:: bash

  $ gel migration create
  Created dbschema/migrations/00001.edgeql
  $ gel migrate
  Applied dbschema/migrations/00001.edgeql

Migrations are sequences of *data definition language* (DDL) commands.
DDL is a low level language that tells the database how exactly to change
the schema. Don't worry, you won't need to write any DDL directly, the Gel
server will generate it for you.

For a full guide on migrations, refer to the :ref:`Creating and applying
migrations <ref_intro_migrations>` guide or the
:ref:`migrations reference <ref_datamodel_migrations>` section.


.. _ref_datamodel_terminology:
.. _ref_datamodel_instances:

Instances, branches, and modules
================================

Gel is like a stack of containers:

* The *instance* is the running Gel process. Every instance has one or
  more |branches|. Instances can be created, started, stopped, and
  destroyed locally with :ref:`gel project <ref_cli_gel_project>`
  or low-level :ref:`gel instance <ref_cli_gel_instance>` commands.

* A *branch* is where your schema and data live. Branches map to PostgreSQL
  databases. Like instances, branches can be conveniently created, removed,
  and switched with the :ref:`gel branch <ref_cli_gel_branch>` commands.
  Read more about branches in the
  :ref:`branches reference <ref_datamodel_branches>`.

* A *module* is a collection of types, functions, and other definitions.
  The default module is called ``default``. Modules are used to organize
  your schema logically. Read more about modules in the
  :ref:`modules reference <ref_datamodel_modules>`.
