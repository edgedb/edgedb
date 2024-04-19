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
    functions
    triggers
    mutation_rewrites
    inheritance
    extensions
    future
    comparison
    introspection/index


EdgeDB schemas are declared using **SDL** (EdgeDB's Schema Definition
Language).

SDL
---

Your schema is defined inside ``.esdl`` files. It's common to define your
entire schema in a single file called ``default.esdl``, but you can split it
across multiple files if you wish.

By convention, your schema files should live in a directory called ``dbschema``
in the root of your project.

.. code-block:: sdl
    :version-lt: 3.0

    # dbschema/default.esdl

    type Movie {
      required property title -> str;
      required link director -> Person;
    }

    type Person {
      required property name -> str;
    }

.. code-block:: sdl

    # dbschema/default.esdl

    type Movie {
      required title: str;
      required director: Person;
    }

    type Person {
      required name: str;
    }

.. important::

  Syntax highlighter packages/extensions for ``.esdl`` files are available for
  `Visual Studio Code <https://marketplace.visualstudio.com/
  itemdetails?itemName=magicstack.edgedb>`_,
  `Sublime Text <https://packagecontrol.io/packages/EdgeDB>`_,
  `Atom <https://atom.io/packages/edgedb>`_, and `Vim <https://github.com/
  edgedb/edgedb-vim>`_.

Migrations
----------

EdgeDB's baked-in migration system lets you painlessly evolve your schema over
time. Just update the contents of your ``.esdl`` file(s) and use the EdgeDB CLI
to *create* and *apply* migrations.

.. code-block:: bash

  $ edgedb migration create
  Created dbschema/migrations/00001.esdl
  $ edgedb migrate
  Applied dbschema/migrations/00001.esdl.

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

An EdgeDB **instance** is a running EdgeDB process. Instances can be created,
started, stopped, and destroyed locally with the :ref:`EdgeDB CLI
<ref_cli_overview>`.

.. _ref_datamodel_branches:

Branches
^^^^^^^^

.. versionadded:: 5.0

Instances can be branched when working on new features, similar to branches in
your VCS. Each branch has its own schema and data.

.. _ref_datamodel_databases:

Database
^^^^^^^^

.. versionadded:: 5.0

    In EdgeDB 5, databases were replaced by branches.

Each instance can contain several **databases**, each with a unique name. At
the time of creation, all instances contain a single default database called
``edgedb``. All incoming queries are executed
against it unless otherwise specified.

.. _ref_datamodel_modules:

Module
^^^^^^

Each branch (or database pre-v5) has a schema consisting of several
**modules**, each with a unique name. Modules can be used to organize large
schemas into logical units. In practice, though, most users put their entire
schema inside a single module called ``default``.

.. code-block:: sdl

  module default {
    # declare types here
  }

.. versionadded:: 3.0

    You may define nested modules using the following syntax:

    .. code-block:: sdl

        module dracula {
            type Person {
              required property name -> str;
              multi link places_visited -> City;
              property strength -> int16;
            }

            module combat {
                function fight(
                  one: dracula::Person,
                  two: dracula::Person
                ) -> str
                  using (
                    (one.name ?? 'Fighter 1') ++ ' wins!'
                    IF (one.strength ?? 0) > (two.strength ?? 0)
                    ELSE (two.name ?? 'Fighter 2') ++ ' wins!'
                  );
            }
        }

    Here we have a ``dracula`` module containing a ``Person`` type. Nested in
    the ``dracula`` module we have a ``combat`` module which will be used for
    all the combat functionality for our game based on Bram Stoker's Dracula we
    built in the `Easy EdgeDB textbook </easy-edgedb>`_.

.. _ref_name_resolution:

.. note:: Name resolution

  When referencing schema objects from another module, you must use
  a *fully-qualified* name in the form ``module_name::object_name``.

The following module names are reserved by EdgeDB and contain pre-defined
types, utility functions, and operators.

* ``std``: standard types, functions, and operators in the :ref:`standard
  library <ref_std>`
* ``math``: algebraic and statistical :ref:`functions <ref_std_math>`
* ``cal``: local (non-timezone-aware) and relative date/time :ref:`types and
  functions <ref_std_datetime>`
* ``schema``: types describing the :ref:`introspection
  <ref_datamodel_introspection>` schema
* ``sys``: system-wide entities, such as user roles and
  :ref:`databases <ref_datamodel_databases>`
* ``cfg``: configuration and settings

.. versionadded:: 3.0

    You can chain together module names in a fully-qualified name to traverse a
    tree of nested modules. For example, to call the ``fight`` function in the
    nested module example above, you would use
    ``dracula::combat::fight(<arguments>)``.
