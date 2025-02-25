.. _ref_datamodel_modules:
.. _ref_eql_sdl_modules:

=======
Modules
=======

Each |branch| has a schema consisting of several **modules**, each with
a unique name. Modules can be used to organize large schemas into
logical units. In practice, though, most users put their entire
schema inside a single module called ``default``.

.. code-block:: sdl

  module default {
    # declare types here
  }

.. _ref_name_resolution:

Name resolution
===============

When you define a module that references schema objects from another module,
you must use a *fully-qualified* name in the form
``other_module_name::object_name``:

.. code-block:: sdl

  module A {
    type User extending B::AbstractUser;
  }

  module B {
    abstract type AbstractUser {
      required name: str;
    }
  }

Reserved module names
=====================

The following module names are reserved by |Gel| and contain pre-defined
types, utility functions, and operators:

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


Modules are containers
======================

They can contain types, functions, and other modules. Here's an example of an
empty module:

.. code-block:: sdl

    module my_module {}

And here's an example of a module with a type:

.. code-block:: sdl

    module my_module {
      type User {
        required name: str;
      }
    }


Nested modules
==============

.. code-block:: sdl

    module dracula {
      type Person {
        required name: str;
        multi places_visited: City;
        strength: int16;
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

You can chain together module names in a fully-qualified name to traverse a
tree of nested modules. For example, to call the ``fight`` function in the
nested module example above, you would use
``dracula::combat::fight(<arguments>)``.


Declaring modules
=================

This section describes the syntax to declare a module in your schema.


Syntax
------

.. sdl:synopsis::

    module <ModuleName> "{"
      [ <schema-declarations> ]
      ...
    "}"

Define a nested module:

.. sdl:synopsis::

    module <ParentModuleName> "{"
      [ <schema-declarations> ]
      module <ModuleName> "{"
        [ <schema-declarations> ]
      "}"
      ...
    "}"


Description
^^^^^^^^^^^

The module block declaration defines a new module similar to the
:eql:stmt:`create module` command, but it also allows putting the
module content as nested declarations:

:sdl:synopsis:`<schema-declarations>`
    Define various schema items that belong to this module.

Unlike :eql:stmt:`create module`, a module block with the
same name can appear multiple times in an SDL document. In that case
all blocks with the same name are merged into a single module under
that name. For example:

.. code-block:: sdl

    module my_module {
      abstract type Named {
        required name: str;
      }
    }

    module my_module {
      type User extending Named;
    }

The above is equivalent to:

.. code-block:: sdl

    module my_module {
      abstract type Named {
        required name: str;
      }

      type User extending Named;
    }

Typically, in the documentation examples of SDL the *module block* is
omitted and instead its contents are described without assuming which
specific module they belong to.

It's also possible to declare modules implicitly. In this style, SDL
declaration uses a :ref:`fully-qualified name <ref_name_resolution>` for the
item that is being declared. The *module* part of the *fully-qualified* name
implies that a module by that name will be automatically created in the
schema. The following declaration is equivalent to the previous examples,
but it declares module ``my_module`` implicitly:

.. code-block:: sdl

    abstract type my_module::Named {
        required name: str;
    }

    type my_module::User extending my_module::Named;

A module block can be nested inside another module block to create a nested
module. If you want to reference an entity in a nested module by its
fully-qualified name, you will need to include all of the containing
modules' names: ``<ParentModuleName>::<ModuleName>::<EntityName>``

.. _ref_eql_ddl_modules:

DDL commands
============

This section describes the low-level DDL commands for creating and dropping
modules. You typically don't need to use these commands directly, but
knowing about them is useful for reviewing migrations.


Create module
-------------

:eql-statement:

Create a new module.

.. eql:synopsis::

    create module [ <parent-name>:: ] <name>
      [ if not exists ];

There's a :ref:`corresponding SDL declaration <ref_eql_sdl_modules>`
for a module, although in SDL a module declaration is likely to also
include that module's content.


Description
^^^^^^^^^^^

The command ``create module`` defines a new module for the current
:versionreplace:`database;5.0:branch`. The name of the new module must be
distinct from any existing module in the current
:versionreplace:`database;5.0:branch`. Unlike :ref:`SDL module declaration
<ref_eql_sdl_modules>` the ``create module`` command does not have sub-commands;
module contents are created separately.

Parameters
^^^^^^^^^^

:eql:synopsis:`if not exists`
    Normally, creating a module that already exists is an error, but
    with this flag the command will succeed. It is useful for scripts
    that add something to a module or, if the module is missing, the
    module is created as well.

Examples
^^^^^^^^

Create a new module:

.. code-block:: edgeql

    create module payments;

Create a new nested module:

.. code-block:: edgeql

    create module payments::currencies;


Drop module
-----------

:eql-statement:

Remove a module.

.. eql:synopsis::

    drop module <name> ;

Description
^^^^^^^^^^^

The command ``drop module`` removes an existing empty module from the
current :versionreplace:`database;5.0:branch`. If the module contains any
schema items, this command will fail.

Examples
^^^^^^^^

Remove a module:

.. code-block:: edgeql

    drop module payments;
