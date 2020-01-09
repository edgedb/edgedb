.. _ref_eql_sdl_modules:

=======
Modules
=======

This section describes the SDL commands pertaining to
:ref:`modules <ref_datamodel_modules>`.


Example
-------

Declare an empty module:

.. code-block:: sdl

    module my_module {}


Declare a module with some content:

.. code-block:: sdl

    module my_module {
        type User {
            required property name -> str;
        }
    }

Syntax
------

Define a module corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_modules>`.

.. sdl:synopsis::

    module <ModuleName> "{"
      [ <schema-declarations> ]
      ...
    "}"


Description
-----------

The module block declaration defines a new module much like
:eql:stmt:`CREATE MODULE`.  Unlike its DDL counterpart the module
block can have sub-declarations:

:sdl:synopsis:`<schema-declarations>`
    Define various schema items that belong to this module.

Unlike :eql:stmt:`CREATE MODULE` command, a module block with the
same name can appear multiple times in an SDL document.  In that case
all blocks with the same name are merged into a single module under
that name. For example:

.. code-block:: sdl

    module my_module {
        abstract type Named {
            required property name -> str;
        }
    }

    module my_module {
        type User extending Named;
    }

The above is equivalent to:

.. code-block:: sdl

    module my_module {
        abstract type Named {
            required property name -> str;
        }

        type User extending Named;
    }

Typically, in the documentation examples of SDL the *module block* is
omitted and instead its contents are described without assuming which
specific module they belong to.

It's also possible to declare modules implicitly. In this style SDL
declaration uses :ref:`fully-qualified
name<ref_eql_fundamentals_name_resolution>` for the item that is being
declared.  The *module* part of the *fully-qualified* name implies
that a module by that name will be automatically created in the
schema.  The following declaration is equivalent to the previous
examples, but it declares module ``my_module`` implicitly:

.. code-block:: sdl

    abstract type my_module::Named {
        required property name -> str;
    }

    type my_module::User extending my_module::Named;
