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
    :version-lt: 3.0

    module my_module {
        type User {
            required property name -> str;
        }
    }

.. code-block:: sdl

    module my_module {
        type User {
            required name: str;
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

.. versionadded:: 3.0

    Define a nested module.

    .. sdl:synopsis::

        module <ParentModuleName> "{"
          [ <schema-declarations> ]
          module <ModuleName> "{"
            [ <schema-declarations> ]
          "}"
          ...
        "}"


Description
-----------

The module block declaration defines a new module similar to the
:eql:stmt:`create module` command, but it also allows putting the
module content as nested declarations:

:sdl:synopsis:`<schema-declarations>`
    Define various schema items that belong to this module.

Unlike :eql:stmt:`create module` command, a module block with the
same name can appear multiple times in an SDL document.  In that case
all blocks with the same name are merged into a single module under
that name. For example:

.. code-block:: sdl
    :version-lt: 3.0

    module my_module {
        abstract type Named {
            required property name -> str;
        }
    }

    module my_module {
        type User extending Named;
    }

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
    :version-lt: 3.0

    module my_module {
        abstract type Named {
            required property name -> str;
        }

        type User extending Named;
    }

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

It's also possible to declare modules implicitly. In this style SDL
declaration uses :ref:`fully-qualified
name<ref_name_resolution>` for the item that is being
declared.  The *module* part of the *fully-qualified* name implies
that a module by that name will be automatically created in the
schema.  The following declaration is equivalent to the previous
examples, but it declares module ``my_module`` implicitly:

.. code-block:: sdl
    :version-lt: 3.0

    abstract type my_module::Named {
        required property name -> str;
    }

    type my_module::User extending my_module::Named;

.. code-block:: sdl

    abstract type my_module::Named {
        required name: str;
    }

    type my_module::User extending my_module::Named;

.. versionadded:: 3.0

   A module block can be nested inside another module block to create a nested
   module. If you want reference an entity in a nested module by its
   fully-qualified name, you will need to reference all of the containing
   modules' names: ``<ParentModuleName>::<ModuleName>::<EntityName>``
