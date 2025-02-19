.. _ref_eql_sdl:

SDL
===

:edb-alt-title: Schema Definition Language


This section describes the high-level language used to define Gel
schema.  It is called the Gel *schema definition language* or
*SDL*.  There's a correspondence between this declarative high-level
language and the imperative low-level :ref:`DDL <ref_eql_ddl>`.

SDL is a declarative language optimized for human readability and
expressing the state of the Gel schema without getting into the details
of how to arrive at that state.  Each *SDL* block represents the complete
schema state for a given :ref:`branch <ref_datamodel_branches>`.

Syntactically, an SDL declaration mirrors the ``create`` DDL for the
corresponding entity, but with all of the ``create`` and ``set``
keywords omitted.  The typical SDL structure is to use :ref:`module
blocks <ref_eql_sdl_modules>` with the rest of the declarations being
nested in their respective modules.

.. XXX: Move this to some other file.

.. versionadded:: 3.0

    |EdgeDB| 3.0 introduces a new SDL syntax which diverges slightly from DDL.
    The old SDL syntax is still fully supported, but the new syntax allows for
    cleaner and less verbose expression of your schemas.

    * Pointers no longer require an arrow (``->``). You may instead use a colon
      after the name of the link or property.
    * The ``link`` and ``property`` keywords are now optional for non-computed
      pointers when the target type is explicitly specified.

    That means that this type definition:

    .. code-block:: sdl

        type User {
          required property email -> str;
        }

    could be replaced with this equivalent one in |EdgeDB| 3+ / Gel:

    .. code-block:: sdl

        type User {
          required email: str;
        }

    When reading our documentation, the version selection dropdown will update
    the syntax of most SDL examples to the preferred syntax for the version
    selected. This is only true for versioned sections of the documentation.


Since SDL is declarative in nature, the specific order of
declarations of module blocks or individual items does not matter.

The built-in :ref:`migration tools<ref_cli_gel_migration>` expect
the schema to be given in SDL format. For example:

.. code-block:: sdl

    # "default" module block
    module default {
        type Movie {
            required title: str;
            # the year of release
            year: int64;
            required director: Person;
            required multi actors: Person;
        }
        type Person {
            required first_name: str;
            required last_name: str;
        }
    }

It is possible to also omit the module blocks, but then individual
declarations must use :ref:`fully-qualified names
<ref_name_resolution>` so that they can be assigned
to their respective modules. For example, the following is equivalent
to the previous migration:

.. code-block:: sdl

    # no module block
    type default::Movie {
        required title: str;
        # the year of release
        year: int64;
        required director: default::Person;
        required multi actors: default::Person;
    }
    type default::Person {
        required first_name: str;
        required last_name: str;
    }

.. toctree::
    :maxdepth: 3
    :hidden:

    objects
    scalars
    properties
    globals
    future
