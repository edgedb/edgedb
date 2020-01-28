.. _ref_eql_sdl_object_types:

============
Object Types
============

This section describes the SDL declarations pertaining to
:ref:`object types <ref_datamodel_object_types>`.


Example
-------

Consider a ``User`` type with a few properties:

.. code-block:: sdl

    type User {
        # define some properties and a link
        required property name -> str;
        property address -> str;

        multi link friends -> User;

        # define an index for User based on name
        index on (__subject__.name);
    }

.. _ref_eql_sdl_object_types_inheritance:

An alternative way to define the same ``User`` type could be by using
abstract types. These abstract types can then be re-used in other type
definitions as well:

.. code-block:: sdl

    abstract type Named {
        required property name -> str;
    }

    abstract type HasAddress {
        property address -> str;
    }

    type User extending Named, HasAddress {
        # define some user-specific properties and a link
        multi link friends -> User;

        # define an index for User based on name
        index on (__subject__.name);
    }

Introducing abstract types opens up the possibility of
:ref:`polymorphic queries <ref_eql_polymorphic_queries>`.


Syntax
------

Define a new object type corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_object_types>`.

.. sdl:synopsis::

    [abstract] type <TypeName> [extending <supertype> [, ...] ]
    [ "{"
        [ <annotation-declarations> ]
        [ <property-declarations> ]
        [ <link-declarations> ]
        [ <index-declarations> ]
        ...
      "}" ]

Description
-----------

The core of the declaration is identical to :eql:stmt:`CREATE TYPE`,
while the valid SDL sub-declarations are listed below:

:sdl:synopsis:`<annotation-declarations>`
    Set object type :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.

:sdl:synopsis:`<property-declarations>`
    Define a concrete :ref:`property <ref_eql_sdl_props>` for this object type.

:sdl:synopsis:`<link-declarations>`
    Define a concrete :ref:`link <ref_eql_sdl_links>` for this object type.

:sdl:synopsis:`<index-declarations>`
    Define an :ref:`index <ref_eql_sdl_indexes>` for this object type.
