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
    :version-lt: 3.0

    type User {
        # define some properties and a link
        required property name -> str;
        property address -> str;

        multi link friends -> User;

        # define an index for User based on name
        index on (__subject__.name);
    }

.. code-block:: sdl

    type User {
        # define some properties and a link
        required name: str;
        address: str;

        multi friends: User;

        # define an index for User based on name
        index on (__subject__.name);
    }

.. _ref_eql_sdl_object_types_inheritance:

An alternative way to define the same ``User`` type could be by using
abstract types. These abstract types can then be re-used in other type
definitions as well:

.. code-block:: sdl
    :version-lt: 3.0

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

.. code-block:: sdl

    abstract type Named {
        required name: str;
    }

    abstract type HasAddress {
        address: str;
    }

    type User extending Named, HasAddress {
        # define some user-specific properties and a link
        multi friends: User;

        # define an index for User based on name
        index on (__subject__.name);
    }

Introducing abstract types opens up the possibility of
:ref:`polymorphic queries <ref_eql_select_polymorphic>`.


.. _ref_eql_sdl_object_types_syntax:

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
        [ <constraint-declarations> ]
        [ <index-declarations> ]
        ...
      "}" ]

Description
-----------

This declaration defines a new object type with the following options:

:eql:synopsis:`abstract`
    If specified, the created type will be *abstract*.

:eql:synopsis:`<TypeName>`
    The name (optionally module-qualified) of the new type.

:eql:synopsis:`extending <supertype> [, ...]`
    Optional clause specifying the *supertypes* of the new type.

    Use of ``extending`` creates a persistent type relationship
    between the new subtype and its supertype(s).  Schema modifications
    to the supertype(s) propagate to the subtype.

    References to supertypes in queries will also include objects of
    the subtype.

    If the same *link* name exists in more than one supertype, or
    is explicitly defined in the subtype and at least one supertype,
    then the data types of the link targets must be *compatible*.
    If there is no conflict, the links are merged to form a single
    link in the new type.

These sub-declarations are allowed in the ``Type`` block:

:sdl:synopsis:`<annotation-declarations>`
    Set object type :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.

:sdl:synopsis:`<property-declarations>`
    Define a concrete :ref:`property <ref_eql_sdl_props>` for this object type.

:sdl:synopsis:`<link-declarations>`
    Define a concrete :ref:`link <ref_eql_sdl_links>` for this object type.

:sdl:synopsis:`<constraint-declarations>`
    Define a concrete :ref:`constraint <ref_eql_sdl_constraints>` for this
    object type.

:sdl:synopsis:`<index-declarations>`
    Define an :ref:`index <ref_eql_sdl_indexes>` for this object type.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Object types <ref_datamodel_object_types>`
  * - :ref:`DDL > Object types <ref_eql_ddl_object_types>`
  * - :ref:`Introspection > Object types
      <ref_datamodel_introspection_object_types>`
  * - :ref:`Cheatsheets > Object types <ref_cheatsheet_object_types>`
