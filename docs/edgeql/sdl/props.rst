.. _ref_eql_sdl_props:

==========
Properties
==========

This section describes the SDL declarations pertaining to
:ref:`properties <ref_datamodel_props>`.


Examples
--------

Declare an *abstract* property "address_base" with a helpful title:

.. code-block:: sdl

    abstract property address_base {
        # declare a specific title for the link
        annotation title := 'Mailing address';
    }

Declare *concrete* properties "name" and "address" within a "User" type:

.. code-block:: sdl

    type User {
        # define concrete properties
        required property name -> str;
        property address extending address_base -> str;

        multi link friends -> User;

        index on (__subject__.name);
    }

Any time that the SDL declaration refers to an inherited property that
is being overloaded (by adding more constraints, for example), the
``overloaded`` keyword must be used. This is to prevent unintentional
overloading due to name clashes:

.. code-block:: sdl

    abstract type Named {
        property name -> str;
    }

    type User extending Named {
        # define concrete properties
        overloaded required property name -> str;
        # ... other links and properties
    }


Syntax
------

Define a new property corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_props>`.

.. sdl:synopsis::

    # Concrete property form used inside type declaration:
    [ overloaded ] [ required ] [{single | multi}] property <name>
      [ extending <base> [, ...] ] -> <type>
      [ "{"
          [ default := <expression> ; ]
          [ readonly := {true | false} ; ]
          [ <annotation-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]

    # Computable property form used inside type declaration:
    [ required ] [{single | multi}] property <name> := <expression>;

    # Abstract property form:
    abstract property [<module>::]<name> [extending <base> [, ...]]
    [ "{"
        [ readonly := {true | false} ; ]
        [ <annotation-declarations> ]
        ...
      "}" ]

Description
-----------

The core of the declaration is identical to :eql:stmt:`CREATE PROPERTY`,
while the valid SDL sub-declarations are listed below:

:sdl:synopsis:`<annotation-declarations>`
    Set property :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.

:sdl:synopsis:`<constraint-declarations>`
    Define a concrete :ref:`constraint <ref_eql_sdl_constraints>` on
    the property.
