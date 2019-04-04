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
        attribute title := 'Mailing address';
    }

Declare *concrete* properties "name" and "address" within a "User" type:

.. code-block:: sdl

    type User {
        # define concrete properties
        required property name -> str;
        property address extending address_base -> str;

        multi link friends -> User;

        index user_name_idx on (__subject__.name);
    }


Syntax
------

Define a new property corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_props>`.

.. sdl:synopsis::

    # Concrete property form used inside type declaration:
    [ required ] [{single | multi}] property <name>
      [ extending <base> [, ...] ] -> <type>
      [ "{"
          [ default := <expression> ; ]
          [ readonly := {true | false} ; ]
          [ <attribute-declarations> ]
          [ <constraint-declarations> ]
          ...
        "}" ]

    # Computable property form used inside type declaration:
    [ required ] [{single | multi}] property <name> := <expression>;

    # Abstract property form:
    abstract property [<module>::]<name> [extending <base> [, ...]]
    [ "{"
        [ readonly := {true | false} ; ]
        [ <attribute-declarations> ]
        ...
      "}" ]

Description
-----------

The core of the declaration is identical to :eql:stmt:`CREATE PROPERTY`,
while the valid SDL sub-declarations are listed below:

:sdl:synopsis:`<attribute-declarations>`
    Set property :ref:`attribute <ref_eql_sdl_schema_attributes>`
    to a given *value*.

:sdl:synopsis:`<constraint-declarations>`
    Define a concrete :ref:`constraint <ref_eql_sdl_constraints>` on
    the property.
