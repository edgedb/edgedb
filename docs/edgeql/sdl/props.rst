.. _ref_eql_sdl_props:

==========
Properties
==========

This section describes the SDL declarations pertaining to
:ref:`properties <ref_datamodel_props>`.


Examples
--------

Declare an *abstract* property "address" with a helpful title:

.. code-block:: sdl

    abstract link address {
        # declare a specific title for the link
        attribute title := 'Mailing address';
    }

Declare *concrete* properties "name" and "address" within a "User" type:

.. code-block:: sdl

    type User {
        # define concrete properties
        property name -> str;
        property address -> str;

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
      [ "{" <subcommand>; [...] "}" ] ;

    # Computable property form used inside type declaration:
    [ required ] [{single | multi}] property <name> := <expression>;

    # Abstract property form:
    abstract property [<module>::]<name> [extending <base> [, ...]]
    [ "{" <subcommand>; [...] "}" ]
