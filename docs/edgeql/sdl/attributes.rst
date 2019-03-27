.. _ref_eql_sdl_schema_attributes:

==========
Attributes
==========

This section describes the SDL declarations pertaining to
:ref:`attributes <ref_datamodel_attributes>`.


Examples
--------

Declare a new attribute:

.. code-block:: sdl

    abstract attribute admin_note;

Specify the value of an attribute for a type:

.. code-block:: sdl

    type Status {
        attribute admin_note := 'system-critical';
        required property name -> str {
            constraint exclusive
        }
    }


Syntax
------

Define a new attribute corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_schema_attributes>`.

.. sdl:synopsis::

    [ abstract ] [ inheritable ] attribute <name>
    [ "{" <attribute-declarations>; [...] "}" ] ;


Description
-----------

:sdl:synopsis:`abstract`
    If specified, the declared attribute will be *abstract*.

:sdl:synopsis:`inheritable`
    The attributes are non-inheritable by default.  That is, if a
    schema item has an attribute defined on it, the descendants of
    that schema item will not automatically inherit the attribute.
    Normal inheritance behavior can be turned on by declaring the
    attribute with the *inheritable* qualifier.

:sdl:synopsis:`<name>`
    Specifies the name of the attribute.

:sdl:synopsis:`<attribute-declarations>`
    Attributes can have attribute declarations.
