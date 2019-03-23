.. _ref_eql_sdl_schema_attributes:

=================
Schema Attributes
=================

This section describes the SDL declarations pertaining to
:ref:`schema attributes <ref_datamodel_attributes>`.

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
