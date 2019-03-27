.. _ref_eql_sdl_object_types:

============
Object Types
============

This section describes the SDL declarations pertaining to
:ref:`object types <ref_datamodel_object_types>`.


Example
-------

.. code-block:: sdl

    type User {
        # define some properties and a link
        property name -> str;
        property address -> str;

        multi link friends -> User;

        # define an index for User based on name
        index user_name_idx on (__subject__.name);
    }


Syntax
------

Define a new object type corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_object_types>`.

.. sdl:synopsis::

    [abstract] type <TypeName> [extending <supertype> [, ...] ]
    [ "{"
        [ <property-declarations> ]
        [ <link-declarations> ]
        [ <index-declarations> ]
        [ <attribute-declarations> ]
        ...
      "}" ]

Description
-----------

:sdl:synopsis:`abstract`
    If specified, the declared type will be *abstract*.

:sdl:synopsis:`<TypeName>`
    Specifies the name of the object type.  Customarily, object type names
    use the CapWords convention.

:sdl:synopsis:`extending <supertype> [, ...]`
    If specified, declares the *supertypes* of the new type.

    Use of ``extending`` creates a persistent type relationship
    between the new subtype and its supertype(s).  Schema modifications
    to the supertype(s) propagate to the subtype.

    References to supertypes in queries will also include objects of
    the subtype.

    If the same *link* or *property* name exists in more than one
    supertype, or is explicitly defined in the subtype and at
    least one supertype then the data types of the link targets must
    be *compatible*.  If there is no conflict, the links are merged to
    form a single link in the new type.

:sdl:synopsis:`<property-declarations>`
    :ref:`Property <ref_eql_sdl_props>` declarations.

:sdl:synopsis:`<link-declarations>`
    :ref:`Link <ref_eql_sdl_links>` declarations.

:sdl:synopsis:`<index-declarations>`
    :ref:`Index <ref_eql_sdl_indexes>` declarations.

:sdl:synopsis:`<attribute-declarations>`
    :ref:`Schema attribute <ref_eql_sdl_schema_attributes>` declarations.
