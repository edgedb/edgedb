.. _ref_eql_sdl_scalars:

============
Scalar Types
============

This section describes the SDL declarations pertaining to
:ref:`scalar types <ref_datamodel_scalar_types>`.

Define a new scalar type corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_scalars>`.


.. sdl:synopsis::

    [abstract] scalar type <TypeName> [extending <supertype> [, ...] ]
    [ "{"
        [ <constraint-declarations> ]
        [ <attribute-declarations> ]
        ...
      "}" ]


Description
-----------

:sdl:synopsis:`abstract`
    If specified, the declared type will be *abstract*.

:sdl:synopsis:`<TypeName>`
    Specifies the name of the scalar type.  Customarily, scalar type names
    use the CapWords convention.

:sdl:synopsis:`extending <supertype> [, ...]`
    If specified, declares the *supertypes* of the new type.

    Use of ``extending`` creates a persistent type relationship
    between the new subtype and its supertype(s).  Schema modifications
    to the supertype(s) propagate to the subtype.

:sdl:synopsis:`<constraint-declarations>`
    :ref:`Constraint <ref_eql_sdl_constraints>` declarations.

:sdl:synopsis:`<attribute-declarations>`
    :ref:`Schema attribute <ref_eql_sdl_schema_attributes>` declarations.


Examples
--------

Create a new non-negative integer type:

.. code-block:: sdl

    scalar type posint64 extending int64 {
        constraint min_value(0);
    }
