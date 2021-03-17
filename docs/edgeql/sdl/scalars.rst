.. _ref_eql_sdl_scalars:

============
Scalar Types
============

This section describes the SDL declarations pertaining to
:ref:`scalar types <ref_datamodel_scalar_types>`.


Example
-------

Declare a new non-negative integer type:

.. code-block:: sdl

    scalar type posint64 extending int64 {
        constraint min_value(0);
    }

.. _ref_eql_sdl_scalars_syntax:

Syntax
------

Define a new scalar type corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_scalars>`.

.. sdl:synopsis::

    [abstract] scalar type <TypeName> [extending <supertype> [, ...] ]
    [ "{"
        [ <annotation-declarations> ]
        [ <constraint-declarations> ]
        ...
      "}" ]


Description
-----------

This declaration defines a new object type with the following options:

:eql:synopsis:`abstract`
    If specified, the created scalar type will be *abstract*.

:eql:synopsis:`<TypeName>`
    The name (optionally module-qualified) of the new scalar type.

:eql:synopsis:`extending <supertype>`
    Optional clause specifying the *supertype* of the new type.

    If :eql:synopsis:`<supertype>` is an
    :eql:type:`enumerated type <std::enum>` declaration then
    an enumerated scalar type is defined.

    Use of ``extending`` creates a persistent type relationship
    between the new subtype and its supertype(s).  Schema modifications
    to the supertype(s) propagate to the subtype.

The valid SDL sub-declarations are listed below:

:sdl:synopsis:`<annotation-declarations>`
    Set scalar type :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.

:sdl:synopsis:`<constraint-declarations>`
    Define a concrete :ref:`constraint <ref_eql_sdl_constraints>` for
    this scalar type.
