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

The core of the declaration is identical to
:eql:stmt:`CREATE SCALAR TYPE`, while the valid SDL sub-declarations
are listed below:

:sdl:synopsis:`<annotation-declarations>`
    Set scalar type :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.

:sdl:synopsis:`<constraint-declarations>`
    Define a concrete :ref:`constraint <ref_eql_sdl_constraints>` for
    this scalar type.
