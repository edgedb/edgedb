.. _ref_eql_sdl_constraints:

===========
Constraints
===========

This section describes the SDL declarations pertaining to
:ref:`constraints <ref_datamodel_constraints>`.


Examples
--------

Declare an *abstract* constraint:

.. code-block:: sdl

    abstract constraint min_value(min: anytype) {
        errmessage :=
            'Minimum allowed value for {__subject__} is {min}.';

        using __subject__ >= min;
    }

Declare a *concrete* constraint on an integer type:

.. code-block:: sdl

    scalar type posint64 extending int64 {
        constraint min_value(0);
    }


Syntax
------

Define a constraint corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_constraints>`.

.. sdl:synopsis::

    [{abstract | delegated}] constraint <name> [ ( [<argspec>] [, ...] ) ]
        [ on ( <subject-expr> ) ]
        [ extending <base> [, ...] ]
    "{"
        [ using <constr-expression> ; ]
        [ errmessage := <error-message> ; ]
        [ <annotation-declarations> ]
        [ ... ]
    "}" ;

    # where <argspec> is:

    [ $<argname>: ] <argtype>


Description
-----------

The core of the declaration is identical to
:eql:stmt:`CREATE CONSTRAINT <CREATE ABSTRACT CONSTRAINT>`,
while the valid SDL sub-declarations are listed below:

:sdl:synopsis:`<annotation-declarations>`
    Set constraint :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.
