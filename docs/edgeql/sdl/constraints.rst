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
        expr := __subject__ >= min;
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
        [ expr := <constr-expression> ; ]
        [ errmessage := <error-message> ; ]
        [ <attribute-declarations> ]
        [ ... ]
    "}" ;

    # where <argspec> is:

    [ $<argname>: ] <argtype>


Description
-----------

:sdl:synopsis:`abstract`
    If specified, the declared constraint will be *abstract*.

:sdl:synopsis:`delegated`
    If specified, the constraint is defined as *delegated*, which means
    that it will not be enforced on the type it's declared on, and
    the enforcement will be delegated to the subtypes of this type.
    This is particularly useful for :eql:constraint:`exclusive`
    constraints in abstract types.

:sdl:synopsis:`<name>`
    The name (optionally module-qualified) of the new constraint.

:sdl:synopsis:`<argspec>`
    An optional list of constraint arguments.
    :sdl:synopsis:`<argname>` optionally specifies
    the argument name, and :sdl:synopsis:`<argtype>`
    specifies the argument type.

:sdl:synopsis:`on ( <subject-expr> )`
    An optional expression defining the *subject* of the constraint.
    If not specified, the subject is the value of the schema item on
    which the concrete constraint is defined.  The expression must
    refer to the original subject of the constraint as
    ``__subject__``.  Note also that ``<subject-expr>`` itself has to
    be parenthesized.

    .. note::

        Currently EdgeDB only supports constraint expressions on scalar
        types and properties.

:sdl:synopsis:`extending <base> [, ...]`
    If specified, declares the *parent* constraints for this constraint.

:sdl:synopsis:`expr := <constr_expression>`
    A boolean expression that returns ``true`` for valid data and
    ``false`` for invalid data.  The expression may refer to the subject
    of the constraint as ``__subject__``.

:sdl:synopsis:`errmessage := <error_message>`
    An optional string literal defining the error message template that
    is raised when the constraint is violated.  The template is a formatted
    string that may refer to constraint context variables in curly braces.
    The template may refer to the following:

    - ``$argname`` -- the value of the specified constraint argument
    - ``__subject__`` -- the value of the ``title`` attribute of the scalar
      type, property or link on which the constraint is defined.

:sdl:synopsis:`<attribute-declarations>`
    :ref:`Schema attribute <ref_eql_sdl_schema_attributes>` declarations.
