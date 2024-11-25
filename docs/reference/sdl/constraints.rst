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

        using (__subject__ >= min);
    }

Declare a *concrete* constraint on an integer type:

.. code-block:: sdl

    scalar type posint64 extending int64 {
        constraint min_value(0);
    }

Declare a *concrete* constraint on an object type:

.. code-block:: sdl
    :version-lt: 3.0

    type Vector {
        required property x -> float64;
        required property y -> float64;
        constraint expression on (
            __subject__.x^2 + __subject__.y^2 < 25
        );
    }

.. code-block:: sdl

    type Vector {
        required x: float64;
        required y: float64;
        constraint expression on (
            __subject__.x^2 + __subject__.y^2 < 25
        );
    }

.. _ref_eql_sdl_constraints_syntax:

Syntax
------

Define a constraint corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_constraints>`.

.. sdl:synopsis::

    [{abstract | delegated}] constraint <name> [ ( [<argspec>] [, ...] ) ]
        [ on ( <subject-expr> ) ]
        [ except ( <except-expr> ) ]
        [ extending <base> [, ...] ]
    "{"
        [ using <constr-expression> ; ]
        [ errmessage := <error-message> ; ]
        [ <annotation-declarations> ]
        [ ... ]
    "}" ;

    # where <argspec> is:

    [ <argname>: ] {<argtype> | <argvalue>}


Description
-----------

This declaration defines a new constraint with the following options:

:eql:synopsis:`abstract`
    If specified, the constraint will be *abstract*.

:eql:synopsis:`delegated`
    If specified, the constraint is defined as *delegated*, which
    means that it will not be enforced on the type it's declared on,
    and the enforcement will be delegated to the subtypes of this
    type. This is particularly useful for :eql:constraint:`exclusive`
    constraints in abstract types. This is only valid for *concrete
    constraints*.

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the new constraint.

:eql:synopsis:`<argspec>`
    An optional list of constraint arguments.

    For an *abstract constraint* :eql:synopsis:`<argname>` optionally
    specifies the argument name and :eql:synopsis:`<argtype>`
    specifies the argument type.

    For a *concrete constraint* :eql:synopsis:`<argname>` optionally
    specifies the argument name and :eql:synopsis:`<argvalue>`
    specifies the argument value.  The argument value specification must
    match the parameter declaration of the abstract constraint.

:eql:synopsis:`on ( <subject-expr> )`
    An optional expression defining the *subject* of the constraint.
    If not specified, the subject is the value of the schema item on
    which the concrete constraint is defined. 

    The expression must refer to the original subject of the constraint as
    ``__subject__``. The expression must be
    :ref:`Immutable <ref_reference_volatility>`, but may refer to
    ``__subject__`` and its properties and links.

    Note also that ``<subject-expr>`` itself has to
    be parenthesized.

:eql:synopsis:`except ( <exception-expr> )`
    An optional expression defining a condition to create exceptions
	to the constraint. If ``<exception-expr>`` evaluates to ``true``,
	the constraint is ignored for the current subject. If it evaluates
	to ``false`` or ``{}``, the constraint applies normally.

	``except`` may only be declared on object constraints, and is
	otherwise follows the same rules as ``on``, above.

:eql:synopsis:`extending <base> [, ...]`
    If specified, declares the *parent* constraints for this abstract
    constraint.

The valid SDL sub-declarations are listed below:

:eql:synopsis:`using <constr_expression>`
    A boolean expression that returns ``true`` for valid data and
    ``false`` for invalid data.  The expression may refer to the
    subject of the constraint as ``__subject__``. This declaration is
    only valid for *abstract constraints*.

:eql:synopsis:`errmessage := <error_message>`
    An optional string literal defining the error message template
    that is raised when the constraint is violated.  The template is a
    formatted string that may refer to constraint context variables in
    curly braces. The template may refer to the following:

    - ``$argname`` -- the value of the specified constraint argument
    - ``__subject__`` -- the value of the ``title`` annotation of the
      scalar type, property or link on which the constraint is
      defined.

    If the content of curly braces does not match any variables,
    the curly braces are emitted as-is. They can also be escaped by 
    using double curly braces.

:sdl:synopsis:`<annotation-declarations>`
    Set constraint :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Constraints <ref_datamodel_constraints>`
  * - :ref:`DDL > Constraints <ref_eql_ddl_constraints>`
  * - :ref:`Introspection > Constraints
      <ref_datamodel_introspection_constraints>`
  * - :ref:`Standard Library > Constraints <ref_std_constraints>`
  * - `Tutorial > Advanced EdgeQL > Constraints
      </tutorial/advanced-edgeql/constraints>`_
