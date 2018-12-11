.. _ref_datamodel_constraints:

===========
Constraints
===========

*Constraints* are an EdgeDB mechanism that provides fine-grained control
over which data is considered valid.  A constraint may be defined on a
:ref:`scalar type <ref_datamodel_scalar_types>`, a
:ref:`concrete link <ref_datamodel_links_concrete>`, or a
:ref:`concrete property <ref_datamodel_props_concrete>`.


Definition
==========


Abstract Constraints
--------------------

An *abstract constraint* may be defined in EdgeDB Schema using the
``abstract constraint`` declaration:

.. eschema:synopsis::

    abstract constraint <constr-name> [( [<argspec>] [, ...] )]
            [on (<subject-expr>)]
            [extending [(] <parent-constr>, [, ...] [)] ]:
        [ expr := <constr-expression> ]
        [ errmessage := <error-message> ]
        [ <attribute-declarations> ]

    # where <argspec> is:

    [ $<argname>: ] <argtype>


Parameters
~~~~~~~~~~

:eschema:synopsis:`<constr-name>`
    The name of the constraint.

:eschema:synopsis:`<argspec>`
    An optional list of constraint arguments.
    :eschema:synopsis:`<argname>` optionally specifies
    the argument name, and :eschema:synopsis:`<argtype>`
    specifies the argument type.

:eschema:synopsis:`<subject-expr>`
    An optional expression defining the *subject* of the constraint.
    If not specified, the subject is the value of the schema item on
    which the constraint is defined.

:eschema:synopsis:`extending <parent_constr> [, ...]`
    If specified, declares the *parent* constraints for this constraint.

:eschema:synopsis:`expr := <constr_expression>`
    An boolean expression that returns ``true`` for valid data and
    ``false`` for invalid data.  The expression may refer to special
    variables: ``__self__`` for the value of the scalar type, link or
    property value; and ``__subject__`` which is the constraint's subject
    expression as defined by :eschema:synopsis:`<subject-expr>`.

:eschema:synopsis:`errmessage := <error_message>`
    An optional string literal defining the error message template that
    is raised when the constraint is violated.  The template is a formatted
    string that may refer to constraint context variables in curly braces.
    The template may refer to the following:

    - ``$argname`` -- the value of the specified constraint argument
    - ``__self__`` -- the value of the ``title`` attribute of the scalar type,
      property or link on which the constraint is defined.

:eschema:synopsis:`<attribute_declarations>`
    :ref:`Schema attribute <ref_datamodel_attributes>` declarations.


Concrete Constraints
--------------------

A *concrete constraint* may be defined in EdgeDB Schema using the
``constraint`` declaration in the context of a ``scalar type``, ``property``,
or ``link`` declaration:

.. eschema:synopsis::

    { scalar type | type | abstract link } <subject-item>:
        <constraint-declaration>

    type <TypeName>:
        { link | property } <link-or-prop-name>:
            <constraint-declaration>

    abstract link <link-name>:
        property <prop-name>:
            <constraint-declaration>

    # where <constraint-declaration> is:

        [ delegated ] constraint <constr_name>
                [( [$<argname> := ] <argvalue> [, ...] )]
                [on (<subject-expr>)]:
            [ <attribute-declarations> ]

Parameters
~~~~~~~~~~

:eschema:synopsis:`delegated`
    If specified, the constraint is defined as *delegated*, which means
    that it will not be enforced on the type it's declared on, and
    the enforcement will be delegated to the subtypes of this type.
    This is particularly useful for ``unique`` constraints in abstract
    types.

:eschema:synopsis:`<constr_name>`
    The name of the previously defined abstract constraint.

:eschema:synopsis:`<argname>`
    The name of an argument.

:eschema:synopsis:`<argvalue>`
    The value of an argument as a literal constant of the correct type.

:eschema:synopsis:`<subject-expr>`
    An optional expression defining the *subject* of the constraint.
    If not specified, the subject is the value of the schema item on
    which the constraint is defined.

:eschema:synopsis:`<attribute-declarations>`
    :ref:`Schema attribute <ref_datamodel_attributes>` declarations.


Standard Constraints
====================

The standard library defines the following constraints:

.. eql:constraint:: std::enum(VARIADIC members: anytype)

    Specifies the list of allowed values directly.

    Example:

    .. code-block:: eschema

        scalar type status_t extending str:
            constraint enum ('Open', 'Closed', 'Merged')

.. eql:constraint:: std::expression on (expr)

    Arbitrary constraint expression.

    Example:

    .. code-block:: eschema

        scalar type starts_with_a extending str:
            constraint expression on (__subject__[0] = 'A')

.. eql:constraint:: std::max(max: anytype)

    Specifies the maximum value for the subject.

    Example:

    .. code-block:: eschema

        scalar type max_100 extending int64:
            constraint max(100)

.. eql:constraint:: std::maxexclusive(max: anytype)

    Specifies the maximum value (as an open interval) for the subject.

    Example:

    .. code-block:: eschema

        scalar type maxex_100 extending int64:
            constraint maxexclusive(100)

.. eql:constraint:: std::maxlength(max: int64)

    Specifies the maximum length of subject string representation.

    Example:

    .. code-block:: eschema

        scalar type username_t extending str:
            constraint maxlength(30)

.. eql:constraint:: std::min(min: anytype)

    Specifies the minimum value for the subject.

    Example:

    .. code-block:: eschema

        scalar type non_negative extending int64:
            constraint min(0)

.. eql:constraint:: std::minexclusive(min: anytype)

    Specifies the minimum value (as an open interval) for the subject.

    Example:

    .. code-block:: eschema

        scalar type positive_float extending float64:
            constraint minexclusive(0)

.. eql:constraint:: std::minlength(min: int64)

    Specifies the minimum length of subject string representation.

    Example:

    .. code-block:: eschema

        scalar type four_decimal_places extending int64:
            constraint minlength(4)

.. eql:constraint:: std::regexp(pattern: str)

    :index: regex regexp regular

    Specifies that the string representation of the subject must match a
    regexp.

    Example:

    .. code-block:: eschema

        scalar type letters_only_t extending str:
            constraint regexp(r'[A-Za-z]*')

.. eql:constraint:: std::unique

    Specifies that the subject value must be unique.

    ``unique`` constraints can only be defined on concrete links or properties.

    Example:

    .. code-block:: eschema

        type User:
            required property name -> str:
                constraint unique
