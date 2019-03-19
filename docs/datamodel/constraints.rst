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

.. sdl:synopsis::

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

:sdl:synopsis:`<constr-name>`
    The name of the constraint.

:sdl:synopsis:`<argspec>`
    An optional list of constraint arguments.
    :sdl:synopsis:`<argname>` optionally specifies
    the argument name, and :sdl:synopsis:`<argtype>`
    specifies the argument type.

:sdl:synopsis:`<subject-expr>`
    An optional expression defining the *subject* of the constraint.
    If not specified, the subject is the value of the schema item on
    which the constraint is defined.

:sdl:synopsis:`extending <parent_constr> [, ...]`
    If specified, declares the *parent* constraints for this constraint.

:sdl:synopsis:`expr := <constr_expression>`
    An boolean expression that returns ``true`` for valid data and
    ``false`` for invalid data.  The expression may refer to special
    variables: ``__self__`` for the value of the scalar type, link or
    property value; and ``__subject__`` which is the constraint's subject
    expression as defined by :sdl:synopsis:`<subject-expr>`.

:sdl:synopsis:`errmessage := <error_message>`
    An optional string literal defining the error message template that
    is raised when the constraint is violated.  The template is a formatted
    string that may refer to constraint context variables in curly braces.
    The template may refer to the following:

    - ``$argname`` -- the value of the specified constraint argument
    - ``__self__`` -- the value of the ``title`` attribute of the scalar type,
      property or link on which the constraint is defined.

:sdl:synopsis:`<attribute_declarations>`
    :ref:`Schema attribute <ref_datamodel_attributes>` declarations.


Concrete Constraints
--------------------

A *concrete constraint* may be defined in EdgeDB Schema using the
``constraint`` declaration in the context of a ``scalar type``, ``property``,
or ``link`` declaration:

.. sdl:synopsis::

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

:sdl:synopsis:`delegated`
    If specified, the constraint is defined as *delegated*, which means
    that it will not be enforced on the type it's declared on, and
    the enforcement will be delegated to the subtypes of this type.
    This is particularly useful for :eql:constraint:`exclusive`
    constraints in abstract types.

:sdl:synopsis:`<constr_name>`
    The name of the previously defined abstract constraint.

:sdl:synopsis:`<argname>`
    The name of an argument.

:sdl:synopsis:`<argvalue>`
    The value of an argument as a literal constant of the correct type.

:sdl:synopsis:`<subject-expr>`
    An optional expression defining the *subject* of the constraint.
    If not specified, the subject is the value of the schema item on
    which the constraint is defined.

:sdl:synopsis:`<attribute-declarations>`
    :ref:`Schema attribute <ref_datamodel_attributes>` declarations.


Standard Constraints
====================

The standard library defines the following constraints:

.. eql:constraint:: std::enum(VARIADIC members: anytype)

    Specifies the list of allowed values directly.

    Example:

    .. code-block:: sdl

        scalar type status_t extending str {
            constraint enum ('Open', 'Closed', 'Merged');
        }

.. eql:constraint:: std::expression on (expr)

    Arbitrary constraint expression.

    Example:

    .. code-block:: sdl

        scalar type starts_with_a extending str {
            constraint expression on (__subject__[0] = 'A');
        }

.. eql:constraint:: std::max(max: anytype)

    Specifies the maximum value for the subject.

    Example:

    .. code-block:: sdl

        scalar type max_100 extending int64 {
            constraint max(100);
        }

.. eql:constraint:: std::max_ex(max: anytype)

    Specifies the maximum value (as an open interval) for the subject.

    Example:

    .. code-block:: sdl

        scalar type maxex_100 extending int64 {
            constraint max_ex(100);
        }

.. eql:constraint:: std::max_len(max: int64)

    Specifies the maximum length of subject string representation.

    Example:

    .. code-block:: sdl

        scalar type username_t extending str {
            constraint max_len(30);
        }

.. eql:constraint:: std::min(min: anytype)

    Specifies the minimum value for the subject.

    Example:

    .. code-block:: sdl

        scalar type non_negative extending int64 {
            constraint min(0);
        }

.. eql:constraint:: std::min_ex(min: anytype)

    Specifies the minimum value (as an open interval) for the subject.

    Example:

    .. code-block:: sdl

        scalar type positive_float extending float64 {
            constraint min_ex(0);
        }

.. eql:constraint:: std::min_len(min: int64)

    Specifies the minimum length of subject string representation.

    Example:

    .. code-block:: sdl

        scalar type four_decimal_places extending int64 {
            constraint min_len(4);
        }

.. eql:constraint:: std::regexp(pattern: str)

    :index: regex regexp regular

    Specifies that the string representation of the subject must match a
    regexp.

    Example:

    .. code-block:: sdl

        scalar type letters_only_t extending str {
            constraint regexp(r'[A-Za-z]*');
        }

.. eql:constraint:: std::exclusive

    Specifies that the link or property value must be exclusive (unique).

    When applied to a ``multi`` link or property, the exclusivity constraint
    guarantees that for every object, the set of values held by a link or
    property does not intersect with any other such set in any other object
    of this type.

    .. note::

        ``exclusive`` constraints cannot be defined on scalar types.

    Example:

    .. code-block:: sdl

        type User {
            # Make sure user names are unique.
            required property name -> str {
                constraint exclusive;
            }

            # Make sure none of the "owned" items belong
            # to any other user.
            multi link owns -> Item {
                constraint exclusive;
            }
        }
