.. _ref_datamodel_constraints:

===========
Constraints
===========

*Constraints* are an EdgeDB mechanism that provides fine-grained control
over which data is considered valid.  A constraint may be defined on a
:ref:`scalar type <ref_datamodel_scalar_types>`, a
:ref:`concrete link <ref_datamodel_links>`, or a
:ref:`concrete property <ref_datamodel_props>`.  In case of a
constraint on a scalar type, the *subjects* of the constraint are
the instances of that scalar, thus the values that the scalar can
take will be restricted.  Whereas for link or property constraints
the *subjects* are the targets of those links or properties,
restricting what objects or values those links and properties may
reference.  The *subject* of a constraint can be referred to in
the constraint expression as ``__subject__``.

Standard Constraints
====================

The standard library defines the following constraints:

.. eql:constraint:: std::one_of(VARIADIC members: anytype)

    Specifies the list of allowed values directly.

    Example:

    .. code-block:: sdl

        scalar type Status extending str {
            constraint one_of ('Open', 'Closed', 'Merged');
        }

.. eql:constraint:: std::expression on (expr)

    Arbitrary constraint expression.

    Example:

    .. code-block:: sdl

        scalar type starts_with_a extending str {
            constraint expression on (__subject__[0] = 'A');
        }

.. eql:constraint:: std::max_value(max: anytype)

    Specifies the maximum value for the subject.

    Example:

    .. code-block:: sdl

        scalar type max_100 extending int64 {
            constraint max_value(100);
        }

.. eql:constraint:: std::max_ex_value(max: anytype)

    Specifies the maximum value (as an open interval) for the subject.

    Example:

    .. code-block:: sdl

        scalar type maxex_100 extending int64 {
            constraint max_ex_value(100);
        }

.. eql:constraint:: std::max_len_value(max: int64)

    Specifies the maximum length of subject string representation.

    Example:

    .. code-block:: sdl

        scalar type Username extending str {
            constraint max_len_value(30);
        }

.. eql:constraint:: std::min_value(min: anytype)

    Specifies the minimum value for the subject.

    Example:

    .. code-block:: sdl

        scalar type non_negative extending int64 {
            constraint min_value(0);
        }

.. eql:constraint:: std::min_ex_value(min: anytype)

    Specifies the minimum value (as an open interval) for the subject.

    Example:

    .. code-block:: sdl

        scalar type positive_float extending float64 {
            constraint min_ex_value(0);
        }

.. eql:constraint:: std::min_len_value(min: int64)

    Specifies the minimum length of subject string representation.

    Example:

    .. code-block:: sdl

        scalar type four_decimal_places extending int64 {
            constraint min_len_value(4);
        }

.. eql:constraint:: std::regexp(pattern: str)

    :index: regex regexp regular

    Specifies that the string representation of the subject must match a
    regexp.

    Example:

    .. code-block:: sdl

        scalar type LettersOnly extending str {
            constraint regexp(r'[A-Za-z]*');
        }

    See :ref:`here <string_regexp>` for more details on regexp patterns.

.. eql:constraint:: std::exclusive

    Specifies that the link or property value must be exclusive (unique).

    When applied to a ``multi`` link or property, the exclusivity constraint
    guarantees that for every object, the set of values held by a link or
    property does not intersect with any other such set in any other object
    of this type.

    This constraint is only valid for concrete links and properties.
    Scalar type definitions cannot include this constraint.

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



See Also
--------

Constraint
:ref:`SDL <ref_eql_sdl_constraints>`,
:ref:`DDL <ref_eql_ddl_constraints>`,
and :ref:`introspection <ref_eql_introspection_constraints>`.
