.. _ref_std_constraints:

===========
Constraints
===========

.. include:: constraint_table.rst


.. eql:constraint:: std::expression on (expr)

    A constraint based on an arbitrary expression returning a boolean.

    The ``expression`` constraint may be used as in this example to create a
    custom scalar type:

    .. code-block:: sdl

        scalar type StartsWithA extending str {
            constraint expression on (__subject__[0] = 'A');
        }

    Example of using an ``expression`` constraint based on two
    object properties to restrict maximum magnitude for a vector:

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

.. eql:constraint:: std::one_of(variadic members: anytype)

    Specifies a list of allowed values.

    Example:

    .. code-block:: sdl

        scalar type Status extending str {
            constraint one_of ('Open', 'Closed', 'Merged');
        }

.. eql:constraint:: std::max_value(max: anytype)

    Specifies the maximum allowed value.

    Example:

    .. code-block:: sdl

        scalar type Max100 extending int64 {
            constraint max_value(100);
        }

.. eql:constraint:: std::max_ex_value(max: anytype)

    Specifies a non-inclusive upper bound for the value.

    Example:

    .. code-block:: sdl

        scalar type Under100 extending int64 {
            constraint max_ex_value(100);
        }

    In this example, in contrast to the ``max_value`` constraint, a value
    of the ``Under100`` type cannot be ``100`` since the valid range of
    ``max_ex_value`` does not include the value specified in the constraint.

.. eql:constraint:: std::max_len_value(max: int64)

    Specifies the maximum allowed length of a value.

    Example:

    .. code-block:: sdl

        scalar type Username extending str {
            constraint max_len_value(30);
        }

.. eql:constraint:: std::min_value(min: anytype)

    Specifies the minimum allowed value.

    Example:

    .. code-block:: sdl

        scalar type NonNegative extending int64 {
            constraint min_value(0);
        }

.. eql:constraint:: std::min_ex_value(min: anytype)

    Specifies a non-inclusive lower bound for the value.

    Example:

    .. code-block:: sdl

        scalar type PositiveFloat extending float64 {
            constraint min_ex_value(0);
        }

    In this example, in contrast to the ``min_value`` constraint, a value
    of the ``PositiveFloat`` type cannot be ``0`` since the valid range of
    ``mix_ex_value`` does not include the value specified in the constraint.

.. eql:constraint:: std::min_len_value(min: int64)

    Specifies the minimum allowed length of a value.

    Example:

    .. code-block:: sdl

        scalar type EmailAddress extending str {
            constraint min_len_value(3);
        }

.. eql:constraint:: std::regexp(pattern: str)

    :index: regex regexp regular

    Limits to string values matching a regular expression.

    Example:

    .. code-block:: sdl

        scalar type LettersOnly extending str {
            constraint regexp(r'[A-Za-z]*');
        }

    See our documentation on :ref:`regular expression patterns
    <string_regexp>` for more information on those.

.. eql:constraint:: std::exclusive

    Specifies that the link or property value must be exclusive (unique).

    When applied to a ``multi`` link or property, the exclusivity constraint
    guarantees that for every object, the set of values held by a link or
    property does not intersect with any other such set in any other object
    of this type.

    This constraint is only valid for concrete links and properties.
    Scalar type definitions cannot include this constraint.

    This constraint has an additional effect of creating an
    implicit :ref:`index <ref_datamodel_indexes>` on a property. 
    This means that there's no need to add explicit indexes 
    for properties with this constraint.

    Example:

    .. code-block:: sdl
        :version-lt: 3.0

        type User {
            # Make sure user names are unique.
            required property name -> str {
                constraint exclusive;
            }
            # Already indexed, don't need to do this:
            # index on (.name) 

            # Make sure none of the "owned" items belong
            # to any other user.
            multi link owns -> Item {
                constraint exclusive;
            }
        }

    .. code-block:: sdl

        type User {
            # Make sure user names are unique.
            required name: str {
                constraint exclusive;
            }
            # Already indexed, don't need to do this:
            # index on (.name)

            # Make sure none of the "owned" items belong
            # to any other user.
            multi owns: Item {
                constraint exclusive;
            }
        }

    Sometimes it may be necessary to create a type where each *combination*
    of properties is unique. This can be achieved by defining an
    ``exclusive`` constraint for the combination, rather than on each
    property:

    .. code-block:: sdl
        :version-lt: 3.0

        type UniqueCoordinates {
            required property x -> int64;
            required property y -> int64;

            # Each combination of x and y must be unique.
            constraint exclusive on ( (.x, .y) );
        }

    .. code-block:: sdl

        type UniqueCoordinates {
            required x: int64;
            required y: int64;

            # Each combination of x and y must be unique.
            constraint exclusive on ( (.x, .y) );
        }

    Any possible expression can appear in the ``on (<expr>)`` clause of
    the ``exclusive`` constraint as long as it adheres to the following:

    * The expression can only contain references to the immediate
      properties or links of the type.
    * No :ref:`backlinks <ref_datamodel_links>` or long paths are
      allowed.
    * Only ``Immutable`` functions are allowed in the constraint
      expression.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Constraints <ref_datamodel_constraints>`
  * - :ref:`SDL > Constraints <ref_eql_sdl_constraints>`
  * - :ref:`DDL > Constraints <ref_eql_ddl_constraints>`
  * - :ref:`Introspection > Constraints
      <ref_datamodel_introspection_constraints>`
  * - `Tutorial > Advanced EdgeQL > Constraints
      </tutorial/advanced-edgeql/constraints>`_
