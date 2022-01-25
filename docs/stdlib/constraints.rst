.. _ref_std_constraints:

===========
Constraints
===========

.. include:: constraint_table.rst


.. eql:constraint:: std::expression on (expr)

    Arbitrary constraint expression.

    Example of using an ``expression`` constraint to create a custom
    scalar:

    .. code-block:: sdl

        scalar type starts_with_a extending str {
            constraint expression on (__subject__[0] = 'A');
        }

    Example of using an ``expression`` constraint based on a couple of
    object properties to restrict maximum magnitude for a vector:

    .. code-block:: sdl

        type Vector {
            required property x -> float64;
            required property y -> float64;
            constraint expression on (
                __subject__.x^2 + __subject__.y^2 < 25
            );
        }

.. eql:constraint:: std::one_of(variadic members: anytype)

    Specifies the list of allowed values directly.

    Example:

    .. code-block:: sdl

        scalar type Status extending str {
            constraint one_of ('Open', 'Closed', 'Merged');
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

    Sometimes it's necessary to create a type where each combination
    of properties is unique. This can be achieved by defining an
    ``exclusive`` constraint for the type, rather than on each
    property:

    .. code-block:: sdl

        type UniqueCoordinates {
            required property x -> int64;
            required property y -> int64;

            # Each combination of x and y must be unique.
            constraint exclusive on ( (.x, .y) );
        }

    In principle, many possible expressions can appear in the ``on
    (<expr>)`` clause of the ``exclusive`` constraint with a few
    caveats:

    * The expression can only contain references to the immediate
      properties or links of the type.
    * No :ref:`backlinks <ref_datamodel_links>` or long paths are
      allowed.
    * Only ``Immutable`` functions are allowed in the constraint
      expression.

    .. note::

        This constraint also has an additional effect of creating an
        implicit :ref:`index <ref_datamodel_indexes>` on the link or
        property. This means that in the above example there's no need to
        add explicit indexes for the ``name`` property.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Constraints <ref_datamodel_constraints>`
  * - :ref:`SDL > Constraints <ref_eql_sdl_constraints>`
  * - :ref:`DDL > Constraints <ref_eql_ddl_constraints>`
  * - :ref:`Introspection > Constraints <ref_eql_introspection_constraints>`
