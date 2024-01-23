.. _ref_datamodel_constraints:

===========
Constraints
===========

.. important::

  This section assumes a basic understanding of EdgeQL.

Constraints give users fine-grained control over which data is considered
valid. They can be defined on :ref:`properties <ref_datamodel_props>`,
:ref:`links <ref_datamodel_links>`, :ref:`object types
<ref_datamodel_object_types>`, and  :ref:`custom scalars
<ref_datamodel_links>`.

Below is a simple property constraint.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property username -> str {
        constraint exclusive;
      }
    }

.. code-block:: sdl

    type User {
      required username: str {
        constraint exclusive;
      }
    }

.. _ref_datamodel_constraints_builtin:

This example uses a built-in constraint, ``exclusive``. Refer to the table
below for a complete list; click the name of a given constraint for the full
documentation.

.. include:: ../stdlib/constraint_table.rst

.. _ref_datamodel_constraints_properties:

Constraints on properties
-------------------------

The ``max_len_value`` constraint below uses the built-in :eql:func:`len`
function, which returns the length of a string.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property username -> str {
        # usernames must be unique
        constraint exclusive;

        # max length (built-in)
        constraint max_len_value(25);
      };
    }

.. code-block:: sdl

    type User {
      required username: str {
        # usernames must be unique
        constraint exclusive;

        # max length (built-in)
        constraint max_len_value(25);
      };
    }

Custom constraints
^^^^^^^^^^^^^^^^^^

The ``expression`` constraint is used to define custom constraint logic. Inside
custom constraints, the keyword ``__subject__`` can used to reference the
*value* being constrained.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property username -> str {
        # max length (as custom constraint)
        constraint expression on (len(__subject__) <= 25);
      };
    }

.. code-block:: sdl

    type User {
      required username: str {
        # max length (as custom constraint)
        constraint expression on (len(__subject__) <= 25);
      };
    }

.. _ref_datamodel_constraints_objects:

Constraints on object types
---------------------------

Constraints can be defined on object types. This is useful when the
constraint logic must reference multiple links or properties.

.. important::

  Inside an object type declaration, you can omit ``__subject__`` and simply
  refer to properties with the :ref:`leading dot notation <ref_dot_notation>`
  (e.g. ``.<name>``).

.. code-block:: sdl
    :version-lt: 3.0

    type ConstrainedVector {
      required property x -> float64;
      required property y -> float64;

      constraint expression on (
        .x ^ 2 + .y ^ 2 <= 25
      );
    }

.. code-block:: sdl

    type ConstrainedVector {
      required x: float64;
      required y: float64;

      constraint expression on (
        .x ^ 2 + .y ^ 2 <= 25
      );
    }

Note that the constraint expression cannot contain arbitrary EdgeQL! Due to
how constraints are implemented, you can only reference ``single`` (non-multi)
properties and links defined on the object type.

.. code-block:: sdl
    :version-lt: 3.0

    # Not valid!
    type User {
      required property username -> str;
      multi link friends -> User;

      # ❌ constraints cannot contain paths with more than one hop
      constraint expression on ('bob' in .friends.username);
    }

.. code-block:: sdl

    # Not valid!
    type User {
      required username: str;
      multi friends: User;

      # ❌ constraints cannot contain paths with more than one hop
      constraint expression on ('bob' in .friends.username);
    }

Computed constraints
^^^^^^^^^^^^^^^^^^^^

Constraints can be defined on computed properties.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property username -> str;
      required property clean_username := str_trim(str_lower(.username));

      constraint exclusive on (.clean_username);
    }

.. code-block:: sdl
    :version-lt: 4.0

    type User {
      required username: str;
      required property clean_username := str_trim(str_lower(.username));

      constraint exclusive on (.clean_username);
    }

.. code-block:: sdl

    type User {
      required username: str;
      required clean_username := str_trim(str_lower(.username));

      constraint exclusive on (.clean_username);
    }


Composite constraints
^^^^^^^^^^^^^^^^^^^^^

To define a composite constraint, create an ``exclusive`` constraint on a
tuple of properties or links.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      property username -> str;
    }

    type BlogPost {
      property title -> str;
      link author -> User;

      constraint exclusive on ((.title, .author));
    }

.. code-block:: sdl

    type User {
      username: str;
    }

    type BlogPost {
      title: str;
      author: User;

      constraint exclusive on ((.title, .author));
    }

.. _ref_datamodel_constraints_partial:

Partial constraints
^^^^^^^^^^^^^^^^^^^

.. versionadded:: 2.0

Constraints on object types can be made partial, so that they don't apply
when some condition holds.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property username -> str;
      property deleted -> bool;

      # Usernames must be unique unless marked deleted
      constraint exclusive on (.username) except (.deleted);
    }

.. code-block:: sdl

    type User {
      required username: str;
      deleted: bool;

      # Usernames must be unique unless marked deleted
      constraint exclusive on (.username) except (.deleted);
    }


.. _ref_datamodel_constraints_links:

Constraints on links
--------------------

You can constrain links such that a given object can only be linked once by
using :eql:constraint:`exclusive`:

.. code-block:: sdl
    :version-lt: 3.0

    type User {
        required property name -> str;

        # Make sure none of the "owned" items belong
        # to any other user.
        multi link owns -> Item {
            constraint exclusive;
        }
    }

.. code-block:: sdl

    type User {
        required name: str;

        # Make sure none of the "owned" items belong
        # to any other user.
        multi owns: Item {
            constraint exclusive;
        }
    }

Link property constraints
^^^^^^^^^^^^^^^^^^^^^^^^^

You can also add constraints for :ref:`link properties
<ref_datamodel_link_properties>`:

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      property name -> str;
      multi link friends -> User {
        single property strength -> float64;
        constraint expression on (
          @strength >= 0
        );
      }
    }

.. code-block:: sdl

    type User {
      name: str;
      multi friends: User {
        strength: float64;
        constraint expression on (
          @strength >= 0
        );
      }
    }

Link ``@source`` and ``@target`` constraints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 4.0

.. note::

    ``@source`` and ``@target`` are available starting with version 4.3.

You can create a composite exclusive constraint on the object linking/linked
*and* a link property by using ``@source`` or ``@target`` respectively. Here's
a schema for a library book management app that tracks books and who has
checked them out:

.. code-block:: sdl

    type Book {
      required title: str;
    }
    type User {
      name: str;
      multi checked_out: Book {
        date: cal::local_date;
        # Ensures a given Book can be checked out
        # only once on a given day.
        constraint exclusive on ((@target, @date));
      }
    }

Here, the constraint ensures that no book can be checked out to two ``User``\s
on the same ``@date``.

In this example demonstrating ``@source``, we've created a schema to track
player picks in a color-based memory game:

.. code-block:: sdl

    type Player {
      required name: str;
      multi picks: Color {
        order: int16;
        constraint exclusive on ((@source, @order));
      }
    }
    type Color {
      required name: str;
    }

This constraint ensures that a single ``Player`` cannot pick two ``Color``\s at
the same ``@order``.

.. _ref_datamodel_constraints_scalars:

Constraints on custom scalars
-----------------------------

Custom scalar types can be constrained.

.. code-block:: sdl

  scalar type username extending str {
    constraint regexp(r'^[A-Za-z0-9_]{4,20}$');
  }

Note: you can't use :eql:constraint:`exclusive` constraints on custom scalar
types, as the concept of exclusivity is only defined in the context of a given
object type.

Use :eql:constraint:`expression` constraints to declare custom constraints
using arbitrary EdgeQL expressions. The example below uses the built-in
:eql:func:`str_trim` function.

.. code-block:: sdl

  scalar type title extending str {
    constraint expression on (
      __subject__ = str_trim(__subject__)
    );
  }


Constraints and type inheritence
--------------------------------

If you define a constraint on a type and then extend that type, the constraint
will *not* be applied individually to each extending type. Instead, it will
apply globally across all the types that inherited the constraint.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property name -> str {
        constraint exclusive;
      }
    }
    type Administrator extending User;
    type Moderator extending User;

.. code-block:: sdl

    type User {
      required name: str {
        constraint exclusive;
      }
    }
    type Administrator extending User;
    type Moderator extending User;

.. code-block:: edgeql-repl

    db> insert Administrator {
    ...   name := 'Jan'
    ... };
    {default::Administrator {id: 7aeaa146-f5a5-11ed-a598-53ddff476532}}
    db> insert Moderator {
    ...   name := 'Jan'
    ... };
    edgedb error: ConstraintViolationError: name violates exclusivity
    constraint
      Detail: value of property 'name' of object type 'default::Moderator'
      violates exclusivity constraint
    db> insert User {
    ...   name := 'Jan'
    ... };
    edgedb error: ConstraintViolationError: name violates exclusivity
    constraint
      Detail: value of property 'name' of object type 'default::User'
      violates exclusivity constraint


As this example demonstrates, this means if an object of one of the extending
types has a value for a property that is exclusive, an object of a different
extending type cannot have the same value.

If that's not what you want, you can instead delegate the constraint to the
inheriting types by prepending the ``delegated`` keyword to the constraint.
The constraint would then be applied just as if it were declared individually
on each of the inheriting types.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property name -> str {
        delegated constraint exclusive;
      }
    }
    type Administrator extending User;
    type Moderator extending User;

.. code-block:: sdl

    type User {
      required name: str {
        delegated constraint exclusive;
      }
    }
    type Administrator extending User;
    type Moderator extending User;

.. code-block:: edgeql-repl

    db> insert Administrator {
    ...   name := 'Jan'
    ... };
    {default::Administrator {id: 7aeaa146-f5a5-11ed-a598-53ddff476532}}
    db> insert User {
    ...   name := 'Jan'
    ... };
    {default::User {id: a6e3fdaf-c44b-4080-b39f-6a07496de66b}}
    db> insert Moderator {
    ...   name := 'Jan'
    ... };
    {default::Moderator {id: d3012a3f-0f16-40a8-8884-7203f393b63d}}
    db> insert Moderator {
    ...   name := 'Jan'
    ... };
    edgedb error: ConstraintViolationError: name violates exclusivity
    constraint
      Detail: value of property 'name' of object type 'default::Moderator'
      violates exclusivity constraint

With the addition of ``delegated`` to the constraints, the inserts were
successful for each of the types. In this case, we did not hit a constraint
violation until we tried to insert a second ``Moderator`` object with the same
name as the one we had just inserted.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Constraints <ref_eql_sdl_constraints>`
  * - :ref:`DDL > Constraints <ref_eql_ddl_constraints>`
  * - :ref:`Introspection > Constraints
      <ref_datamodel_introspection_constraints>`
  * - :ref:`Standard Library > Constraints <ref_std_constraints>`
  * - `Tutorial > Advanced EdgeQL > Constraints
      </tutorial/advanced-edgeql/constraints>`_
