.. _ref_datamodel_constraints:

===========
Constraints
===========

.. important::

  This section assumes a basic understanding of EdgeQL.

Constraints gives users fine-grained control over which data is considered
valid. The can be defined on :ref:`properties <ref_datamodel_props>`,
:ref:`links <ref_datamodel_links>`, :ref:`object types
<ref_datamodel_object_types>`, and  :ref:`custom scalars
<ref_datamodel_links>`.

.. _ref_datamodel_constraints_builtin:

Built-in constraints
--------------------

For convenience, EdgeDB provides some pre-defined constraints. Click the name
of a given constraint for the full documentation.

.. include:: ../stdlib/constraint_table.rst

The ``expression`` constraint is used to define custom constraint logic. Inside
custom constraints, the keyword ``__subject__`` can used to reference the
*value* being constrained.

.. _ref_datamodel_constraints_properties:

Constraints on properties
-------------------------

The constraint below uses the built-in :eql:func:`len` function, which returns
the length of a string.

.. code-block:: sdl

  type User {
    required property username -> str {
      # as custom constraint
      constraint expression on (len(__subject__) < 25);

      # with built-in
      constraint min_len_value(25);
    };
  }


.. _ref_datamodel_constraints_objects:

Constraints on object types
---------------------------

Constraints can be defined on object types. This is useful when the
constraint logic must reference multiple links or properties.

.. important::

  Inside an object type declaration, you can omit ``__subject__`` and simple
  refer to properties with the :ref:`leading dot notation <ref_dot_notation>`
  (e.g. ``.<name>``).

.. code-block:: sdl

  type ConstrainedVector {
    required property x -> float64;
    required property y -> float64;

    constraint expression on (
      .x ^ 2 + .y ^ 2 <= 25
    );
  }


.. _ref_datamodel_constraints_links:

Constraints on links
--------------------

When defining a constraint on a link, ``__subject__`` refers to the *link
itself*. This is commonly used add constraints to :ref:`link properties
<ref_datamodel_link_properties>`.

.. code-block:: sdl

  type User {
    property name -> str;
    multi link friends -> User {
      single property strength -> float64;
      constraint expression on (
        __subject__@strength >= 0
      );
    }
  }


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

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Constraints <ref_eql_sdl_constraints>`
  * - :ref:`DDL > Constraints <ref_eql_ddl_constraints>`
  * - :ref:`Introspection > Constraints <ref_eql_introspection_constraints>`
  * - :ref:`Standard Library > Constraints <ref_std_constraints>`
