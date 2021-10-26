.. _ref_datamodel_constraints:

===========
Constraints
===========

Constraints gives users fine-grained control over which data is considered
valid. The can be defined on a
:ref:`scalar type <ref_datamodel_scalar_types>`, an
:ref:`object type <ref_datamodel_object_types>`, a
:ref:`concrete link <ref_datamodel_links>`, or a
:ref:`concrete property <ref_datamodel_props>`.


.. important::

  Inside constraint declarations, the special keyword ``__subject__`` can used
  to reference the *value* being constrained.

Built-in constraints
--------------------

For convenience, EdgeDB provides some pre-defined constraints. Click the name
of a given constraint for the full documentation.

.. include:: ../stdlib/constraint_table.rst


Scalar type constraints
-----------------------

.. code-block:: sdl

  scalar type username extending str {
    constraint regexp(r'^[A-Za-z0-9_]{4,20}$');
  }

Note: you can't use :eql:constraint:`exclusive` constraints on custom scalar
types, as the concept of exclusivity is only defined in the context of a given
object type.


Use :eql:constraint:`expression` constraints to declare custom constraints
using arbitrary EdgeQL expressions.

.. code-block:: sdl

  scalar type title extending str {
    constraint expression on (
      __subject__ = str_trim(__subject__)
    );
  }

Property constraints
--------------------

The constraint below uses the built-in :eql:func:`len` function, which returns
the length of a string.

.. code-block:: sdl

  type User {
    required property username -> str {
      constraint expression on (len(__subject__) < 25);
    };
  }

Object type constraints
-----------------------

Constraints can be defined on object types themselves. This is useful when the
constraint logic must reference multiple links or properties.

.. important::

  Inside an object type declaration, you can omit ``__subject__`` and simple
  refer to properties with the shorthand ``.<name>`` notation.

.. code-block:: sdl

  type Vector {
    required property x -> float64;
    required property y -> float64;

    constraint expression on (
      .x ^ 2 + .y ^ 2 <= 25
    );
  }

Link constraints
----------------

When defining a constraint on a link, __subject__ refers to the *link itself*.
This is commonly used add constraints to link properties.

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


See Also
--------

Constraint
:ref:`SDL <ref_eql_sdl_constraints>`,
:ref:`DDL <ref_eql_ddl_constraints>`,
:ref:`introspection <ref_eql_introspection_constraints>`, and
constraints defined in the :ref:`standard library <ref_std_constraints>`.
