.. _ref_datamodel_inheritance:

===========
Inheritance
===========

Inheritance is a crucial aspect of schema modeling in EdgeDB. Schema items can
*extend* other item(s) of the same kind. When extending, *child* items inherit
the definition of their *parent(s)*.

You can declare ``abstract`` instances of most schema types: objects,
properties, links, constraints, and annotations.

- :ref:`Objects <ref_datamodel_inheritance_objects>`
- :ref:`Properties <ref_datamodel_inheritance_props>`
- :ref:`Links <ref_datamodel_inheritance_links>`
- :ref:`Constraints <ref_datamodel_inheritance_constraints>`
- :ref:`Annotations <ref_datamodel_inheritance_annotations>`

.. _ref_datamodel_inheritance_objects:

Object types
------------

Object types can *extend* other object types. The extending type (AKA the
*subtype*) inherits all links, properties, indexes, constraints, etc. from its
*supertypes*.

.. code-block:: sdl

  abstract type Animal {
    property species -> str;
  }

  type Dog extending Animal {
    property breed -> str;
  }

For details on querying polymorphic data, see :ref:`EdgeQL > Select >
Polymorphic queries <ref_eql_select_polymorphic>`.

.. _ref_datamodel_inheritance_multiple:

Multiple Inheritance
^^^^^^^^^^^^^^^^^^^^

Object types can :ref:`extend more
than one type <ref_eql_sdl_object_types_inheritance>` — that's called
*multiple inheritance*. This mechanism allows building complex object
types out of combinations of more basic types.

.. code-block:: sdl

  abstract type HasName {
    property first_name -> str;
    property last_name -> str;
  }

  abstract type Email {
    property email -> str;
  }

  type Person extending HasName, HasEmail {
    property profession -> str;
  }


.. _ref_datamodel_overloading:

Overloading
^^^^^^^^^^^

An object type can overload an inherited property or link. All overloaded
declarations must be prefixed with the ``overloaded`` prefix to avoid

.. code-block:: sdl

  abstract type Person {
    property name -> str;
    multi link friends -> Person;
  }

  type Student extending Person {
    overloaded property name -> str {
      constraint exclusive;
    }
    overloaded multi link friends -> Student;
  }

Overloaded fields cannot *generalize* the associated type; it can only make it
*more specific* by setting the type to a subtype of the original or adding
additional constraints.

.. _ref_datamodel_inheritance_props:

Properties
----------

Properties can be *concrete* (the default) or *abstract*. Abstract properties
are declared independent of a source or target, can contain :ref:`annotations
<ref_datamodel_annotations>`, and can be marked as ``readonly``.

.. code-block:: sdl

  abstract property title_prop {
    annotation title := 'A title.';
    readonly := false;
  }

.. _ref_datamodel_inheritance_links:

Links
-----

It's possible to define ``abstract`` links that aren't tied to a particular
*source* or *target*. Abstract links can be marked as readonly and contain
annotations, property declarations, constraints, and indexes.

.. code-block:: sdl

  abstract link link_with_strength {
    property strength -> float64;
    index on (__subject__@strength);
  }

  type Person {
    multi link friends extending link_with_strength -> Person;
  }


.. _ref_datamodel_inheritance_constraints:

Constraints
-----------


Use ``abstract`` to declare reusable, user-defined constraint types.

.. code-block:: sdl

  abstract constraint in_range(min: anyreal, max: anyreal) {
    errmessage :=
      'Value must be in range [{min}, {max}].';
    using (max > __subject__ and __subject__ >= min);
  }

  type Player {
    property points -> int64 {
      constraint in_range(0, 100);
    }
  }


.. _ref_datamodel_inheritance_annotations:

Annotations
-----------

EdgeQL supports three annotations types by default: ``title``, ``description``,
and ``deprecated``. Use ``abstract annotation`` to declare custom user-defined
annotation types.

.. code-block:: sdl

  abstract annotation admin_note;

  type Status {
    annotation admin_note := 'system-critical';
    # more properties
  }

By default, annotations defined on abstract types, properties, and links will
not be inherited by their subtypes. To override this behavior, use the
``inheritable modifier``.

.. code-block:: sdl

  abstract inheritable annotation admin_note;

