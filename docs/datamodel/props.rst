.. _ref_datamodel_props:

==========
Properties
==========

:index: property

Properties are used to associate primitive data with an :ref:`object type
<ref_datamodel_object_types>` or :ref:`link <ref_datamodel_linkprops>`.


.. code-block:: sdl

  type Player {
    property email -> str;
    property points -> int64;
    property is_online -> bool;
  }

Similar to :ref:`links <ref_datamodel_links>`, properties have a
*source* (the object type or link on which they are defined) and a *target*
(the designated type).


Property types
--------------

Every property has a type. This can be a
:ref:`scalar type <ref_datamodel_scalar_types>`, an :ref:`array
<ref_std_array>`, a :ref:`tuple <ref_std_tuple>`, or an enum.

Scalar types
^^^^^^^^^^^^
.. include:: ../stdlib/scalar_table.rst


.. _ref_datamodel_props_array:

Arrays
^^^^^^

Arrays store zero or more *scalar* values in an ordered list. You cannot define
arrays of non-scalar types. Arrays cannot be nested.

.. code-block:: sdl

  type Person {
    property str_array -> array<str>;
    property json_array -> array<json>;

    # INVALID: arrays of object types not allowed
    # property friends -> array<Person>

    # INVALID: arrays cannot be nested
    # property nested_array -> array<array<str>>
  }

For a full reference on array types, see the :ref:`Array docs <ref_std_array>`.

Tuples
^^^^^^

Like arrays, tuples are ordered sequences of primitive data. Unlike arrays,
each element of a tuple can have a distinct type. Tuples can be nested
arbitrarily.



.. code-block:: sdl

  type Person {

    property unnamed_tuple -> tuple<str, bool, int64>;
    property nested_tuple -> tuple<tuple<str,str>, tuple<bool, int64>>;

  }

Tuple can either be *unnamed* (as above) or *named*. Each element of a named
tuple is associated with a *key*.


.. code-block:: sdl

  type BlogPost {
    property metadata -> tuple<title: str, published: bool, upvotes: int64>;
  }

.. important::

  When you query an *unnamed* tuple using one of EdgeQL's :ref:`client
  libraries <ref_clients_index>`, its value is converted to a list/array. When
  you fetch a named tuple, it is converted into an object/dictionary/hashmap
  (depending on the language).

Enums
^^^^^

To represent an enum, declare a custom scalar that extends the abstract
:ref:`enum <ref_std_enum>` type.

.. code-block:: sdl

  scalar type Color extending enum<Red, Green, Blue>;

  type Shirt {
    property color -> Color;
  }

.. important::

  To reference enum values inside EdgeQL queries, use dot notation, e.g.
  ``Color.Green``.

For a full reference on enum types, see the :ref:`Enum docs <ref_std_enum>`.

Sequences
^^^^^^^^^

To represent an auto-incrementing integer property, declare a custom scalar
that extends the abstract ``sequence`` type. Reference the :ref:`Sequence
reference <ref_std_sequence>` for details.

Syntax
------

Required properties
^^^^^^^^^^^^^^^^^^^

Properties can be either ``optional`` (the default) or ``required``.

.. code-block:: sdl

  type User {
    required property email -> str;
  }

Property cardinality
^^^^^^^^^^^^^^^^^^^^
Properties have a **cardinality**, either ``single`` (the default) or
``multi``.

.. code-block:: sdl

  type User {

    # single isn't necessary here
    # properties are single by default
    single property name -> str;

    # an unordered set of strings
    multi property nicknames -> str;

    # an unordered set of string arrays
    multi property set_of_arrays -> array<str>;
  }

The values associated with a ``multi`` property are stored in no particular
order. If order is important, use an :ref:`array
<ref_datamodel_props_array>`.

Default values
^^^^^^^^^^^^^^
Properties can have a default value. This default can be a static value or  an arbitrary EdgeQL expression, which will be evaluated upon insertion.

.. code-block:: sdl

  type Player {
    required property points -> int64 {
      default := 0;
    }

    required property latitude -> float64 {
      default := (360 * random() - 180);
    }
  }

Readonly properties
^^^^^^^^^^^^^^^^^^^
Properties can be marked as ``readonly``. Below, the ``User.external_id`` property can be set at the time of creation, but it can never be modified.

.. code-block:: sdl

  type User {
    required property external_id -> uuid {
      readonly := true;
    }
  }

Constraints
^^^^^^^^^^^
Properties can contain additional constraints. The example below showcases a subset of EdgeDB's built-in constraints.

.. code-block:: sdl

  type BlogPost {
    property title -> str {
      constraint exclusive;
      constraint min_len_value(8);
      constraint max_len_value(30);
    }

    property status -> str {
      constraint one_of('Draft', 'InReview', 'Published');
    }

    property upvotes -> int64 {
      constraint min_value(0);
      constraint max_value(9999);
    }
  }

You can constrain properties with arbitrary EdgeQL expressions.

.. code-block:: sdl

  type Movie {
    property star_rating -> int64 {
      constraint expression on (
        __subject__ = __subject__ % 5
      );
      constraint min_len_value(8);
      constraint max_len_value(30);
    }

For a full reference of built-in constraints, see the :ref:`Constraints
reference <ref_std_constraints>`.


Annotations
^^^^^^^^^^^

All properties can contain annotations. Currently support annotations are ``title``, ``description``

.. code-block:: sdl

  type User {
    property email -> str {
      annotation title := 'Email address';
      annotation description := 'The user\'s email address.';
    }
  }

Properties can contain additional metadata, including  can add an ``exclusive``
constraint to a link to guarantee that no other instances can link to the same
target(s).

Abstract properties
^^^^^^^^^^^^^^^^^^^

Properties can *concrete* (the default) or *abstract*. Abstract properties are
declared independent of a source or target. Abstract constraints can declare
:ref:`annotations <ref_datamodel_annotations>` or ``readonly`` status.

.. code-block:: sdl

  abstract property email_prop {
    annotation title := 'An email address';
    readonly := true;
  }

  type Student {
    property email extending email_prop -> str;
  }


Link properties
---------------

Properties can also be defined on **links**. For a full guide, refer to the
:ref:`Link Properties <ref_datamodel_linkprops>` docs.

See Also
--------

Property
:ref:`SDL <ref_eql_sdl_props>`,
:ref:`DDL <ref_eql_ddl_props>`,
and :ref:`introspection <ref_eql_introspection_object_types>`.
