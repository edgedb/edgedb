.. _ref_datamodel_props:

==========
Properties
==========

:index: property

Properties are used to associate primitive data with an :ref:`object type
<ref_datamodel_object_types>` or :ref:`link <ref_datamodel_link_properties>`.


.. code-block:: sdl
    :version-lt: 3.0

    type Player {
      property email -> str;
      property points -> int64;
      property is_online -> bool;
    }

.. code-block:: sdl

    type Player {
      email: str;
      points: int64;
      is_online: bool;
    }

Properties are associated with a *key* (e.g. ``first_name``) and a primitive
type (e.g. ``str``). The term *primitive type* is an umbrella term that
encompasses :ref:`scalar types <ref_datamodel_scalars>` like ``str`` and
``bool``, :ref:`enums <ref_datamodel_enums>`, :ref:`arrays
<ref_datamodel_arrays>`, and :ref:`tuples <ref_datamodel_tuples>`.


Required properties
-------------------

Properties can be either ``optional`` (the default) or ``required``.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property email -> str;
    }

.. code-block:: sdl

    type User {
      required email: str;
    }

.. _ref_datamodel_props_cardinality:

Property cardinality
--------------------

Properties have a **cardinality**, either ``single`` (the default) or
``multi``. A ``multi`` property of type ``str`` points to an *unordered set* of
strings.

.. code-block:: sdl
    :version-lt: 3.0

    type User {

      # single isn't necessary here
      # properties are single by default
      single property name -> str;

      # an unordered set of strings
      multi property nicknames -> str;

      # an unordered set of string arrays
      multi property set_of_arrays -> array<str>;
    }

.. code-block:: sdl

    type User {

      # single isn't necessary here
      # properties are single by default
      single name: str;

      # an unordered set of strings
      multi nicknames: str;

      # an unordered set of string arrays
      multi set_of_arrays: array<str>;
    }

**Comparison to arrays**

The values associated with a ``multi`` property are stored in no
particular order. If order is important, use an :ref:`array
<ref_datamodel_arrays>`. Otherwise, ``multi`` properties are recommended. For a
more involved discussion, see :ref:`EdgeQL > Sets
<ref_eql_set_array_conversion>`.

.. _ref_datamodel_props_default_values:

Default values
--------------

Properties can have a default value. This default can be a static value or an
arbitrary EdgeQL expression, which will be evaluated upon insertion.

.. code-block:: sdl
    :version-lt: 3.0

    type Player {
      required property points -> int64 {
        default := 0;
      }

      required property latitude -> float64 {
        default := (360 * random() - 180);
      }
    }

.. code-block:: sdl

    type Player {
      required points: int64 {
        default := 0;
      }

      required latitude: float64 {
        default := (360 * random() - 180);
      }
    }

Readonly properties
-------------------

Properties can be marked as ``readonly``. In the example below, the
``User.external_id`` property can be set at the time of creation but not
modified thereafter.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property external_id -> uuid {
        readonly := true;
      }
    }

.. code-block:: sdl

    type User {
      required external_id: uuid {
        readonly := true;
      }
    }

Constraints
-----------

Properties can be augmented wth constraints. The example below showcases a
subset of EdgeDB's built-in constraints.

.. code-block:: sdl
    :version-lt: 3.0

    type BlogPost {
      property title -> str {
        constraint exclusive; # all post titles must be unique
        constraint min_len_value(8);
        constraint max_len_value(30);
        constraint regexp(r'^[A-Za-z0-9 ]+$');
      }

      property status -> str {
        constraint one_of('Draft', 'InReview', 'Published');
      }

      property upvotes -> int64 {
        constraint min_value(0);
        constraint max_value(9999);
      }
    }

.. code-block:: sdl

    type BlogPost {
      title: str {
        constraint exclusive; # all post titles must be unique
        constraint min_len_value(8);
        constraint max_len_value(30);
        constraint regexp(r'^[A-Za-z0-9 ]+$');
      }

      status: str {
        constraint one_of('Draft', 'InReview', 'Published');
      }

      upvotes: int64 {
        constraint min_value(0);
        constraint max_value(9999);
      }
    }

You can constrain properties with arbitrary :ref:`EdgeQL <ref_edgeql>`
expressions returning ``bool``. To reference the value of the property, use the
special scope keyword ``__subject__``.

.. code-block:: sdl
    :version-lt: 3.0

    type BlogPost {
      property title -> str {
        constraint expression on (
          __subject__ = str_trim(__subject__)
        );
      }
    }

.. code-block:: sdl

    type BlogPost {
      title: str {
        constraint expression on (
          __subject__ = str_trim(__subject__)
        );
      }
    }

The constraint above guarantees that ``BlogPost.title`` doesn't contain any
leading or trailing whitespace by checking that the raw string is equal to the
trimmed version. It uses the built-in :eql:func:`str_trim` function.

For a full reference of built-in constraints, see the :ref:`Constraints
reference <ref_std_constraints>`.


Annotations
-----------

Properties can contain annotations, small human-readable notes. The built-in
annotations are ``title``, ``description``, and ``deprecated``. You may also
declare :ref:`custom annotation types <ref_datamodel_inheritance_annotations>`.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      property email -> str {
        annotation title := 'Email address';
        annotation description := "The user's email address.";
        annotation deprecated := 'Use NewUser instead.';
      }
    }

.. code-block:: sdl

    type User {
      email: str {
        annotation title := 'Email address';
        annotation description := "The user's email address.";
        annotation deprecated := 'Use NewUser instead.';
      }
    }


Abstract properties
-------------------

Properties can be *concrete* (the default) or *abstract*. Abstract properties
are declared independent of a source or target, can contain :ref:`annotations
<ref_datamodel_annotations>`, and can be marked as ``readonly``.

.. code-block:: sdl
    :version-lt: 3.0

    abstract property email_prop {
      annotation title := 'An email address';
      readonly := true;
    }

    type Student {
      # inherits annotations and "readonly := true"
      property email extending email_prop -> str;
    }

.. code-block:: sdl

    abstract property email_prop {
      annotation title := 'An email address';
      readonly := true;
    }

    type Student {
      # inherits annotations and "readonly := true"
      email: str {
        extending email_prop;
      };
    }


Link properties
---------------

Properties can also be defined on **links**. For a full guide, refer to
:ref:`Guides > Using link properties <ref_guide_linkprops>`.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Properties <ref_eql_sdl_props>`
  * - :ref:`DDL > Properties <ref_eql_ddl_props>`
  * - :ref:`Introspection > Object types
      <ref_datamodel_introspection_object_types>`
