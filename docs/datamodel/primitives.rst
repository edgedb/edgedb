.. _ref_datamodel_primitives:

==========
Primitives
==========

EdgeDB has a robust type system consisting of primitive and object types.
Below is a review of EdgeDB's primitive types; later, these will be used to
declare *properties* on object types.


.. _ref_datamodel_scalars:

Scalar types
^^^^^^^^^^^^

.. include:: ../stdlib/scalar_table.rst

Custom scalar types can also be declared. For full documentation, see :ref:`SDL
> Scalar types <ref_eql_sdl_scalars>`.

.. _ref_datamodel_enums:

Enums
^^^^^

To represent an enum, declare a custom scalar that extends the abstract
:ref:`enum <ref_std_enum>` type.

.. code-block:: sdl
    :version-lt: 3.0

    scalar type Color extending enum<Red, Green, Blue>;

    type Shirt {
      property color -> Color;
    }

.. code-block:: sdl

    scalar type Color extending enum<Red, Green, Blue>;

    type Shirt {
      color: Color;
    }

.. important::

  To reference enum values inside EdgeQL queries, use dot notation, e.g.
  ``Color.Green``.

For a full reference on enum types, see the :ref:`Enum docs <ref_std_enum>`.


.. _ref_datamodel_arrays:

Arrays
^^^^^^

Arrays store zero or more primitive values of the same type in an ordered list.
Arrays cannot contain object types or other arrays.

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      property str_array -> array<str>;
      property json_array -> array<json>;

      # INVALID: arrays of object types not allowed
      # property friends -> array<Person>

      # INVALID: arrays cannot be nested
      # property nested_array -> array<array<str>>
    }

.. code-block:: sdl

    type Person {
      str_array: array<str>;
      json_array: array<json>;

      # INVALID: arrays of object types not allowed
      # friends: array<Person>

      # INVALID: arrays cannot be nested
      # nested_array: array<array<str>>
    }

For a full reference on array types, see the :ref:`Array docs <ref_std_array>`.


.. _ref_datamodel_tuples:

Tuples
^^^^^^

Like arrays, tuples are ordered sequences of primitive data. Unlike arrays,
each element of a tuple can have a distinct type. Tuple elements can be *any
type*, including primitives, objects, arrays, and other tuples.

.. code-block:: sdl
    :version-lt: 3.0

    type Person {

      property unnamed_tuple -> tuple<str, bool, int64>;
      property nested_tuple -> tuple<tuple<str, tuple<bool, int64>>>;
      property tuple_of_arrays -> tuple<array<str>, array<int64>>;

    }

.. code-block:: sdl

    type Person {

      unnamed_tuple: tuple<str, bool, int64>;
      nested_tuple: tuple<tuple<str, tuple<bool, int64>>>;
      tuple_of_arrays: tuple<array<str>, array<int64>>;

    }

Optionally, you can assign a *key* to each element of the tuple. Tuples
containing explicit keys are known as *named tuples*. You must assign keys to
all elements (or none of them).

.. code-block:: sdl
    :version-lt: 3.0

    type BlogPost {
      property metadata -> tuple<title: str, published: bool, upvotes: int64>;
    }

.. code-block:: sdl

    type BlogPost {
      metadata: tuple<title: str, published: bool, upvotes: int64>;
    }

Named and unnamed tuples are the same data structure under the hood. You can
add, remove, and change keys in a tuple type after it's been declared. For
details, see :ref:`EdgeQL > Literals > Tuples <ref_eql_literal_tuple>`.

.. important::

  When you query an *unnamed* tuple using one of EdgeQL's :ref:`client
  libraries <ref_clients_index>`, its value is converted to a list/array. When
  you fetch a named tuple, it is converted into an object/dictionary/hashmap
  depending on the language.

.. _ref_datamodel_ranges:

Ranges
^^^^^^

.. versionadded:: 2.0

Ranges represent some interval of values. The intervals can be bound or
unbound on either end. They can also be empty, containing no values. Only
some scalar types have corresponding range types:

- ``range<int32>``
- ``range<int64>``
- ``range<float32>``
- ``range<float64>``
- ``range<decimal>``
- ``range<datetime>``
- ``range<cal::local_datetime>``
- ``range<cal::local_date>``

.. code-block:: sdl
    :version-lt: 3.0

    type DieRoll {
      property values -> range<int64>;
    }

.. code-block:: sdl

    type DieRoll {
      values: range<int64>;
    }

For a full reference on ranges, functions and operators see the :ref:`Range
docs <ref_std_range>`.

Sequences
^^^^^^^^^

To represent an auto-incrementing integer property, declare a custom scalar
that extends the abstract ``sequence`` type. Creating a sequence type
initializes a global ``int64`` counter that auto-increments whenever a new
object is created. All properties that point to the same sequence type will
share the counter.

.. code-block:: sdl
    :version-lt: 3.0

    scalar type ticket_number extending sequence;
    type Ticket {
      property number -> ticket_number;
    }

.. code-block:: sdl

    scalar type ticket_number extending sequence;
    type Ticket {
      number: ticket_number;
    }

For a full reference on sequences, see the :ref:`Sequence docs
<ref_std_sequence>`.
