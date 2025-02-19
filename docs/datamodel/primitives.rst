.. _ref_datamodel_primitives:

==========
Primitives
==========

|Gel| has a robust type system consisting of primitive and object types.
types. Primitive types are used to declare *properties* on object types,
as query and function arguments, as as well as in other contexts.

.. _ref_datamodel_scalars:

Built-in scalar types
=====================

Gel comes with a range of built-in scalar types, such as:

* String: :eql:type:`str`
* Boolean: :eql:type:`bool`
* Various numeric types: :eql:type:`int16`, :eql:type:`int32`,
  :eql:type:`int64`, :eql:type:`float32`, :eql:type:`float64`, :eql:type:`bigint`, :eql:type:`decimal`
* JSON: :eql:type:`json`,
* UUID: :eql:type:`uuid`,
* Date/time: :eql:type:`datetime`, :eql:type:`duration`
  :eql:type:`cal::local_datetime`, :eql:type:`cal::local_date`,
  :eql:type:`cal::local_time`, :eql:type:`cal::relative_duration`,
  :eql:type:`cal::date_duration`
* Miscellaneous: :eql:type:`sequence`, :eql:type:`bytes`, etc.

Custom scalars
==============

You can extend built-in scalars with additional constraints or annotations.
Here's an example of a non-negative custom ``int64`` variant:

.. code-block:: sdl

    scalar type posint64 extending int64 {
        constraint min_value(0);
    }

.. _ref_datamodel_enums:

Enums
=====

Enum types are created by extending the abstract :eql:type:`enum` type, e.g.:

.. code-block:: sdl

  scalar type Color extending enum<Red, Green, Blue>;

  type Shirt {
    color: Color;
  }

which can be queries with:

.. code-block:: edgeql

  select Shirt filter .color = Color.Red;

For a full reference on enum types, see the :ref:`Enum docs <ref_std_enum>`.

.. _ref_datamodel_arrays:

Arrays
======

Arrays store zero or more primitive values of the same type in an ordered list.
Arrays cannot contain object types or other arrays, but can contain virtually
any other type.

.. code-block:: sdl

  type Person {
    str_array: array<str>;
    json_array: array<json>;
    tuple_array: array<tuple<float32, float32>>;

    # INVALID: arrays of object types not allowed:
    # friends: array<Person>

    # INVALID: arrays cannot be nested:
    # nested_array: array<array<str>>

    # VALID: arrays can contain tuples with arrays in them
    nested_array_via_tuple: array<tuple<array<str>>>
  }

Array syntax in EdgeQL is very intuitive (indexing starts at ``0``):

.. code-block:: edgeql

  select [1, 2, 3];
  select [1, 2, 3][1] = 2;  # true

For a full reference on array types, see the :ref:`Array docs <ref_std_array>`.

.. _ref_datamodel_tuples:

Tuples
======

Like arrays, tuples are ordered sequences of primitive data. Unlike arrays,
each element of a tuple can have a distinct type. Tuple elements can be *any
type*, including primitives, objects, arrays, and other tuples.

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

  type BlogPost {
    metadata: tuple<title: str, published: bool, upvotes: int64>;
  }

Named and unnamed tuples are the same data structure under the hood. You can
add, remove, and change keys in a tuple type after it's been declared. For
details, see :ref:`Tuples <ref_eql_literal_tuple>`.

.. note::

  When you query an *unnamed* tuple using one of EdgeQL's
  :ref:`client libraries <ref_clients_index>`, its value is converted to a
  list/array. When you fetch a named tuple, it is converted into an
  object/dictionary/hashmap depending on the language.

.. _ref_datamodel_ranges:

Ranges
======

Ranges represent some interval of values. The intervals can be bound or
unbound on either end. They can also be empty, containing no values. Only
some scalar types have corresponding range types:

- Numeric ranges: ``range<int32>``, ``range<int64>``, ``range<float32>``,
  ``range<float64>``, ``range<decimal>``
- Date/time ranges: ``range<datetime>``, ``range<cal::local_datetime>``,
  ``range<cal::local_date>``

Example:

.. code-block:: sdl

  type DieRoll {
    values: range<int64>;
  }

For a full reference on ranges, functions and operators see the
:ref:`Range docs <ref_std_range>`.

Sequences
=========

To represent an auto-incrementing integer property, declare a custom scalar
that extends the abstract ``sequence`` type. Creating a sequence type
initializes a global ``int64`` counter that auto-increments whenever a new
object is created. All properties that point to the same sequence type will
share the counter.

.. code-block:: sdl

  scalar type ticket_number extending sequence;
  type Ticket {
    number: ticket_number;
    rendered_number := 'TICKET-\(.number)';
  }

For a full reference on sequences, see the :ref:`Sequence docs <ref_std_sequence>`.

.. _ref_eql_sdl_scalars:
.. _ref_eql_sdl_scalars_syntax:

Declaring scalars
=================

This section describes the syntax to declare a custom scalar type in your
schema.


Syntax
------

.. sdl:synopsis::

  [abstract] scalar type <TypeName> [extending <supertype> [, ...] ]
  [ "{"
      [ <annotation-declarations> ]
      [ <constraint-declarations> ]
      ...
    "}" ]

Description
^^^^^^^^^^^

This declaration defines a new object type with the following options:

:eql:synopsis:`abstract`
    If specified, the created scalar type will be *abstract*.

:eql:synopsis:`<TypeName>`
    The name (optionally module-qualified) of the new scalar type.

:eql:synopsis:`extending <supertype>`
    Optional clause specifying the *supertype* of the new type.

    If :eql:synopsis:`<supertype>` is an
    :eql:type:`enumerated type <std::enum>` declaration then
    an enumerated scalar type is defined.

    Use of ``extending`` creates a persistent type relationship
    between the new subtype and its supertype(s).  Schema modifications
    to the supertype(s) propagate to the subtype.

The valid SDL sub-declarations are listed below:

:sdl:synopsis:`<annotation-declarations>`
    Set scalar type :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.

:sdl:synopsis:`<constraint-declarations>`
    Define a concrete :ref:`constraint <ref_eql_sdl_constraints>` for
    this scalar type.


.. _ref_eql_ddl_scalars:

DDL commands
============

This section describes the low-level DDL commands for creating, altering, and
dropping scalar types. You typically don't need to use these commands directly,
but knowing about them is useful for reviewing migrations.

Create scalar
-------------

:eql-statement:
:eql-haswith:

Define a new scalar type.

.. eql:synopsis::

  [ with <with-item> [, ...] ]
  create [abstract] scalar type <name> [ extending <supertype> ]
  [ "{" <subcommand>; [...] "}" ] ;

  # where <subcommand> is one of

    create annotation <annotation-name> := <value>
    create constraint <constraint-name> ...

Description
^^^^^^^^^^^

The command ``create scalar type`` defines a new scalar type for use in the
current |branch|.

If *name* is qualified with a module name, then the type is created
in that module, otherwise it is created in the current module.
The type name must be distinct from that of any existing schema item
in the module.

If the ``abstract`` keyword is specified, the created type will be
*abstract*.

All non-abstract scalar types must have an underlying core
implementation. For user-defined scalar types this means that
``create scalar type`` must have another non-abstract scalar type
as its *supertype*.

The most common use of ``create scalar type`` is to define a scalar
subtype with constraints.

Most sub-commands and options of this command are identical to the
:ref:`SDL scalar type declaration <ref_eql_sdl_scalars_syntax>`. The
following subcommands are allowed in the ``create scalar type`` block:

:eql:synopsis:`create annotation <annotation-name> := <value>;`
    Set scalar type's :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`create annotation` for details.

:eql:synopsis:`create constraint <constraint-name> ...`
    Define a new constraint for this scalar type.  See
    :eql:stmt:`create constraint` for details.


Examples
^^^^^^^^

Create a new non-negative integer type:

.. code-block:: edgeql

  create scalar type posint64 extending int64 {
      create constraint min_value(0);
  };

Create a new enumerated type:

.. code-block:: edgeql

  create scalar type Color
      extending enum<Black, White, Red>;


Alter scalar
------------

:eql-statement:
:eql-haswith:

Alter the definition of a scalar type.

.. eql:synopsis::

  [ with <with-item> [, ...] ]
  alter scalar type <name>
  "{" <subcommand>; [...] "}" ;

  # where <subcommand> is one of

    rename to <newname>
    extending ...
    create annotation <annotation-name> := <value>
    alter annotation <annotation-name> := <value>
    drop annotation <annotation-name>
    create constraint <constraint-name> ...
    alter constraint <constraint-name> ...
    drop constraint <constraint-name> ...

Description
^^^^^^^^^^^

The command ``alter scalar type`` changes the definition of a scalar type.
*name* must be a name of an existing scalar type, optionally qualified
with a module name.

The following subcommands are allowed in the ``alter scalar type`` block:

:eql:synopsis:`rename to <newname>;`
    Change the name of the scalar type to *newname*.

:eql:synopsis:`extending ...`
    Alter the supertype list. It works the same way as in
    :eql:stmt:`alter type`.

:eql:synopsis:`alter annotation <annotation-name>;`
    Alter scalar type :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>`
    Remove scalar type's :eql:synopsis:`<annotation-name>` from
    :eql:synopsis:`<value>`.
    See :eql:stmt:`drop annotation` for details.

:eql:synopsis:`alter constraint <constraint-name> ...`
    Alter the definition of a constraint for this scalar type. See
    :eql:stmt:`alter constraint` for details.

:eql:synopsis:`drop constraint <constraint-name>`
    Remove a constraint from this scalar type. See
    :eql:stmt:`drop constraint` for details.

All the subcommands allowed in the ``create scalar type`` block are also
valid subcommands for ``alter scalar type`` block.


Examples
^^^^^^^^

Define a new constraint on a scalar type:

.. code-block:: edgeql

  alter scalar type posint64 {
      create constraint max_value(100);
  };

Add one more label to an enumerated type:

.. code-block:: edgeql

  alter scalar type Color
      extending enum<Black, White, Red, Green>;


Drop scalar
-----------

:eql-statement:
:eql-haswith:

Remove a scalar type.

.. eql:synopsis::

  [ with <with-item> [, ...] ]
  drop scalar type <name> ;

Description
^^^^^^^^^^^

The command ``drop scalar type`` removes a scalar type.

Parameters
^^^^^^^^^^

*name*
    The name (optionally qualified with a module name) of an existing
    scalar type.

Example
^^^^^^^

Remove a scalar type:

.. code-block:: edgeql

  drop scalar type posint64;
