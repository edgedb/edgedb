.. _ref_guide_modeling_relations:

==================
Modeling relations
==================

By combinining *link cardinality* and *exclusivity constraints*, we can model
every kind of relationship: one-to-one, one-to-many, many-to-one, and
many-to-many.

.. list-table::

  * - **Relation type**
    - **Cardinality**
    - **Exclusive**
  * - One-to-one
    - ``single``
    - Yes
  * - One-to-many
    - ``single``
    - No
  * - Many-to-one
    - ``multi``
    - Yes
  * - Many-to-many
    - ``multi``
    - No

Many-to-one
^^^^^^^^^^^

Many-to-one relationships typically represent concepts like ownership,
membership, or hierarchies. For example, ``Person`` and ``Shirt``. One person
may own many shirts, and a shirt is (usually) owned by just one person.

.. code-block:: sdl

  type Person {
    required property name -> str
  }

  type Shirt {
    required property color -> str;
    link owner -> Person;
  }

Remember, links are ``single`` by default, so each shirt only corresponds to
one person. In the absence of any exclusivity constraints, multiple shirts can
link to the same person. Thus, we have a one-to-many relationship between
``Person`` and ``Shirt``.

When fetching a ``Person``, it's possible to deeply fetch their collection of
``Shirts`` by traversing the ``Shirt.owner`` link *in reverse*. This is known
as a **backlink**; read the :ref:`SELECT docs <ref_eql_statements_select>` to
learn more.

One-to-many
^^^^^^^^^^^

In essence, one-to-many and many-to-one relationships are identical; the
"directionality" of a relation is just a matter of perspective. In practice,
though, there is a second way to implement such a relationship in EdgeDB.
There's no material difference between these two approaches; which one you
choose is largely a matter of taste.

.. code-block:: sdl

  type Person {
    required property name -> str;
    multi link shirts -> Shirt {
      # ensures a one-to-many relationship
      constraint exclusive;
    }
  }

  type Shirt {
    required property color -> str;
  }

Don't forget the exclusive constraint! This is required to ensure that each
``Shirt`` corresponds to a single ``Person``. Without it, the relationship will
be many-to-many.


One-to-one
^^^^^^^^^^

Under a *one-to-one* relationship, the source object links to a single intance
of the target type, and vice versa. As an example consider a schema to
represent assigned parking spaces.

.. code-block:: sdl

  type Employee {
    required property name -> str;
    link assigned_space -> ParkingSpace {
      constraint exclusive;
    }
  }

  type ParkingSpace {
    required property number -> int64;
  }

All links are ``single`` unless otherwise specified, so no ``Employee`` can
have more than one ``assigned_space``. Moreover, the
:eql:constraint:`exclusive` constraint guarantees that a given ``ParkingSpace``
can't be assigned to multiple ``Employees`` at once. Together the ``single
link`` and exclusivity constraint constitute a *one-to-one* relationship.

Many-to-many
^^^^^^^^^^^^

A *many-to-many* relation is the least constrained kind of relationship. There
is no exclusivity or cardinality constraints in either direction. As an example
consider a simple app where ``Users`` can "like" their favorite ``Movies``.

.. code-block:: sdl

  type User {
    required property name -> str;
    multi link likes -> Movie;
  }
  type Movie {
    required property title -> str;
  }

A user can like multiple movies. And in the absence of an ``exclusive``
constraint, each movie can be liked by multiple users. Thus this is a
*many-to-many* relationship.

