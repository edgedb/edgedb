.. _ref_datamodel_links:

=====
Links
=====

:index: link one-to-one one-to-many many-to-one many-to-many

Links define a specific relationship between two :ref:`object
types <ref_datamodel_object_types>`.

Defining links
--------------

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      link best_friend -> Person;
    }

.. code-block:: sdl

    type Person {
      best_friend: Person;
    }

Links are *directional*; they have a source (the object type on which they are
declared) and a *target* (the type they point to).

Link cardinality
----------------

All links have a cardinality: either ``single`` or ``multi``. The default is
``single`` (a "to-one" link). Use the ``multi`` keyword to declare a "to-many"
link.

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      multi link friends -> Person;
    }

.. code-block:: sdl

    type Person {
      multi friends: Person;
    }

Required links
--------------

All links are either ``optional`` or ``required``; the default is ``optional``.
Use the ``required`` keyword to declare a required link. A required link must
point to *at least one* target instance. In this scenario, every ``Person``
must have a ``best_friend``:

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      required link best_friend -> Person;
    }

.. code-block:: sdl

    type Person {
      required best_friend: Person;
    }

Links with cardinality ``multi`` can also be ``required``;
``required multi`` links must point to *at least one* target object.

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      property name -> str;
    }

    type GroupChat {
      required multi link members -> Person;
    }

.. code-block:: sdl

    type Person {
      name: str;
    }

    type GroupChat {
      required multi members: Person;
    }

In this scenario, each ``GroupChat`` must contain at least one person.
Attempting to create a ``GroupChat`` with no members would fail.

Exclusive constraints
---------------------

You can add an ``exclusive`` constraint to a link to guarantee that no other
instances can link to the same target(s).

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      property name -> str;
    }

    type GroupChat {
      required multi link members -> Person {
        constraint exclusive;
      }
    }

.. code-block:: sdl

    type Person {
      name: str;
    }

    type GroupChat {
      required multi members: Person {
        constraint exclusive;
      }
    }

In the ``GroupChat`` example, the ``GroupChat.members`` link is now
``exclusive``. Two ``GroupChat`` objects cannot link to the same ``Person``;
put differently, no ``Person`` can be a ``member`` of multiple ``GroupChat``.

.. _ref_guide_modeling_relations:

Modeling relations
------------------

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
    - ``multi``
    - Yes
  * - Many-to-one
    - ``single``
    - No
  * - Many-to-many
    - ``multi``
    - No

.. _ref_guide_many_to_one:

Many-to-one
^^^^^^^^^^^

Many-to-one relationships typically represent concepts like ownership,
membership, or hierarchies. For example, ``Person`` and ``Shirt``. One person
may own many shirts, and a shirt is (usually) owned by just one person.

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      required property name -> str
    }

    type Shirt {
      required property color -> str;
      link owner -> Person;
    }

.. code-block:: sdl

    type Person {
      required name: str
    }

    type Shirt {
      required color: str;
      owner: Person;
    }

Since links are ``single`` by default, each ``Shirt`` only corresponds to
one ``Person``. In the absence of any exclusivity constraints, multiple shirts
can link to the same ``Person``. Thus, we have a one-to-many relationship
between ``Person`` and ``Shirt``.

When fetching a ``Person``, it's possible to deeply fetch their collection of
``Shirts`` by traversing the ``Shirt.owner`` link *in reverse*. This is known
as a **backlink**; read the :ref:`select docs <ref_eql_statements_select>` to
learn more.

.. _ref_guide_one_to_many:

One-to-many
^^^^^^^^^^^

Conceptually, one-to-many and many-to-one relationships are identical; the
"directionality" of a relation is just a matter of perspective. Here, the
same "shirt owner" relationship is represented with a ``multi`` link.

.. code-block:: sdl
    :version-lt: 3.0

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

.. code-block:: sdl

    type Person {
      required name: str;
      multi shirts: Shirt {
        # ensures a one-to-many relationship
        constraint exclusive;
      }
    }

    type Shirt {
      required color: str;
    }

.. note::

  Don't forget the exclusive constraint! This is required to ensure that each
  ``Shirt`` corresponds to a single ``Person``. Without it, the relationship
  will be many-to-many.

Under the hood, a ``multi`` link is stored in an intermediate `association
table <https://en.wikipedia.org/wiki/Associative_entity>`_, whereas a
``single`` link is stored as a column in the object type where it is declared.
As a result, single links are marginally more efficient. Generally ``single``
links are recommended when modeling 1:N relations.

.. _ref_guide_one_to_one:

One-to-one
^^^^^^^^^^

Under a *one-to-one* relationship, the source object links to a single instance
of the target type, and vice versa. As an example consider a schema to
represent assigned parking spaces.

.. code-block:: sdl
    :version-lt: 3.0

    type Employee {
      required property name -> str;
      link assigned_space -> ParkingSpace {
        constraint exclusive;
      }
    }

    type ParkingSpace {
      required property number -> int64;
    }

.. code-block:: sdl

    type Employee {
      required name: str;
      assigned_space: ParkingSpace {
        constraint exclusive;
      }
    }

    type ParkingSpace {
      required number: int64;
    }

All links are ``single`` unless otherwise specified, so no ``Employee`` can
have more than one ``assigned_space``. Moreover, the
:eql:constraint:`exclusive` constraint guarantees that a given ``ParkingSpace``
can't be assigned to multiple employees at once. Together the ``single
link`` and exclusivity constraint constitute a *one-to-one* relationship.

.. _ref_guide_many_to_many:

Many-to-many
^^^^^^^^^^^^

A *many-to-many* relation is the least constrained kind of relationship. There
is no exclusivity or cardinality constraints in either direction. As an example
consider a simple app where a ``User`` can "like" their favorite ``Movies``.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property name -> str;
      multi link likes -> Movie;
    }
    type Movie {
      required property title -> str;
    }

.. code-block:: sdl

    type User {
      required name: str;
      multi likes: Movie;
    }
    type Movie {
      required title: str;
    }

A user can like multiple movies. And in the absence of an ``exclusive``
constraint, each movie can be liked by multiple users. Thus this is a
*many-to-many* relationship.


Default values
--------------

Like properties, links can declare a default value in the form of an EdgeQL
expression, which will be executed upon insertion. In the example below, new
people are automatically assigned three random friends.

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      required property name -> str;
      multi link friends -> Person {
        default := (select Person order by random() limit 3);
      }
    }

.. code-block:: sdl

    type Person {
      required name: str;
      multi friends: Person {
        default := (select Person order by random() limit 3);
      }
    }

.. _ref_datamodel_link_properties:

Link properties
---------------

Like object types, links in EdgeDB can contain **properties**. Link properties
can be used to store metadata about links, such as *when* they were created or
the *nature/strength* of the relationship.

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      property name -> str;
      multi link family_members -> Person {
        property relationship -> str;
      }
    }

.. code-block:: sdl

    type Person {
      name: str;
      multi family_members: Person {
        relationship: str;
      }
    }

Above, we model a family tree with a single ``Person`` type. The ``Person.
family_members`` link is a many-to-many relation; each ``family_members`` link
can contain a string ``relationship`` describing the relationship of the two
individuals.

Due to how they're persisted under the hood, link properties must always be
``single`` and ``optional``.

In practice, link properties are most useful with many-to-many relationships.
In that situation there's a significant difference between the *relationship*
described by the link and the *target object*. Thus it makes sense to separate
properties of the relationships and properties of the target objects. On the
other hand, for one-to-one, one-to-many, and many-to-one relationships there's
an exact correspondence between the link and one of the objects being linked.
In these situations any property of the relationship can be equally expressed
as the property of the target object (for one-to-many and one-to-one cases) or
as the property of the source object (for many-to-one and one-to-one cases).
It is generally advisable to use object properties instead of link properties
in these cases due to better ergonomics of selecting, updating, and even
casting into :eql:type:`json` when keeping all data in the same place rather
than spreading it across link and object properties.

.. note::

  For a full guide on modeling, inserting, updating, and querying link
  properties, see the :ref:`Using Link Properties <ref_guide_linkprops>`
  guide.

.. _ref_datamodel_link_deletion:

Deletion policies
-----------------

Links can declare their own **deletion policy**. There are two kinds of events
that might trigger these policies: *target deletion* and *source deletion*.

Target deletion
^^^^^^^^^^^^^^^

Target deletion policies determine what action should be taken when the
*target* of a given link is deleted. They are declared with the ``on target
delete`` clause.

.. code-block:: sdl
    :version-lt: 3.0

    type MessageThread {
      property title -> str;
    }

    type Message {
      property content -> str;
      link chat -> MessageThread {
        on target delete delete source;
      }
    }

.. code-block:: sdl

    type MessageThread {
      title: str;
    }

    type Message {
      content: str;
      chat: MessageThread {
        on target delete delete source;
      }
    }

The ``Message.chat`` link in the example uses the ``delete source`` policy.
There are 4 available target deletion policies.

- ``restrict`` (default) - Any attempt to delete the target object immediately
  raises an exception.
- ``delete source`` - when the target of a link is deleted, the source
  is also deleted. This is useful for implementing cascading deletes.

  .. note::

    There is `a limit
    <https://github.com/edgedb/edgedb/issues/3063>`_ to the depth of a deletion
    cascade due to an upstream stack size limitation.

- ``allow`` - the target object is deleted and is removed from the
  set of the link targets.
- ``deferred restrict`` - any attempt to delete the target object
  raises an exception at the end of the transaction, unless by
  that time this object is no longer in the set of link targets.

.. _ref_datamodel_links_source_deletion:

Source deletion
^^^^^^^^^^^^^^^

.. versionadded:: 2.0

Source deletion policies determine what action should be taken when the
*source* of a given link is deleted. They are declared with the ``on source
delete`` clause.

.. code-block:: sdl
    :version-lt: 3.0

    type MessageThread {
      property title -> str;
      multi link messages -> Message {
        on source delete delete target;
      }
    }

    type Message {
      property content -> str;
    }

.. code-block:: sdl

    type MessageThread {
      title: str;
      multi messages: Message {
        on source delete delete target;
      }
    }

    type Message {
      content: str;
    }

Under this policy, deleting a ``MessageThread`` will *unconditionally* delete
its ``messages`` as well.

To avoid deleting a ``Message`` that is linked to by other ``MessageThread``
objects via their ``message`` link, append ``if orphan`` to that link's
deletion policy.

.. code-block:: sdl-diff
    :version-lt: 3.0

      type MessageThread {
        property title -> str;
        multi link messages -> Message {
    -     on source delete delete target;
    +     on source delete delete target if orphan;
        }
      }

.. code-block:: sdl-diff

      type MessageThread {
        title: str;
        multi messages: Message {
    -     on source delete delete target;
    +     on source delete delete target if orphan;
        }
      }

.. note::

    Deletion policies using ``if orphan`` will result in the target being
    deleted *unless it is linked by another object via the same link the policy
    is on*. This qualifier does not apply globally across all links in the
    database or across different links even if they're on the same type. For
    example, a ``Message`` might be linked from both a ``MessageThread`` and a
    ``Channel``. If the ``MessageThread`` linking to it is deleted, the
    deletion policy would still result in the ``Message`` being deleted as long
    as no other ``MessageThread`` objects link to it on that same field. It is
    orphaned with respect to the ``MessageThread`` type's ``link`` field, even
    though it is not orphaned globally.

    Similarly, if the ``MessageThread`` had two links both linking to messages
    — maybe the existing ``messages`` link and another called ``related`` to
    link other related ``Message`` objects that are not in the thread — ``if
    orphan`` could result in linked messages being deleted even if they were
    also linked from another ``MessageThread`` object's ``related`` link
    because they were orphaned with respect to the ``messages`` link.


.. _ref_datamodel_link_polymorphic:

Polymorphic links
-----------------

Links can have ``abstract`` targets, in which case the link is considered
**polymorphic**. Consider the following schema:

.. code-block:: sdl
    :version-lt: 3.0

    abstract type Person {
      property name -> str;
    }

    type Hero extending Person {
      # additional fields
    }

    type Villain extending Person {
      # additional fields
    }

.. code-block:: sdl

    abstract type Person {
      name: str;
    }

    type Hero extending Person {
      # additional fields
    }

    type Villain extending Person {
      # additional fields
    }

The ``abstract`` type ``Person`` has two concrete subtypes: ``Hero`` and
``Villain``. Despite being abstract, ``Person`` can be used as a link target in
concrete object types.

.. code-block:: sdl
    :version-lt: 3.0

    type Movie {
      property title -> str;
      multi link characters -> Person;
    }

.. code-block:: sdl

    type Movie {
      title: str;
      multi characters: Person;
    }

In practice, the ``Movie.characters`` link can point to a ``Hero``,
``Villain``, or any other non-abstract subtype of ``Person``. For details on
how to write queries on such a link, refer to the :ref:`Polymorphic Queries
docs <ref_eql_select_polymorphic>`


Abstract links
--------------

It's possible to define ``abstract`` links that aren't tied to a particular
*source* or *target*. If you're declaring several links with the same set
of properties, annotations, constraints, or indexes, abstract links can be used
to eliminate repetitive SDL.

.. code-block:: sdl
    :version-lt: 3.0

    abstract link link_with_strength {
      property strength -> float64;
      index on (__subject__@strength);
    }

    type Person {
      multi link friends extending link_with_strength -> Person;
    }

.. code-block:: sdl

    abstract link link_with_strength {
      strength: float64;
      index on (__subject__@strength);
    }

    type Person {
      multi friends: Person {
        extending link_with_strength;
      };
    }


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Links <ref_eql_sdl_links>`
  * - :ref:`DDL > Links <ref_eql_ddl_links>`
  * - :ref:`Introspection > Object types
      <ref_eql_introspection_object_types>`
