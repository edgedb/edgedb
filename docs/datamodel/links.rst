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

On the other hand, backlinks work in reverse to find objects that link to the
object, and thus assume ``multi`` as a default. Use the ``single`` keyword to
declare a "to-one" backlink.

.. code-block:: sdl
    :version-lt: 4.0

    type Author {
      link posts := .<authors[is Article];
    }

    type CompanyEmployee {
      single link company := .<employees[is Company];
    }

.. code-block:: sdl

    type Author {
      posts := .<authors[is Article];
    }

    type CompanyEmployee {
      single company := .<employees[is Company];
    }

Required links
--------------

All links are either ``optional`` or ``required``; the default is ``optional``.
Use the ``required`` keyword to declare a required link. A required link must
point to *at least one* target instance, and if the cardinality of the required
link is ``single``, it must point to *exactly one* target instance. In this
scenario, every ``Person`` must have *exactly one* ``best_friend``:

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
put differently, no ``Person`` can be a ``member`` of multiple ``GroupChat``
objects.

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

.. note::

  Choosing a link direction can be tricky when modeling these kinds of
  relationships. Should you model the relationship as one-to-many using a
  ``multi`` link, or as   many-to-one using a ``single`` link with a
  backlink to traverse in the other direction? A general rule of thumb
  in this case is as follows.

  Use a ``multi`` link if:

  - The relationship is relatively stable and thus not updated very
    frequently. For example, a list of postal addresses in a
    user profile.
  - The number of elements in the link tends to be small.

  Otherwise, prefer a single link from one object type coupled with a
  computed backlink on the other. This is marginally more efficient
  and generally recommended when modeling 1:N relations:

  .. code-block:: sdl
      :version-lt: 4.0

      type Post {
        required author: User;
      }

      type User {
        multi link posts := (.<author[is Post])
      }

  .. code-block:: sdl

      type Post {
        required author: User;
      }

      type User {
        multi posts := (.<author[is Post])
      }

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

.. note::

  Links are always distinct. That means it's not possible to link the same
  objects twice.

  .. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property name -> str;
      multi link watch_history -> Movie {
        seen_at: datetime;
      };
    }
    type Movie {
      required property title -> str;
    }

  .. code-block:: sdl

    type User {
      required name: str;
      multi watch_history: Movie {
        seen_at: datetime;
      };
    }
    type Movie {
      required title: str;
    }

  With this model it's not possible to watch the same movie twice. Instead, you
  might change your ``seen_at`` link property to an array to store multiple
  watch times.

  .. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property name -> str;
      multi link watch_history -> Movie {
        seen_at: array<datetime>;
      };
    }
    type Movie {
      required property title -> str;
    }

  .. code-block:: sdl

    type User {
      required name: str;
      multi watch_history: Movie {
        seen_at: array<datetime>;
      };
    }
    type Movie {
      required title: str;
    }

  Alternatively, the watch history could be modeled more traditionally as its
  own type.

  .. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property name -> str;
      multi link watch_history := .<user[is WatchHistory];
    }
    type Movie {
      required property title: str;
    }
    type WatchHistory {
      required link user -> User;
      required link movie -> Movie;
      property seen_at -> datetime;
    }

  .. code-block:: sdl
    :version-lt: 4.0

    type User {
      required name: str;
      multi link watch_history := .<user[is WatchHistory];
    }
    type Movie {
      required title: str;
    }
    type WatchHistory {
      required user: User;
      required movie: Movie;
      seen_at: datetime;
    }

  .. code-block:: sdl

    type User {
      required name: str;
      multi watch_history := .<user[is WatchHistory];
    }
    type Movie {
      required title: str;
    }
    type WatchHistory {
      required user: User;
      required movie: Movie;
      seen_at: datetime;
    }

  Be sure to use single links in the join table instead of a multi link
  otherwise there will be four tables in the database.

Filtering, ordering, and limiting links
---------------------------------------

The clauses ``filter``, ``order by`` and ``limit`` can be used on links
as well.

If no properties of a link are selected, you can put the relevant clauses
into the shape itself. Assuming the same schema in the previous paragraph:

.. code-block:: edgeql

    select User {
      likes order by .title desc limit 10
    };

If properties are selected on that link, then place the clauses after
the link's shape:

.. code-block:: edgeql

    select User {
      likes: {
        id,
        title
      } order by .title desc limit 10
    };


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

.. note::

    The divide between "link" and "property" is important when it comes to
    understanding what link properties can do. They are link **properties**,
    not link **links**. This means link properties can contain only primitive
    data — data of any of the :ref:`scalar types <ref_datamodel_scalars>` like
    ``str``, ``int32``, or ``bool``, :ref:`enums <ref_datamodel_enums>`,
    :ref:`arrays <ref_datamodel_arrays>`, and :ref:`tuples
    <ref_datamodel_tuples>`. They cannot contain links to other objects.

    That means this would not work:

    .. code-block::
        :version-lt: 3.0

        type Person {
          property name -> str;
          multi link friends -> Person {
            link introduced_by -> Person;
          }
        }

    .. code-block::

        type Person {
          name: str;
          multi friends: Person {
            introduced_by: Person;
          }
        }

.. note::

    Link properties cannot be made required. They are always optional.

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
as the property of the source object (for one-to-many and one-to-one cases) or
as the property of the target object (for many-to-one and one-to-one cases).
It is generally advisable to use object properties instead of link properties
in these cases due to better ergonomics of selecting, updating, and even
casting into :eql:type:`json` when keeping all data in the same place rather
than spreading it across link and object properties.


Inserting and updating link properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To add a link with a link property, add the link property to a shape on the
linked object being added. Be sure to prepend the link property's name with
``@``.

.. code-block:: edgeql

    insert Person {
      name := "Bob",
      family_members := (
        select detached Person {
          @relationship := "sister"
        }
        filter .name = "Alice"
      )
    };

The shape could alternatively be included on an insert if the object being
linked (the ``Person`` named "Alice" in this example) is being inserted as part
of the query. If the outer person ("Bob" in the example) already exists and
only the links need to be added, this can be done in an ``update`` query
instead of an ``insert`` as shown in the example above.

Updating a link's property is similar to adding a new one except that you no
longer need to select from the object type being linked: you can instead select
the existing link on the object being updated because the link has already been
established. Here, we've discovered that Alice is actually Bob's *step*-sister,
so we want to change the link property on the already-established link between
the two:

.. code-block:: edgeql

    update Person
    filter .name = "Bob"
    set {
      family_members := (
        select .family_members {
          @relationship := "step-sister"
        }
        filter .name = "Alice"
      )
    };

Using ``select .family_members`` here with the shape including the link
property allows us to modify the link property of the existing link.

.. warning::

    A link property cannot be referenced in a set union *except* in the case of
    a :ref:`for loop <ref_eql_for>`. That means this will *not* work:

    .. code-block:: edgeql

        # 🚫 Does not work
        insert Movie {
          title := 'The Incredible Hulk',
          characters := {(
              select Person {
                @character_name := 'The Hulk'
              } filter .name = 'Mark Ruffalo'
            ),
            (
              select Person {
                @character_name := 'Abomination'
              } filter .name = 'Tim Roth'
            )}
        };

    That query will produce an error: ``QueryError: invalid reference to link
    property in top level shape``

    You can use this workaround instead:

    .. code-block:: edgeql

        # ✅ Works!
        insert Movie {
          title := 'The Incredible Hulk',
          characters := assert_distinct((
            with actors := {
              ('The Hulk', 'Mark Ruffalo'),
              ('Abomination', 'Tim Roth')
            },
            for actor in actors union (
              select Person {
                @character_name := actor.0
              } filter .name = actor.1
            )
          ))
        };

    Note that we are also required to wrap the ``actors`` query with
    :eql:func:`assert_distinct` here to assure the compiler that the result set
    is distinct.


Querying link properties
^^^^^^^^^^^^^^^^^^^^^^^^

To query a link property, add the link property's name prepended with ``@`` to
a shape on the link.

.. code-block:: edgeql-repl

    db> select Person {
    ...   name,
    ...   family_members: {
    ...     name,
    ...     @relationship
    ...   }
    ... };
    {
      default::Person {name: 'Alice', family_members: {}},
      default::Person {
        name: 'Bob',
        family_members: {
          default::Person {name: 'Alice', @relationship: 'step-sister'}
        }
      },
    }

.. note::

    In the query results above, Alice appears to have no family members even
    though we know that, if she is Bob's step-sister, he must be her
    step-brother. We would need to update Alice manually before this is
    reflected in the database. Since link properties cannot be required, not
    setting one is always allowed and results in the value being the empty set
    (``{}``).

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

There are 3 available source deletion policies:

- ``allow`` - the source object is deleted and is removed from the set of the
  link's source objects.
- ``delete target`` - when the source of a link is deleted, the target
  is unconditionally deleted.
- ``delete target if orphan`` - the source object is deleted and the target
  object is unconditionally deleted unless the target object is linked to by
  another source object via the same link.

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

    The ``if orphan`` qualifier does not apply globally across all links in the
    database or across any other links even if they're from the same type.
    Deletion policies using ``if orphan`` will result in the target being
    deleted unless

    1. it is linked by another object via **the same link the policy is on**,
       or
    2. its deletion is restricted by another link's ``on target delete`` policy
       (which defaults to ``restrict`` unless otherwise specified)

    For example, a ``Message`` might be linked from both a ``MessageThread``
    and a ``Channel``, which is defined like this:

    .. code-block:: sdl

        type Channel {
          title: str;
          multi messages: Message {
            on target delete allow;
          }
        }

    If the ``MessageThread`` linking to the ``Message`` is deleted, the source
    deletion policy would still result in the ``Message`` being deleted as long
    as no other ``MessageThread`` objects link to it on their ``messages`` link
    and the deletion isn't otherwise restricted (e.g., the default policy of
    ``on target delete restrict`` has been overridden, as in the schema above).
    The object is deleted despite not being orphaned with respect to *all*
    links because it *is* orphaned with respect to the ``MessageThread`` type's
    ``messages`` field, which is the link governed by the deletion policy.

    If the ``Channel`` type's ``messages`` link had the default policy, the
    outcome would change.

    .. code-block:: sdl-diff

        type Channel {
          title: str;
          multi messages: Message {
      -     on target delete allow;
          }
        }

    With this schema change, the ``Message`` object would *not* be deleted, but
    not because the message isn't globally orphaned. Deletion would be
    prevented because of the default target deletion policy of ``restrict``
    which would now be in force on the linking ``Channel`` object's
    ``messages`` link.

    The limited scope of ``if orphan`` holds true even when the two links to an
    object are from the same type. If ``MessageThread`` had two different links
    both linking to messages — maybe the existing ``messages`` link and another
    called ``related`` used to link other related ``Message`` objects that are
    not in the thread — ``if orphan`` on a deletion policy on ``message`` could
    result in linked messages being deleted even if they were also linked from
    another ``MessageThread`` object's ``related`` link because they were
    orphaned with respect to the ``messages`` link.


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
      <ref_datamodel_introspection_object_types>`
