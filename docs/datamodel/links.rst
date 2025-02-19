.. _ref_datamodel_links:

=====
Links
=====

Links define a relationship between two
:ref:`object types <ref_datamodel_object_types>` in Gel.

Links in |Gel| are incredibly powerful and flexible. They can be used to model
relationships of any cardinality, can be traversed in both directions,
can be polymorphic, can have constraints, and many other things.


Links are directional
=====================

Links are *directional*: they have a **source** (the type on which they are
declared) and a **target** (the type they point to).

E.g. the following schema defines a link from ``Person`` to ``Person`` and
a link from ``Company`` to ``Person``:

.. code-block:: sdl

  type Person {
    link best_friend: Person;
  }

  type Company {
    multi link employees: Person;
  }

The ``employees`` link's source is ``Company`` and its target is ``Person``.

The ``link`` keyword is optional, and can be omitted.


Link cardinality
================

.. index:: single, multi

All links have a cardinality: either ``single`` or ``multi``. The default is
``single`` (a "to-one" link). Use the ``multi`` keyword to declare a "to-many"
link:

.. code-block:: sdl

  type Person {
    multi friends: Person;
  }


Required links
==============

.. index:: required, optional, not null

All links are either ``optional`` or ``required``; the default is ``optional``.
Use the ``required`` keyword to declare a required link. A required link must
point to *at least one* target instance, and if the cardinality of the required
link is ``single``, it must point to *exactly one* target instance. In this
scenario, every ``Person`` must have *exactly one* ``best_friend``:

.. code-block:: sdl

  type Person {
    required best_friend: Person;
  }

Links with cardinality ``multi`` can also be ``required``;
``required multi`` links must point to *at least one* target object:

.. code-block:: sdl

  type Person {
    name: str;
  }

  type GroupChat {
    required multi members: Person;
  }

Attempting to create a ``GroupChat`` with no members would fail.

Exclusive constraints
=====================

.. index:: constraint exclusive

You can add an ``exclusive`` constraint to a link to guarantee that no other
instances can link to the same target(s):

.. code-block:: sdl

  type Person {
    name: str;
  }

  type GroupChat {
    required multi members: Person {
      constraint exclusive;
    }
  }

With ``exclusive`` on ``GroupChat.members``, two ``GroupChat`` objects cannot
link to the same ``Person``; put differently, no ``Person`` can be a
``member`` of multiple ``GroupChat`` objects.

Backlinks
=========

.. index:: backlink

In Gel you can traverse links in reverse to find objects that link to
the object. You can do that directly in your query. E.g. for this example
schema:

.. code-block:: sdl

  type Author {
    name: str;
  }

  type Article {
    title: str;
    multi authors: Author;
  }

You can find all articles by "John Doe" by traversing the ``authors``
link in reverse:

.. code-block:: edgeql

  select Author {
    articles := .<authors[is Article]
  }
  filter .name = "John Doe";

While the ``.<authors[is Article]`` exppression looks complicated,
the syntax is easy to read once you understand the structure of it:

* ``.<`` is used to traverse a link in reverse, it's the reverse of
  the familiar ``.`` operator.

* ``authors`` is the name of the link that the type on the other side
  has to point to ``Author``.  In this case we know that ``Article``
  has a link ``authors`` to ``Author``, so we use it!

* ``[is Article]`` is a filter that ensures we only traverse links
  that point to ``Article`` objects.

If there's a backlink that you will be traversing often, you can declare it
as a computed link:

.. code-block:: sdl-diff

    type Author {
      name: str;
  +   articles := .<authors[is Article];
    }

Last point to note: **backlinks** work in reverse to find objects that link
to the object, and therefore assume ``multi`` as a default.
Use the ``single`` keyword to declare a "to-one" backlink computed link:

.. code-block:: sdl

  type CompanyEmployee {
    single company := .<employees[is Company];
  }


Default values
==============

.. index:: default

Links can declare a default value in the form of an EdgeQL expression, which
will be executed upon insertion. In this example, new people are automatically
assigned three random friends:

.. code-block:: sdl

  type Person {
    required name: str;
    multi friends: Person {
      default := (select Person order by random() limit 3);
    }
  }


Modeling relations
==================

.. index:: cardinality, one-to-one, one-to-many, many-to-one, many-to-many,
           link table, association table

By combining *link cardinality* and *exclusivity constraints*, we can model
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
-----------

Many-to-one relationships typically represent concepts like ownership,
membership, or hierarchies. For example, ``Person`` and ``Shirt``. One person
may own many shirts, and a shirt is (usually) owned by just one person.

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
``Shirts`` by traversing the ``Shirt.owner`` link *in reverse*, known as a
**backlink**. See the :ref:`select docs <ref_eql_statements_select>` to
learn more.


.. _ref_guide_one_to_many:

One-to-many
-----------

Conceptually, one-to-many and many-to-one relationships are identical; the
"directionality" is a matter of perspective. Here, the same "shirt owner"
relationship is represented with a ``multi`` link:

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

  Don't forget the ``exclusive`` constraint! Without it, the relationship
  becomes many-to-many.

Under the hood, a ``multi`` link is stored in an intermediate `association
table <https://en.wikipedia.org/wiki/Associative_entity>`_, whereas a
``single`` link is stored as a column in the object type where it is declared.

.. note::

  Choosing a link direction can be tricky. Should you model this
  relationship as one-to-many (with a ``multi`` link) or as many-to-one
  (with a ``single`` link and a backlink)? A general rule of thumb:

  - Use a ``multi`` link if the relationship is relatively stable and
    not updated frequently, and the set of related objects is typically
    small. For example, a list of postal addresses in a user profile.
  - Otherwise, prefer a single link from one object type and a computed
    backlink on the other. This can be more efficient and is generally
    recommended for 1:N relations:

  .. code-block:: sdl

    type Post {
      required author: User;
    }

    type User {
      multi posts := (.<author[is Post])
    }


.. _ref_guide_one_to_one:

One-to-one
----------

Under a *one-to-one* relationship, the source object links to a single instance
of the target type, and vice versa. As an example, consider a schema to
represent assigned parking spaces:

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
have more than one ``assigned_space``. The :eql:constraint:`exclusive`
constraint guarantees that a given ``ParkingSpace`` can't be assigned to
multiple employees. Together, these form a one-to-one relationship.


.. _ref_guide_many_to_many:

Many-to-many
------------

A *many-to-many* relation is the least constrained kind of relationship. There
is no exclusivity or cardinality constraint in either direction. As an example,
consider a simple app where a ``User`` can "like" their favorite ``Movie``:

.. code-block:: sdl

  type User {
    required name: str;
    multi likes: Movie;
  }

  type Movie {
    required title: str;
  }

A user can like multiple movies. And in the absence of an ``exclusive``
constraint, each movie can be liked by multiple users, creating a many-to-many
relationship.

.. note::

  Links are always distinct. It's not possible to link the **same** objects
  twice. For example:

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

  In this model, a user can't watch the same movie more than once (the link
  from a specific user to a specific movie can exist only once). One approach
  is to store multiple timestamps in an array on the link property:

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

  Alternatively, you might introduce a dedicated type:

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

  Remember to use **single** links in the join table so you don't end up
  with extra tables.


.. _ref_datamodel_link_properties:

Link properties
===============

.. index:: linkprops, metadata, link table

Like object types, links in Gel can contain **properties**. Link properties
can store metadata about the link, such as the *date* a link was created
or the *strength* of the relationship:

.. code-block:: sdl

  type Person {
    name: str;
    multi family_members: Person {
      relationship: str;
    }
  }

.. note::

  Link properties can only be **primitive** data (scalars, enums,
  arrays, or tuples) — *not* links to other objects. Also note that
  link properties cannot be made required. They are always optional
  by design.

Link properties are especially useful with many-to-many relationships, where
the link itself is a distinct concept with its own data. For relations
like one-to-one or one-to-many, it's often clearer to store data in the
object type itself instead of in a link property.

Read more about link properties in the :ref:`dedicated link properties article
<ref_datamodel_linkprops>`.

Inserting and updating link properties
--------------------------------------

To add a link with a link property, include the property name (prefixed by
``@``) in the shape:

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

Updating a link's property on an **existing** link is similar. You can select
the link from within the object being updated:

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

.. warning::

  A link property cannot be referenced in a set union *except* in the case of
  a :ref:`for loop <ref_eql_for>`. For instance:

  .. code-block:: edgeql

      # 🚫 Does not work
      insert Movie {
        title := 'The Incredible Hulk',
        characters := {
          (
            select Person {
              @character_name := 'The Hulk'
            }
            filter .name = 'Mark Ruffalo'
          ),
          (
            select Person {
              @character_name := 'Abomination'
            }
            filter .name = 'Tim Roth'
          )
        }
      };

  will produce an error ``QueryError: invalid reference to link property in
  top level shape``.

  One workaround is to insert them via a ``for`` loop, combined with
  :eql:func:`assert_distinct`:

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
            }
            filter .name = actor.1
          )
        ))
      };

Querying link properties
------------------------

To query a link property, add the link property's name (prefixed with ``@``)
in the shape:

.. code-block:: edgeql-repl

  db> select Person {
  ...   name,
  ...   family_members: {
  ...     name,
  ...     @relationship
  ...   }
  ... };

.. note::

  In the results above, Bob has a *step-sister* property on the link to
  Alice, but Alice does not automatically have a property describing Bob.
  Changes to link properties are not mirrored on the "backlink" side unless
  explicitly updated, because link properties cannot be required.

.. note::

  For a full guide on modeling, inserting, updating, and querying link
  properties, see the :ref:`Using Link Properties <ref_datamodel_linkprops>`
  guide.


.. _ref_datamodel_link_deletion:

Deletion policies
=================

.. index:: on target delete, on source delete, restrict, delete source, allow,
           deferred restrict, delete target, if orphan

Links can declare their own **deletion policy** for when the **target** or
**source** is deleted.

Target deletion
---------------

The clause ``on target delete`` determines the action when the target object is
deleted:

- ``restrict`` (default) — raises an exception if the target is deleted.
- ``delete source`` — deletes the source when the target is deleted (a cascade).
- ``allow`` — removes the target from the link if the target is deleted.
- ``deferred restrict`` — like ``restrict`` but defers the error until the
  end of the transaction if the object remains linked.

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


.. _ref_datamodel_links_source_deletion:

Source deletion
---------------

The clause ``on source delete`` determines the action when the **source** is
deleted:

- ``allow`` — deletes the source, removing the link to the target.
- ``delete target`` — unconditionally deletes the target as well.
- ``delete target if orphan`` — deletes the target if and only if it's no
  longer linked by any other object *via the same link*.

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

You can add ``if orphan`` if you'd like to avoid deleting a target that remains
linked elsewhere via the **same** link name.

.. code-block:: sdl-diff

    type MessageThread {
      title: str;
      multi messages: Message {
  -     on source delete delete target;
  +     on source delete delete target if orphan;
      }
    }

.. note::

  The ``if orphan`` qualifier **does not** apply globally across
  all links in the database or even all links from the same type. If another
  link *by a different name* or *with a different on-target-delete* policy
  points at the same object, it *doesn't* prevent the object from being
  considered "orphaned" for the link that includes ``if orphan``.


.. _ref_datamodel_link_polymorphic:

Polymorphic links
=================

.. index:: abstract, subtypes, polymorphic

Links can be **polymorphic**, i.e., have an ``abstract`` target. In the
example below, we have an abstract type ``Person`` with concrete subtypes
``Hero`` and ``Villain``:

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

A polymorphic link can target any non-abstract subtype:

.. code-block:: sdl

  type Movie {
    title: str;
    multi characters: Person;
  }

When querying a polymorphic link, you can filter by a specific subtype, cast
the link to a subtype, etc. See :ref:`Polymorphic Queries <ref_eql_select_polymorphic>`
for details.

Abstract links
==============

.. index:: abstract

It's possible to define ``abstract`` links that aren't tied to a particular
source or target, and then extend them in concrete object types. This can help
eliminate repetitive declarations:

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


.. _ref_eql_sdl_links_overloading:

Overloading
===========

.. index:: overloaded

When an inherited link is modified (by adding more constraints or changing its
target type, etc.), the ``overloaded`` keyword is required. This prevents
unintentional overloading due to name clashes:

.. code-block:: sdl

  abstract type Friendly {
    # this type can have "friends"
    multi friends: Friendly;
  }

  type User extending Friendly {
    # overload the link target to to be specifically User
    overloaded multi friends: User;

    # ... other links and properties
  }


.. _ref_eql_sdl_links:
.. _ref_eql_sdl_links_syntax:

Declaring links
===============

This section describes the syntax to use links in your schema.

Syntax
------

.. sdl:synopsis::

  # Concrete link form used inside type declaration:
  [ overloaded ] [{required | optional}] [{single | multi}]
    [ link ] <name> : <type>
    [ "{"
        [ extending <base> [, ...] ; ]
        [ default := <expression> ; ]
        [ readonly := {true | false} ; ]
        [ on target delete <action> ; ]
        [ on source delete <action> ; ]
        [ <annotation-declarations> ]
        [ <property-declarations> ]
        [ <constraint-declarations> ]
        ...
      "}" ]

  # Computed link form used inside type declaration:
  [{required | optional}] [{single | multi}]
    [ link ] <name> := <expression>;

  # Computed link form used inside type declaration (extended):
  [ overloaded ] [{required | optional}] [{single | multi}]
    link <name> [: <type>]
    [ "{"
        using (<expression>) ;
        [ extending <base> [, ...] ; ]
        [ <annotation-declarations> ]
        [ <constraint-declarations> ]
        ...
      "}" ]

  # Abstract link form:
  abstract link <name>
  [ "{"
      [ extending <base> [, ...] ; ]
      [ readonly := {true | false} ; ]
      [ <annotation-declarations> ]
      [ <property-declarations> ]
      [ <constraint-declarations> ]
      [ <index-declarations> ]
      ...
    "}" ]

There are several forms of link declaration, as shown in the syntax synopsis
above:

- the first form is the canonical definition form;
- the second form is used for defining a
  :ref:`computed link <ref_datamodel_computed>`;
- and the last form is used to define an abstract link.

The following options are available:

:eql:synopsis:`overloaded`
    If specified, indicates that the link is inherited and that some
    feature of it may be altered in the current object type.  It is an
    error to declare a link as *overloaded* if it is not inherited.

:eql:synopsis:`required`
    If specified, the link is considered *required* for the parent
    object type.  It is an error for an object to have a required
    link resolve to an empty value.  Child links **always** inherit
    the *required* attribute, i.e it is not possible to make a
    required link non-required by extending it.

:eql:synopsis:`optional`
    This is the default qualifier assumed when no qualifier is
    specified, but it can also be specified explicitly. The link is
    considered *optional* for the parent object type, i.e. it is
    possible for the link to resolve to an empty value.

:eql:synopsis:`multi`
    Specifies that there may be more than one instance of this link
    in an object, in other words, ``Object.link`` may resolve to a set
    of a size greater than one.

:eql:synopsis:`single`
    Specifies that there may be at most *one* instance of this link
    in an object, in other words, ``Object.link`` may resolve to a set
    of a size not greater than one.  ``single`` is assumed if nether
    ``multi`` nor ``single`` qualifier is specified.

:eql:synopsis:`extending <base> [, ...]`
    Optional clause specifying the *parents* of the new link item.

    Use of ``extending`` creates a persistent schema relationship
    between the new link and its parents.  Schema modifications
    to the parent(s) propagate to the child.

    If the same *property* name exists in more than one parent, or
    is explicitly defined in the new link and at least one parent,
    then the data types of the property targets must be *compatible*.
    If there is no conflict, the link properties are merged to form a
    single property in the new link item.

:eql:synopsis:`<type>`
    The type must be a valid :ref:`type expression <ref_eql_types>`
    denoting an object type.

The valid SDL sub-declarations are listed below:

:eql:synopsis:`default := <expression>`
    Specifies the default value for the link as an EdgeQL expression.
    The default value is used in an ``insert`` statement if an explicit
    value for this link is not specified.

    The expression must be :ref:`Stable <ref_reference_volatility>`.

:eql:synopsis:`readonly := {true | false}`
    If ``true``, the link is considered *read-only*.  Modifications
    of this link are prohibited once an object is created.  All of the
    derived links **must** preserve the original *read-only* value.

:sdl:synopsis:`<annotation-declarations>`
    Set link :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.

:sdl:synopsis:`<property-declarations>`
    Define a concrete :ref:`property <ref_eql_sdl_props>` on the link.

:sdl:synopsis:`<constraint-declarations>`
    Define a concrete :ref:`constraint <ref_eql_sdl_constraints>` on the link.

:sdl:synopsis:`<index-declarations>`
    Define an :ref:`index <ref_eql_sdl_indexes>` for this abstract
    link. Note that this index can only refer to link properties.


.. _ref_eql_ddl_links:

DDL commands
============

This section describes the low-level DDL commands for creating, altering, and
dropping links. You typically don't need to use these commands directly, but
knowing about them is useful for reviewing migrations.

Create link
-----------

:eql-statement:
:eql-haswith:

Define a new link.

.. eql:synopsis::

  [ with <with-item> [, ...] ]
  {create|alter} type <TypeName> "{"
    [ ... ]
    create [{required | optional}] [{single | multi}]
      link <name>
      [ extending <base> [, ...] ] -> <type>
      [ "{" <subcommand>; [...] "}" ] ;
    [ ... ]
  "}"

  # Computed link form:

  [ with <with-item> [, ...] ]
  {create|alter} type <TypeName> "{"
    [ ... ]
    create [{required | optional}] [{single | multi}]
      link <name> := <expression>;
    [ ... ]
  "}"

  # Abstract link form:

  [ with <with-item> [, ...] ]
  create abstract link [<module>::]<name> [extending <base> [, ...]]
  [ "{" <subcommand>; [...] "}" ]

  # where <subcommand> is one of

    set default := <expression>
    set readonly := {true | false}
    create annotation <annotation-name> := <value>
    create property <property-name> ...
    create constraint <constraint-name> ...
    on target delete <action>
    on source delete <action>
    reset on target delete
    create index on <index-expr>

Description
^^^^^^^^^^^

The combinations of ``create type ... create link`` and ``alter type ...
create link`` define a new concrete link for a given object type, in DDL form.

There are three forms of ``create link``:

1. The canonical definition form (specifying a target type).
2. The computed link form (declaring a link via an expression).
3. The abstract link form (declaring a module-level link).

Parameters
^^^^^^^^^^^

Most sub-commands and options mirror those found in the
:ref:`SDL link declaration <ref_eql_sdl_links_syntax>`. In DDL form:

- ``set default := <expression>`` specifies a default value.
- ``set readonly := {true | false}`` makes the link read-only or not.
- ``create annotation <annotation-name> := <value>`` adds an annotation.
- ``create property <property-name> ...`` defines a property on the link.
- ``create constraint <constraint-name> ...`` defines a constraint on the link.
- ``on target delete <action>`` and ``on source delete <action>`` specify
  deletion policies.
- ``reset on target delete`` resets the target deletion policy to default
  or inherited.
- ``create index on <index-expr>`` creates an index on the link.

Examples
^^^^^^^^

.. code-block:: edgeql

  alter type User {
    create multi link friends -> User
  };

.. code-block:: edgeql

  alter type User {
    create link special_group := (
      select __source__.friends
      filter .town = __source__.town
    )
  };

.. code-block:: edgeql

  create abstract link orderable {
    create property weight -> std::int64
  };

  alter type User {
    create multi link interests extending orderable -> Interest
  };


Alter link
----------

:eql-statement:
:eql-haswith:

Changes the definition of a link.

.. eql:synopsis::

  [ with <with-item> [, ...] ]
  {create|alter} type <TypeName> "{"
    [ ... ]
    alter link <name>
    [ "{" ] <subcommand>; [...] [ "}" ];
    [ ... ]
  "}"

  [ with <with-item> [, ...] ]
  alter abstract link [<module>::]<name>
  [ "{" ] <subcommand>; [...] [ "}" ];

  # where <subcommand> is one of

    set default := <expression>
    reset default
    set readonly := {true | false}
    reset readonly
    rename to <newname>
    extending ...
    set required
    set optional
    reset optionality
    set single
    set multi
    reset cardinality
    set type <typename> [using (<conversion-expr>)]
    reset type
    using (<computed-expr>)
    create annotation <annotation-name> := <value>
    alter annotation <annotation-name> := <value>
    drop annotation <annotation-name>
    create property <property-name> ...
    alter property <property-name> ...
    drop property <property-name> ...
    create constraint <constraint-name> ...
    alter constraint <constraint-name> ...
    drop constraint <constraint-name> ...
    on target delete <action>
    on source delete <action>
    create index on <index-expr>
    drop index on <index-expr>

Description
^^^^^^^^^^^

This command modifies an existing link on a type. It can also be used on
an abstract link at the module level.

Parameters
^^^^^^^^^^

- ``rename to <newname>`` changes the link's name.
- ``extending ...`` changes or adds link parents.
- ``set required`` / ``set optional`` changes the link optionality.
- ``reset optionality`` reverts optionality to default or inherited value.
- ``set single`` / ``set multi`` changes cardinality.
- ``reset cardinality`` reverts cardinality to default or inherited value.
- ``set type <typename> [using (<expr>)]`` changes the link's target type.
- ``reset type`` reverts the link's type to inherited.
- ``using (<expr>)`` changes the expression of a computed link.
- ``create annotation``, ``alter annotation``, ``drop annotation`` manage
  annotations.
- ``create property``, ``alter property``, ``drop property`` manage link
  properties.
- ``create constraint``, ``alter constraint``, ``drop constraint`` manage
  link constraints.
- ``on target delete <action>`` and ``on source delete <action>`` manage
  deletion policies.
- ``reset on target delete`` reverts the target deletion policy.
- ``create index on <index-expr>`` / ``drop index on <index-expr>`` manage
  indexes on link properties.

Examples
^^^^^^^^

.. code-block:: edgeql

  alter type User {
    alter link friends create annotation title := "Friends";
  };

.. code-block:: edgeql

  alter abstract link orderable rename to sorted;

.. code-block:: edgeql

  alter type User {
    alter link special_group using (
      # at least one of the friend's interests
      # must match the user's
      select __source__.friends
      filter .interests IN __source__.interests
    );
  };

Drop link
---------

:eql-statement:
:eql-haswith:

Removes the specified link from the schema.

.. eql:synopsis::

  [ with <with-item> [, ...] ]
  alter type <TypeName> "{"
    [ ... ]
    drop link <name>
    [ ... ]
  "}"

  [ with <with-item> [, ...] ]
  drop abstract link [<module>]::<name>

Description
^^^^^^^^^^^

- ``alter type ... drop link <name>`` removes the link from an object type.
- ``drop abstract link <name>`` removes an abstract link from the schema.

Examples
^^^^^^^^

.. code-block:: edgeql

  alter type User drop link friends;

.. code-block:: edgeql

  drop abstract link orderable;



.. list-table::
  :class: seealso

  * - **See also**
    - :ref:`Introspection > Object types <ref_datamodel_introspection_object_types>`
