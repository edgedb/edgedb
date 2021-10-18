.. _ref_datamodel_links:

=====
Links
=====

:index: link one-to-one one-to-many many-to-one many-to-many

Links define a specific relationship between two :ref:`object
types <ref_datamodel_object_types>`.

See the :ref:`Modeling Relations <ref_guide_modeling_relations>` guide for a
breakdown of how to model one-to-one, one-to-many, and many-to-many
relationships in EdgeDB.

Defining links
--------------

.. code-block:: sdl

  type Person {
    link best_friend -> Person;
  }

Links are *directional*; they have a source (the object type on which they are
declared) and a *target* (the type they point to).

Link cardinality
----------------

All links have a cardinality: either ``single`` or ``multi``. The default is
``single`` (a "to-one" link). Use the ``multi`` keyword to declare a "to-many"
link.

.. code-block:: sdl

  type Person {
    multi link friends -> Person;
  }

Required links
--------------

All links are either ``optional`` or ``required``; the default is ``optional``.
Use the ``required`` keyword to declare a required link. A required link must
point to *at least one* target instance. In this scenario, every ``Person``
must have a ``best_friend``.

.. code-block:: sdl

  type Person {
    required link best_friend -> Person;
  }

You can define ``required multi links`` on an object type. In this scenario,
every ``GroupChat`` must contain *at least one* user. Attempting to create a
``GroupChat`` with no users would fail.

.. code-block:: sdl

  type User {
    property name -> str;
  }

  type GroupChat {
    required multi link members -> User;
  }

Exclusive constraints
---------------------

You can add an ``exclusive`` constraint to a link to guarantee that no other
instances can link to the same target(s).

.. code-block:: sdl

  type Person {
    property name -> str;
  }

  type Club {
    required multi link members -> User {
      constraint exclusive;
    }
  }

In the ``GroupChat`` example, the ``GroupChat.members`` link is now
``exclusive``. No two ``GroupChats`` can link to the same ``User``; put
differently, no ``User`` can be a ``member`` of multiple ``GroupChats``.


Link properties
---------------

In EdgeDB, links can store *properties*. There are countless scenarios where
it's useful to store additional information about the *link itself*. For
instance we can model a family tree with a single ``Person`` type.

.. code-block:: sdl

  type Person {
    property name -> str;
    multi link family_members -> Person {
      property relationship -> str;
    }
  }

``Person.family_members`` is a many-to-many relation. Each ``family_members``
link can contain a string ``relationship`` containing the relationship of the ]
two individuals.

Refer to :ref:`Link Properties <ref_datamodel_linkprops>` for a more thorough
reference.


Deletion policies
-----------------

Links can declare their own **deletion policy**. When they target of a link is
deleted, there are 4 possible *actions* that can be taken:

- ``restrict`` (default) - any attempt to delete the target object immediately
  raises an exception;
- ``delete source`` - when the target of a link is deleted, the source
  is also deleted;
- ``allow`` - the target object is deleted and is removed from the
  set of the link targets;
- ``deferred restrict`` - any attempt to delete the target object
  raises an exception at the end of the transaction, unless by
  that time this object is no longer in the set of link targets.

To set a policy:

.. code-block:: sdl

  type MessageThread {
    property name -> str;
  }

  type Message {
    link chat -> MessageThread {
      on target delete delete source;
    }
  }

.. _ref_datamodel_link_deletion:

Polymorphic links
-----------------

Links can have ``abstract`` targets, in which case the link is considered
**polymorphic**. Consider the following schema:

.. code-block:: sdl

  abstract type Person {
    property name -> str;
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

  type Movie {
    property title -> str;
    multi link characters -> Person;
  }

In practice, the ``Movie.characters`` link can point to a ``Hero``,
``Villain``, or any other non-abstract subtype of ``Person``. For details on
how to write queries on such a link, refer to the :ref:`Polymorphic Queries
docs <ref_eql_polymorphic_queries>`


Abstract links
--------------

It's possible to define ``abstract`` links that aren't tied to a particular
*source* or *target*. If you're declaring several links containing the same set
of properties, annotations, constraints, or indexes, this can be used way to
eliminate repetitive link declarations.

.. code-block:: sdl

  abstract link link_with_strength {
    property strength -> float64;
    index on (__subject__@strength);
  }

  type Person {
    multi link friends extending link_with_strength -> Person;
  }

See Also
--------

:ref:`Cookbook <ref_cookbook_links>` section about links.

Link
:ref:`SDL <ref_eql_sdl_links>`,
:ref:`DDL <ref_eql_ddl_links>`,
:ref:`introspection <ref_eql_introspection_object_types>`
(as part of overall object introspection).
