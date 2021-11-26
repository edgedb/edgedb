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
must have a ``best_friend``:

.. code-block:: sdl

  type Person {
    required link best_friend -> Person;
  }

Links with cardinality ``multi`` can also be ``required``;
``required multi`` links must point to *at least one* target object.

.. code-block:: sdl

  type Person {
    property name -> str;
  }

  type GroupChat {
    required multi link members -> Person;
  }

In this scenario, each ``GroupChat`` must contain at least one person.
Attempting to create a ``GroupChat`` with no members would fail.

Exclusive constraints
---------------------

You can add an ``exclusive`` constraint to a link to guarantee that no other
instances can link to the same target(s).

.. code-block:: sdl

  type Person {
    property name -> str;
  }

  type GroupChat {
    required multi link members -> Person {
      constraint exclusive;
    }
  }

In the ``GroupChat`` example, the ``GroupChat.members`` link is now
``exclusive``. No two ``GroupChats`` can link to the same ``Person``; put
differently, no ``Person`` can be a ``member`` of multiple ``GroupChats``.

.. important::

  The combination of link cardinality and exclusive constraints are sufficient
  to model all kinds of relations: one-to-one, one-to-many, and many-to-many.
  For details, read the :ref:`Modeling Relations
  <ref_guide_modeling_relations>` guide.

Default values
--------------

Like properties, links can declare a default value in the form of an EdgeQL
expression, which will be executed upon insertion. In the example below, new
people are automatically assigned three random friends.

.. code-block:: sdl

  type Person {
    required property name -> str;
    multi link friends -> Person {
      default := (select Person order by random() limit 3);
    }
  }


.. _ref_datamodel_link_properties:

Link properties
---------------

In EdgeDB, links can store *properties*. Like object types, links can contain
**properties**. Link properties can be used to store metadata about links, such
as *when* it was created or the *nature/strength* of the relationship.

.. code-block:: sdl

  type Person {
    property name -> str;
    multi link family_members -> Person {
      property relationship -> str;
    }
  }

Above, we model a family tree with a single ``Person`` type. The ``Person.
family_members`` link is a many-to-many relation; each ``family_members`` link
can contain a string ``relationship`` describing the relationship of the two
individuals.

Due to how they're persisted under the hood, link properties must always be
``single`` and ``optional``.

.. note::

  For a full guide on modeling, inserting, updating, and querying link
  properties, see the :ref:`Using Link Properties <ref_guide_linkprops>` guide.

.. _ref_datamodel_link_deletion:

Deletion policies
-----------------

Links can declare their own **deletion policy**. When the target of a link is
deleted, there are 4 possible *actions* that can be taken:

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
docs <ref_eql_select_polymorphic>`


Abstract links
--------------

It's possible to define ``abstract`` links that aren't tied to a particular
*source* or *target*. If you're declaring several links with the same set
of properties, annotations, constraints, or indexes, abstract links can be used
to eliminate repetitive SDL.

.. code-block:: sdl

  abstract link link_with_strength {
    property strength -> float64;
    index on (__subject__@strength);
  }

  type Person {
    multi link friends extending link_with_strength -> Person;
  }


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Links <ref_eql_sdl_links>`
  * - :ref:`DDL > Links <ref_eql_ddl_links>`
  * - :ref:`Introspection > Object types
      <ref_eql_introspection_object_types>`
