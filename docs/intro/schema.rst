.. _ref_intro_schema:

======
Schema
======


This page is intended as a rapid-fire overview of EdgeDB's schema definition
language (SDL) so you can hit the ground running with EdgeDB. Refer to the
linked pages for more in-depth documentation!

Scalar types
------------

EdgeDB implements a rigorous type system containing the following primitive
types.

.. list-table::

  * - Strings
    - ``str``
  * - Booleans
    - ``bool``
  * - Numbers
    - ``int16`` ``int32`` ``int64`` ``float32`` ``float64`` 
      ``bigint`` ``decimal``
  * - UUID
    - ``uuid``
  * - JSON
    - ``json``
  * - Dates and times
    - ``datetime`` ``cal::local_datetime`` ``cal::local_date``
      ``cal::local_time``
  * - Durations
    - ``duration`` ``cal::relative_duration`` ``cal::date_duration``
  * - Binary data
    - ``bytes``
  * - Auto-incrementing counters
    - ``sequence``
  * - Enums
    - ``enum<x, y, z>``

These primitives can be combined into arrays, tuples, and ranges.

.. list-table::

  * - Arrays
    - ``array<str>``
  * - Tuples (unnamed)
    - ``tuple<str, int64, bool>``
  * - Tuples (named)
    - ``tuple<name: str, age: int64, is_awesome: bool>``
  * - Ranges
    - ``range<float64>``

Collectively, *primitive* and *collection* types comprise EdgeDB's *scalar
type system*.

Object types
------------

Object types are analogous to tables in SQL. They can contain **properties**,
which can correspond to any scalar types, and **links**, which can correspond
to any object types.

Properties
----------

Declare a property by naming it and setting its type.

.. code-block:: sdl
    :version-lt: 3.0

    type Movie {
      property title -> str;
    }

.. code-block:: sdl

    type Movie {
      title: str;
    }

The ``property`` keyword can be omitted for non-computed properties since
EdgeDB v3.

See :ref:`Schema > Object types <ref_std_object_types>`.

Required vs optional
^^^^^^^^^^^^^^^^^^^^

Properties are optional by default. Use the ``required`` keyword to make them
required.

.. code-block:: sdl
    :version-lt: 3.0

    type Movie {
      required property title -> str;       # required
      property release_year -> int64;       # optional
    }

.. code-block:: sdl

    type Movie {
      required title: str;       # required
      release_year: int64;       # optional
    }

See :ref:`Schema > Properties <ref_datamodel_props>`.

Constraints
^^^^^^^^^^^

Add a pair of curly braces after the property to define additional
information, including constraints.

.. code-block:: sdl
    :version-lt: 3.0

    type Movie {
      required property title -> str {
        constraint exclusive;
        constraint min_len_value(8);
        constraint regexp(r'^[A-Za-z0-9 ]+$');
      }
    }

.. code-block:: sdl

    type Movie {
      required title: str {
        constraint exclusive;
        constraint min_len_value(8);
        constraint regexp(r'^[A-Za-z0-9 ]+$');
      }
    }

See :ref:`Schema > Constraints <ref_datamodel_constraints>`.


Computed properties
^^^^^^^^^^^^^^^^^^^

Object types can contain *computed properties* that correspond to EdgeQL
expressions. This expression is dynamically computed whenever the property is
queried.

.. code-block:: sdl
    :version-lt: 3.0

    type Movie {
      required property title -> str;
      property uppercase_title := str_upper(.title);
    }

.. code-block:: sdl
    :version-lt: 4.0

    type Movie {
      required title: str;
      property uppercase_title := str_upper(.title);
    }

.. code-block:: sdl

    type Movie {
      required title: str;
      uppercase_title := str_upper(.title);
    }

See :ref:`Schema > Computeds <ref_datamodel_computed>`.

Links
-----

Object types can have links to other object types.

.. code-block:: sdl
    :version-lt: 3.0

    type Movie {
      required property title -> str;
      link director -> Person;
    }

    type Person {
      required property name -> str;
    }

.. code-block:: sdl

    type Movie {
      required title: str;
      director: Person;
    }

    type Person {
      required name: str;
    }

The ``link`` keyword can be omitted for non-computed links since EdgeDB v3.

Use the ``required`` and ``multi`` keywords to specify the cardinality of the
relation.

.. code-block:: sdl
    :version-lt: 3.0

    type Movie {
      required property title -> str;

      link cinematographer -> Person;             # zero or one
      required link director -> Person;           # exactly one
      multi link writers -> Person;               # zero or more
      required multi link actors -> Person;       # one or more
    }

    type Person {
      required property name -> str;
    }

.. code-block:: sdl

    type Movie {
      required title: str;

      cinematographer: Person;             # zero or one
      required director: Person;           # exactly one
      multi writers: Person;               # zero or more
      required multi actors: Person;       # one or more
    }

    type Person {
      required name: str;
    }

To define a one-to-one relation, use an ``exclusive`` constraint.

.. code-block:: sdl
    :version-lt: 3.0

    type Movie {
      required property title -> str;
      required link stats -> MovieStats {
        constraint exclusive;
      };
    }

    type MovieStats {
      required property budget -> int64;
      required property box_office -> int64;
    }

.. code-block:: sdl

    type Movie {
      required title: str;
      required stats: MovieStats {
        constraint exclusive;
      };
    }

    type MovieStats {
      required budget: int64;
      required box_office: int64;
    }

See :ref:`Schema > Links <ref_datamodel_links>`.

Computed links
^^^^^^^^^^^^^^

Objects can contain "computed links": stored expressions that return a set of
objects. Computed links are dynamically computed when they are referenced in
queries. The example below defines a backlink.

.. code-block:: sdl
    :version-lt: 3.0

    type Movie {
      required property title -> str;
      multi link actors -> Person;

      # returns all movies with same title
      multi link same_title := (
        with t := .title
        select detached Movie filter .title = t
      )
    }

.. code-block:: sdl
    :version-lt: 4.0

    type Movie {
      required title: str;
      multi actors: Person;

      # returns all movies with same title
      multi link same_title := (
        with t := .title
        select detached Movie filter .title = t
      )
    }

.. code-block:: sdl

    type Movie {
      required title: str;
      multi actors: Person;

      # returns all movies with same title
      multi same_title := (
        with t := .title
        select detached Movie filter .title = t
      )
    }

Backlinks
^^^^^^^^^

A common use case for computed links is *backlinks*.

.. code-block:: sdl
    :version-lt: 3.0

    type Movie {
      required property title -> str;
      multi link actors -> Person;
    }

    type Person {
      required property name -> str;
      multi link acted_in := .<actors[is Movie];
    }

.. code-block:: sdl
    :version-lt: 4.0

    type Movie {
      required title: str;
      multi actors: Person;
    }

    type Person {
      required name: str;
      multi link acted_in := .<actors[is Movie];
    }

.. code-block:: sdl

    type Movie {
      required title: str;
      multi actors: Person;
    }

    type Person {
      required name: str;
      multi acted_in := .<actors[is Movie];
    }

The computed link ``acted_in`` returns all ``Movie`` objects with a link
called ``actors`` that points to the current ``Person``. The easiest way to
understand backlink syntax is to split it into two parts:

``.<actors``
  This uses a special syntax ``.<`` to return all objects in the database with
  a link called ``actors`` that points to the current object. This set could
  conceivably contain other objects besides ``Movie``; for instance, we could
  define a ``TVShow`` type that also included ``link actors -> Person``.

``[is Movie]``
  This is a *type filter* that filters out all objects that aren't ``Movie``
  objects. A backlink still works without this filter, but could contain any 
  other number of objects besides ``Movie`` objects.

See :ref:`Schema > Computeds > Backlinks <ref_datamodel_links_backlinks>`.

Constraints
-----------

Constraints can also be defined at the *object level*.

.. code-block:: sdl
    :version-lt: 3.0

    type BlogPost {
      property title -> str;
      link author -> User;

      constraint exclusive on ((.title, .author));
    }


.. code-block:: sdl

    type BlogPost {
      title: str;
      author: User;

      constraint exclusive on ((.title, .author));
    }

Constraints can contain exceptions; these are called *partial constraints*.

.. code-block:: sdl
    :version-lt: 3.0

    type BlogPost {
      property title -> str;
      property published -> bool;

      constraint exclusive on (.title) except (not .published);
    }

.. code-block:: sdl

    type BlogPost {
      title: str;
      published: bool;

      constraint exclusive on (.title) except (not .published);
    }

Indexes
-------

Use ``index on`` to define indexes on an object type.

.. code-block:: sdl
    :version-lt: 3.0

    type Movie {
      required property title -> str;
      required property release_year -> int64;

      index on (.title);                        # simple index
      index on ((.title, .release_year));       # composite index
      index on (str_trim(str_lower(.title)));   # computed index
    }

.. code-block:: sdl

    type Movie {
      required title: str;
      required release_year: int64;

      index on (.title);                        # simple index
      index on ((.title, .release_year));       # composite index
      index on (str_trim(str_lower(.title)));   # computed index
    }

The ``id`` property, all links, and all properties with ``exclusive``
constraints are automatically indexed.

See :ref:`Schema > Indexes <ref_datamodel_indexes>`.

Schema mixins
-------------

Object types can be declared as ``abstract``. Non-abstract types can *extend*
abstract types.

.. code-block:: sdl
    :version-lt: 3.0

    abstract type Content {
      required property title -> str;
    }

    type Movie extending Content {
      required property release_year -> int64;
    }

    type TVShow extending Content {
      required property num_seasons -> int64;
    }

.. code-block:: sdl

    abstract type Content {
      required title: str;
    }

    type Movie extending Content {
      required release_year: int64;
    }

    type TVShow extending Content {
      required num_seasons: int64;
    }

Multiple inheritance is supported.

.. code-block:: sdl
    :version-lt: 3.0

    abstract type HasTitle {
      required property title -> str;
    }

    abstract type HasReleaseYear {
      required property release_year -> int64;
    }

    type Movie extending HasTitle, HasReleaseYear {
      link sequel_to -> Movie;
    }

.. code-block:: sdl

    abstract type HasTitle {
      required title: str;
    }

    abstract type HasReleaseYear {
      required release_year: int64;
    }

    type Movie extending HasTitle, HasReleaseYear {
      sequel_to: Movie;
    }

See :ref:`Schema > Object types > Inheritance
<ref_datamodel_objects_inheritance>`.

Polymorphism
------------

Links can correspond to abstract types. These are known as *polymorphic links*.

.. code-block:: sdl
    :version-lt: 3.0

    abstract type Content {
      required property title -> str;
    }

    type Movie extending Content {
      required property release_year -> int64;
    }

    type TVShow extending Content {
      required property num_seasons -> int64;
    }

    type Franchise {
      required property name -> str;
      multi link entries -> Content;
    }

.. code-block:: sdl

    abstract type Content {
      required title: str;
    }

    type Movie extending Content {
      required release_year: int64;
    }

    type TVShow extending Content {
      required num_seasons: int64;
    }

    type Franchise {
      required name: str;
      multi entries: Content;
    }

See :ref:`Schema > Links > Polymorphism
<ref_datamodel_link_polymorphic>` and :ref:`EdgeQL > Select > Polymorphic
queries <ref_eql_select_polymorphic>`.

