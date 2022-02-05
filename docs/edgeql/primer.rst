.. _ref_eql_primer:

EdgeQL: A primer
================

Before we can talk about EdgeQL we need to understand how data is modelled in EdgeDB. The fundamental unit of schema is the object type—analogous to tables in SQL. Object types contain properties.

.. code-block:: sdl-diff

  type Player {
    property username -> str;
  }

Properties can have the following primitive types.

- ``str``
- ``bool``
- ``int64``
- ``int32``
- ``int16``
- ``float64``
- ``float32``
- ``uuid``
- ``bytes``
- ``json``
- ``duration``
- ``datetime``
- ``cal::relative_duration``
- ``cal::local_datetime``
- ``cal::local_date``
- ``cal::local_time``

You can declare custom enum types.

.. code-block:: sdl

  type enum Color extending enum<Red, Green, Blue>;s

These basic types can be combined into array and tuple types.

.. code-block:: sdl

  type Player {
    property username -> str;
    property nicknames -> array<str>;
    property position -> tuple<float64, float64>;
  }

By default properties are optional. They can be made required with the ``required`` keyword.

.. code-block:: sdl-diff

    type Player {
  -   property username -> str;
  +   required property username -> str;
    }

Note that all object types automatically have a ``required`` ``id`` property with a uniqueness constraint—no need to declare it.

.. note::

  I'm intentionally skipping the concept of ``multi`` properties...not common enough, and impossible to explain without getting too deep into set stuff.

.. By default properties are ``single``. That means only a single element of the associated type can be assigned to them. By marking a property with ``multi``, a set of values can be assigned.

.. .. code-block:: sdl-diff

..     type Person {
..       required property name -> str;
..   +   multi property nicknames -> str;
..     }


.. These modifiers change the allowable *cardinality* of the assigned value. The cardinality is the number of elements assigned to the property.

.. .. list-table::

..   * - ``property``
..     - 0 or 1
..   * - ``required property``
..     - exactly 1
..   * - ``multi property``
..     - 0 or more
..   * - ``multi required property``
..     - 1 or more

Let's use these basics to start building a schema for a Netflix clone. Starting with a simple ``Movie`` type.

.. code-block:: sdl-diff

  + type Movie {
  +   required property title -> str;
  +   property runtime -> duration;
  + }


Constraints can be added to properties.

.. code-block:: sdl-diff

    type Movie {
  -   required property title -> str;
  +   required property title -> str {
  +     constraint exclusive;
  +   };
      property runtime -> duration;
    }


There are a number of built-in constraint types, or you can define custom ones.

- ``exclusive``
- ``one_of``
- ``max_value``
- ``max_ex_value``
- ``max_len_value``
- ``min_value``
- ``min_ex_value``
- ``min_len_value``
- ``regexp``
- ``expression`` (custom)


Declaring one-to-many links.

.. code-block:: sdl-diff

  + type Person {
  +   property name -> str;
  + }

    type Movie {
      required property title -> str {
        constraint exclusive;
      };
      property runtime -> duration;
  +   link director -> Person;
    }


An equivalent representation using a ``multi`` link with an exclusive property.

.. code-block:: sdl-diff

    type Person {
      property name -> str;
  +   multi link directed -> Person {
  +     constraint exclusive;
  +   };
    }

    type Movie {
      required property title -> str {
        constraint exclusive;
      };
      property runtime -> duration;
      link director -> Person;
    }

Declaring one-to-one relations.

.. code-block:: sdl-diff

    type Person {
      property name -> str;
    }

    type Movie {
      required property title -> str {
        constraint exclusive;
      };
      property runtime -> duration;
      link director -> Person;
  +   link stats -> MovieStatistics { constraint exclusive; };
    }

  + type MovieStatistics {
  +   total_views -> int64;
  + }


Declaring many-to-many links.

.. code-block:: sdl-diff

    type Person {
      property name -> str;
    }

    type Movie {
      required property title -> str {
        constraint exclusive;
      };
      property runtime -> duration;
      link director -> Person;
  +   multi link actors -> Person;
    }

Adding properties to links. We call EdgeDB *graph-relational* for a reason!

.. code-block:: sdl-diff

    type Person {
      property name -> str;
    }

    type Movie {
      required property title -> str {
        constraint exclusive;
      };
      property runtime -> duration;
      link director -> Person;
  -   multi link actors -> Person;
  +   multi link actors -> Person {
  +     property character_name -> str;
  +   }
    }


Whoops, we forgot about TV shows.

.. code-block:: sdl-diff

    type Person {
      property name -> str;
    }

    type Movie {
      required property title -> str {
        constraint exclusive;
      };
      property runtime -> duration;
    }

  + type TVShow {
  +   required property title -> str {
  +     constraint exclusive;
  +   }
  +   property num_episodes -> int64;
  + }

Hmm looks a little duplicative. Let's make this a bit more elegant with mixins.

.. code-block:: sdl-diff

    type Person {
      property name -> str;
    }

  + type Content {
  +   required property title -> str {
  +     constraint exclusive;
  +   }
  + }

  - type Movie {
  + type Movie extending Content {
  -   required property title -> str {
  -     constraint exclusive;
  -   };
      property runtime -> duration;
    }

  - type TVShow {
  + type TVShow extending Content {
  -   required property title -> str {
  -     constraint exclusive;
  -   };
      property num_episodes -> int64;
    }

That's better. Now that we have a handle on the basics of schema modeling, lets look at querying.

.. code-block:: edgeql

  select Movie;

Selecting fields with shapes.

.. code-block:: edgeql

  select Movie {
    id,
    title,
    runtime
  };

Nested shapes

.. code-block:: edgeql

  select Movie {
    id,
    title,
    runtime,
    cast: {
      name
    }
  };

Nested shape with link properties

.. code-block:: edgeql

  select Movie {
    id,
    title,
    runtime,
    cast: {
      name,
      @character_name
    }
  };

Let's add a computed property. Computed properties use "defined as" operator, AKA the walrus operator: https://res.cloudinary.com/practicaldev/image/fetch/s--T9WkgS9z--/c_imagga_scale,f_auto,fl_progressive,h_420,q_auto,w_1000/https://dev-to-uploads.s3.amazonaws.com/i/ic9zo8rzfd1y6qb5m732.jpg

The ``uppercase_title`` computed property contains a full uppercased version of the movie title.

.. code-block:: edgeql

  select Movie {
    title,
    uppercase_title := str_upper(Movie.title)
  }

Here ``Movie.title`` is called a *path*. This is a unique feature of EdgeQL, because EdgeDB is the only major database with the concept of named links and properties baked deeply into its schema and query language.

From any set of objects, you can "traverse" its properties and links using with dot notation to retrieve the linked data. Here's an example that traverses a link.


.. code-block:: edgeql

  select Movie {
    title,
    num_actors := count(Movie.actors)
  }

A note about syntax. This queries can be simlified with what we call "leading dot notation". Inside ``select Movie`` we're in the scope of ``Movie`` so we can refer to its properties and links with leading dot shorthand.

.. code-block:: edgeql

  select Movie {
    title,
    uppercase_title := str_upper(.title),
    num_actors := count(.actors)
  }

We mentioned that paths can be used to traverse links. Links can also be traversed in reverse. These are called backlinks. The query below uses backlinks to fetch a Person and all objects with a link ``actors`` that link to the

.. code-block:: edgeql

  select Person {
    name,
    acted_in := .<actors
  }

This isn't very useful because we have no information about what kind of objects may have an ``actors`` link to this Person.

(??)-[actors]-> (Person) <-[actors]-(??)

Now you may be thinking: "Of course we do, the only type with a link called ``actors`` that points to ``Person`` is ``Movie``." That's true, but it may not always be the case. At any point another type may come along like this.

.. code-block:: sdl

  type Flugelhorn {
    multi link actors -> Person;
  }

All of a sudden we have no guarantee what the type of ``acted_in`` might be. It could be a Movie or it could be a Flugelhorn.

.. code-block:: edgeql

  select Person {
    name,
    acted_in := .<actors
  }

To account for this EdgeQL has a concept of a "type intersection". You can think of it as a way to filter a set of objects by type.

.. code-block:: edgeql

  select Person {
    name,
    acted_in := .<actors[is Movie]
  }

Now EdgeQL knows ``acted_in`` corresponds to a set of ``Movie`` so we can add a shape to this:

.. code-block:: edgeql

  select Person {
    name,
    acted_in := .<actors[is Movie] {
      title,
      duration
    }
  }


But in practice, it's usually to add backlinks like this—any any other commonly used computed properties or links—directly into your schema.

.. code-block:: sdl-diff

    type Person {
      property name -> str;
  +   acted_in := .<actors[is Movie];
    }

Now the ``Person.acted_in`` link can be used in shapes just like a "real" link.

.. code-block:: edgeql

    select Person {
      name,
      acted_in: {
        title,
        runtime
      }
    }

Paths are also a key part of how to filter your queries.

.. code-block:: edgeql

  select Movie {
    id,
    title,
  }
  filter .title = "Dune"

And order them. And paginate them.

.. code-block:: edgeql-diff

    select Movie {
      id,
      title,
    }
    filter .title = "Dune"
  + order by .runtime
  + offset 10
  + limit 10


This was a very basic primer. There's a lot more to learn that's beyond the scope of this talk, but this ought to get you started!

.. code-block:: sdl

  + scalar type Rating extending int64 {
  +  constraint min_value(1);
  +  constraint max_value(5);
  + }

    type Person {
      property name -> str;
      multi link reviews -> Review;
    }

  + type Review {
  +   link reviewer -> Person;
  +   link content -> Content;
  +   property rating -> Rating;
  + }

  - type Movie extending Content {
  + type Movie extending Content, Favoriteable, Reviewable {
      required property title -> str {
        constraint exclusive;
  +     constraint min_len_value(3)
      }
  +   annotation description := "Abstract type for all forms of media content."
  +   index on (.title);
    }
