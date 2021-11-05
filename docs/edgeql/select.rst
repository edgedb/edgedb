.. _ref_eql_select:

Select
======

- Basic examples
- Selecting objects
- Shapes
- Nested shapes
- Filter
  - Scope
- Order by
- Limit/offset
- With clauses
- Computed properties
  - Backlinks
  - Subqueries
- Polymorphic queries
- Free objects

.. _ref_eql_select_backlinks:

The ``select`` command retrieves or computes a set of values. We've already seen simple queries that select primitive values.

.. code-block:: edgeql-repl

  db> select 'hello world';
  {'hello world'}
  db> select [1, 2, 3];
  {[1, 2, 3]}


However most queries are selecting *objects* that live in the database. For demonstration purposes, the queries below assume the following schema.

.. code-block:: sdl

  module default {

    abstract type Person {
      required property name -> str {
        constraint exclusive;
      };
    }

    type Villain extending Person {
      link nemesis -> Hero;
    }

    type Hero extending Person {
      property secret_identity -> str;
      multi link villains := .<nemesis[IS Villain];
    }

    type Movie {
      required property title -> str;
      multi link characters -> Person;
    }

  }


Selecting objects
-----------------

Let's start by selecting all ``Villains`` objects in the database. In this example, there are only three. Remember, ``Villain`` is a :ref:`reference <ref_eql_set_references>` to the set of all Villain objects.

.. code-block:: edgeql-repl

  db> select Villain;
  {
    default::Villain {id: ea7bad4c...},
    default::Villain {id: 6ddbb04a...},
    default::Villain {id: b233ca98...},
  }

.. note::

  For the sake of readability, the ``id`` values have been truncated.

By default, this only returns the ``id`` of each object. If serialized to JSON, this result would look like this:

.. code-block::

  [
    {"id": "ea7bad4c-35d6-11ec-9519-0361f8abd380"},
    {"id": "6ddbb04a-3c23-11ec-b81f-7b7516f2a868"},
    {"id": "b233ca98-3c23-11ec-b81f-6ba8c4f0084e"},
  ]


.. _ref_eql_shapes:

Shapes
^^^^^^

To specify which properties to select, we attach a **shape** to ``Hero``. A shape can be attached to any object type expression in EdgeQL.

.. code-block:: edgeql-repl

  edgedb> select Villain { id, name };
  {
    default::Villain { id: ea7bad4c..., name: 'Whiplash' },
    default::Villain { id: 6ddbb04a..., name: 'Green Goblin', },
    default::Villain { id: b233ca98..., name: 'Doc Ock' },
  }

Nested shapes
^^^^^^^^^^^^^

Nested shapes can be used to fetch linked objects and their properties. Here we fetch all ``Villain`` objects and their nemeses.

.. code-block:: edgeql-repl

  db> select Villain {
  ...   name
  ...   nemesis: { name }
  ... };
  {
    default::Villain {
      name: 'Green Goblin',
      nemesis: default::Hero {name: 'Spider-Man'},
    },
    ...
  }

In the context of EdgeQL, computable links like ``Hero.villains`` are treated identically to concrete/non-computable links like ``Villain.nemesis``.

.. code-block:: edgeql-repl

  db> select Hero {
  ...   name
  ...   villains: { name }
  ... };
  {
    default::Hero {
      name: 'Spider-Man',
      villains: {
        default::Villain {name: 'Green Goblin'},
        default::Villain {name: 'Doc Ock'},
      },
    },
    ...
  }


Filtering
---------

To filter the set of selected objects, use a ``filter <expr>`` clause. The ``<expr>`` that follows the ``filter`` keyword can be *any boolean expression*.

To reference the ``name`` property of the ``Villain``objects being selected, we use ``Villain.name``.

.. code-block:: edgeql-repl

  db> select Villain {id, name}
  ... filter Villain.name = "Doc Ock"
  {default::Villain {id: b233ca98..., name: 'Doc Ock'}}


.. .. note::

..   This query contains two occurrences of ``Villain``. The first (outer) is a reference to the set of all ``Villain`` objects; the inner occurrence is *inside* the select shape and has a different meaning. Inside the shape, we are in the *scope* of the object type being selected. Thus, the ``Villain`` keyword refers to the *current object* being selected. It is similar to the ``this`` keyword inside a class definition (also known as ``self`` in Python).

However, this looks a little clunky, so EdgeQL provides a shorthand: just drop ``Villain`` entirely and simply use ``.name``. Since we are selecting a set of Villains, it's clear from context that ``.name`` must refer to a link/property of the ``Villain`` type. In other words, we are in the **scope** of the ``Villain`` type.

.. code-block:: edgeql-repl

  db> select Villain {name}
  ... filter .name = "Doc Ock"
  {default::Villain {name: 'Doc Ock'}}

.. Filtering by ID
.. ^^^^^^^^^^^^^^^

.. To filter by ``id``, remember to cast the desired value to :ref:`uuid <ref_std_uuid>`:

.. .. code-block:: edgeql-repl

..   db> select Villain {id, name}
..   ... filter .id = <uuid>"b233ca98-3c23-11ec-b81f-6ba8c4f0084e"
..   {
..     default::Villain {
..       id: 'b233ca98-3c23-11ec-b81f-6ba8c4f0084e',
..       name: 'Doc Ock'
..     }
..   }

Nested filters
^^^^^^^^^^^^^^

Filters can be added at every level of shape nesting. The query below applies a filter to both the selected ``Hero`` objects and their linked ``villains``.

.. code-block:: edgeql-repl

  edgedb> select Hero {
  .......   name,
  .......   villains: {
  .......     name
  .......   } filter .name ilike "%er"
  ....... } filter .name ilike "%man";
  {
    default::Hero {
      name: 'Iron Man',
      villains: {default::Villain {name: 'Justin Hammer'}},
    },
    default::Hero {
      name: 'Spider-Man',
      villains: {
        default::Villain {name: 'Shocker'},
        default::Villain {name: 'Tinkerer'},
        default::Villain {name: 'Kraven the Hunter'},
      },
    },
  }

Note that the *scope* changes inside nested shapes. When we use ``.name`` in the outer ``filter``, it refers to the name of the hero. But when we use ``.name`` in the nested ``villains`` shape, the scope has changed to ``Villain``.



Ordering
--------

Order the result of a query with an ``order by`` clause.

.. code-block:: edgeql-repl

  edgedb> select Villain { name }
  ....... order by .name
  {
    default::Villain {name: 'Abomination'},
    default::Villain {name: 'Doc Ock'},
    default::Villain {name: 'Green Goblin'},
    default::Villain {name: 'Justin Hammer'},
    default::Villain {name: 'Kraven the Hunter'},
    default::Villain {name: 'Loki'},
    default::Villain {name: 'Shocker'},
    default::Villain {name: 'The Vulture'},
    default::Villain {name: 'Tinkerer'},
    default::Villain {name: 'Zemo'},
  }

The expression provided to ``order by`` can be any primitive singleton expression, including arrays and tuples.

.. note::

  When ordering by arrays or tuples, the leftmost elements are compared. If these elements are the same, the next element is used to "break the tie", and so on. If all elements are the same, the order is not well defined.

.. code-block:: edgeql-repl

  edgedb> select Villain { name }
  ....... order by .name desc
  {
    default::Villain {name: 'Zemo'},
    ...
    default::Villain {name: 'Abomination'}
  }

For a full reference on ordering, including how empty values are handles, see :ref:`Reference > Commands > Select <ref_reference_select_order>`.

Pagination
----------

EdgeDB supports standard ``limit`` and ``offset`` operators. These are commonly used in conjunction with ``order by``.

.. code-block:: edgeql-repl

  edgedb> select Villain { name }
  ....... order by .name
  ....... offset 3
  ....... limit 3;
  {
    default::Villain {name: 'Hela'},
    default::Villain {name: 'Justin Hammer'},
    default::Villain {name: 'Kraven the Hunter'},
  }

The expressions passed to ``limit`` and ``offset`` can be any singleton ``int64`` expression. This query fetches all Villains except the last (sorted by name).

.. code-block:: edgeql-repl

  edgedb> select Villain {name}
  ....... order by .name
  ....... limit count(Villain) - 1;
  {
    default::Villain {name: 'Abomination'},
    default::Villain {name: 'Doc Ock'},
    ...
    default::Villain {name: 'Winter Soldier'}, # no Zemo
  }


Computed fields
---------------

Shapes can contain *computed fields*. These are EdgeQL expressions that are computed on the fly during the execution of the query. As with other clauses, we can use dot notation (e.g. ``.name``) to refer to the properties and links of the object type currently *in scope*.


.. code-block:: edgeql-repl

  db> select Villain {
  ...   name,
  ...   name_upper := str_upper(.name)
  ... };
  {
    default::Villain {
      id: 4114dd56...,
      name: 'Abomination',
      name_upper: 'ABOMINATION',
    },
    ...
  }

As with nested filters, the *current scope* changes inside nested shapes.

.. code-block:: edgeql-repl

  db> select Villain {
  ...   id,
  ...   name,
  ...   name_upper := str_upper(.name),
  ...   nemesis: {
  ...     secret_identity,
  ...     real_name_upper := str_upper(.secret_identity)
  ...   }
  ... };
  {
    default::Villain {
      id: 6ddbb04a...,
      name: 'Green Goblin',
      name_upper: 'GREEN GOBLIN',
      nemesis: default::Hero {
        secret_identity: 'Peter Parker',
        real_name_upper: 'PETER PARKER',
      },
    },
    ...
  }


Backlinks
^^^^^^^^^

Fetching backlinks is a common use case for computed fields. To demonstrate this, let's fetch a list of all movies starring a particular Hero.

.. code-block:: edgeql-repl

  edgedb> select Hero {
  .......   name,
  .......   movies := .<characters[is Movie] { title }
  ....... } filter .name = "Iron Man";
  {
    default::Hero {
      name: 'Iron Man',
      movies: {
        default::Movie {title: 'Iron Man'},
        default::Movie {title: 'Iron Man 2'},
        default::Movie {title: 'Iron Man 3'},
        default::Movie {title: 'Captain America: Civil War'},
        default::Movie {title: 'The Avengers'},
      },
    },
  }

.. note::

  The computed backlink ``villains`` is a combination of the *backlink operator* ``.<`` and a type intersection ``[is Villain]``. For a full reference on backlink syntax, see :ref:`EdgeQL > Paths <ref_eql_paths_backlinks>`.

Instead of re-declaring backlinks inside every query where they're needed, it's common to add them directly into your schema as computed links.

.. code-block:: sdl-diff

    abstract type Person {
      required property name -> str {
        constraint exclusive;
      };
  +   movies := .<characters[is Movie]
    }

This simplifies future queries.

.. code-block:: edgeql

  select Hero {
    name,
    movies: { title }
  } filter .name = "Iron Man";


Subqueries
^^^^^^^^^^

There's no limit to the complexity of computed expressions. EdgeQL is designed to be fully composable; entire queries can be embedded inside each other. Below, we use a subquery to select all movies containing a villain's nemesis.

.. code-block:: edgeql-repl

  edgedb> select Villain {
  .......   name,
  .......   nemesis_name := .nemesis.name
  .......   movies_with_nemesis := (
  .......     select Movie { title }
  .......     filter Villain.nemesis in .characters
  .......   )
  ....... };
  {
    default::Villain {
      name: 'Loki',
      nemesis_name: 'Thor',
      movies_with_nemesis: {
        default::Movie {title: 'Thor'},
        default::Movie {title: 'Thor: The Dark World'},
        default::Movie {title: 'Thor: Ragnarok'},
        default::Movie {title: 'The Avengers'},
      },
    },
    ...
  }


.. _ref_eql_polymorphic_queries:

Polymorphic queries
-------------------

:index: poly polymorphism nested shapes

All queries thus far have referenced concrete object types: ``Hero`` and ``Villain``. However, both of these types extend the abstract type ``Person``, from which they inherit the ``name`` property.



It's possible to directly query all ``Person`` objects; the resulting set with be a mix of ``Hero`` and ``Villain`` objects (and possibly other subtypes of ``Person``, should they be declared).

.. code-block:: edgeql-repl

  edgedb> select Person { name };
  {
    default::Villain {name: 'Abomination'},
    default::Villain {name: 'Zemo'},
    default::Hero {name: 'The Hulk'},
    default::Hero {name: 'Iron Man'},
    ...
  }

You may also encounter such "mixed sets" when querying a link that points to an abstract type (such as ``Movie.characters``) or a :eql:op:`union type <TYPEOR>`.


.. code-block:: edgeql-repl

  edgedb> select Movie {
  .......   title,
  .......   characters: {
  .......     name
  .......   }
  ....... }
  ....... filter .title = "Iron Man 2"
  {
    default::Movie {
      title: 'Iron Man 2',
      characters: {
        default::Villain {name: 'Whiplash'},
        default::Villain {name: 'Justin Hammer'},
        default::Hero {name: 'Iron Man'},
        default::Hero {name: 'Black Widow'},
      },
    },
  }

We can fetch different properties *conditional* on the subtype of each object by prefixing property/link references with ``[is <type>]``. This is known as a **polymorphic query**.

.. code-block:: edgeql-repl

  edgedb> select Person {
  .......   name,
  .......   secret_identity := [is Hero].secret_identity,
  .......   number_of_villains := count([is Hero].villains),
  .......   nemesis := [is Villain].nemesis {
  .......     name
  .......   }
  ....... };
  {
    default::Villain {
      name: 'Green Goblin',
      secret_identity: {},
      number_of_villains: 0,
      nemesis: default::Hero {name: 'Spider-Man'},
    },
    default::Hero {
      name: 'Spider-Man',
      secret_identity: 'Peter Parker',
      number_of_villains: 6,
      nemesis: {},
    },
    ...
  }

This syntax is intentionally similar to the type intersection operator. In effect, this operator conditionally returns the value of the referenced field only if the object matches a particular type. If the match fails, an empty set is returned.

The line ``secret_identity := [is Hero].secret_identity`` is a bit redundant, since the computed property has the same name as the polymorphic field. In these cases, EdgeQL supports a shorthand.

.. code-block:: edgeql-repl

  edgedb> select Person {
  .......   name,
  .......   [is Hero].secret_identity,
  .......   [is Villain].nemesis: {
  .......     name
  .......   }
  ....... };
  {
    default::Villain {
      name: 'Green Goblin',
      secret_identity: {},
      nemesis: default::Hero {name: 'Spider-Man'},
    },
    default::Hero {
      name: 'Spider-Man',
      secret_identity: 'Peter Parker',
      nemesis: {},
    },
    ...
  }

.. Relatedly, it's possible to filter links by subtype "inline" with a similar syntax. Below, we filter ``Movie.characters`` to include exclusively ``Hero`` objects.

.. .. code-block::

..   edgedb> select Movie {
..   .......   title,
..   .......   characters[IS Hero]: {
..   .......     secret_identity
..   .......   },
..   ....... };
..   {
..     default::Movie {
..       title: 'Spider-Man: Homecoming',
..       characters: {default::Hero {secret_identity: 'Peter Parker'}},
..     },
..     default::Movie {
..       title: 'Iron Man',
..       characters: {default::Hero {secret_identity: 'Tony Stark'}},
..     },
..     ...
..   }


Free objects
------------

To select several values simultaneously, you can "bundle" them into a "free object". Free objects are a set of key-value pairs that can contain any expression. Here, the term "free" is used to indicate that the object in question is not an instance of a particular *object type*; instead, it's constructed ad hoc inside the query.

.. code-block:: edgeql-repl

  db> select {
  ...   my_string := "This is a string",
  ...   my_number := 42,
  ...   several_numbers := {1, 2, 3},
  ...   all_heroes := Hero { name }
  ... };
  {
    {
      my_string: 'This is a string',
      my_number: 42,
      several_numbers: {1, 2, 3},
      all_heroes: {
        default::Hero {name: 'The Hulk'},
        default::Hero {name: 'Iron Man'},
        default::Hero {name: 'Spider-Man'},
        default::Hero {name: 'Thor'},
        default::Hero {name: 'Captain America'},
        default::Hero {name: 'Black Widow'},
      },
    },
  }


Note that the result is a *singleton* but each key corresponds to a set of values, which may have any cardinality.


With clauses
------------

All top-level EdgeQL statements (``select``, ``insert``, ``update``, and ``delete``) can be prefixed with a ``with`` clause. These clauses let you declare standalone expressions that can be used in your query.

.. code-block:: edgeql-repl

  edgedb> with hero_name := "Iron Man"
  ....... select Hero { secret_identity }
  ....... filter .name = hero_name;
  {default::Hero {secret_identity: 'Tony Stark'}}


For full documentation on ``with``, see :ref:`EdgeQL > With <ref_eql_with>`.
