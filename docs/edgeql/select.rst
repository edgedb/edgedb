.. _ref_eql_select:

Select
======


The ``select`` command retrieves or computes a set of values. We've already
seen simple queries that select primitive values.

.. code-block:: edgeql-repl

  db> select 'hello world';
  {'hello world'}
  db> select [1, 2, 3];
  {[1, 2, 3]}
  db> select {1, 2, 3};
  {1, 2, 3}


With the help of a ``with`` block, we can add filters, ordering, and
pagination clauses.

.. code-block:: edgeql-repl

  db> with x := {1, 2, 3, 4, 5}
  ... select x
  ... filter x >= 3;
  {3, 4, 5}
  db> with x := {1, 2, 3, 4, 5}
  ... select x
  ... order by x desc;
  {5, 4, 3, 2, 1}
  db> with x := {1, 2, 3, 4, 5}
  ... select x
  ... offset 1 limit 3;
  {2, 3, 4}

These queries can also be rewritten to use inline aliases, like so:

.. code-block:: edgeql-repl

  db> select x := {1, 2, 3, 4, 5}
  ... filter x >= 3;


.. _ref_eql_select_objects:

Selecting objects
-----------------

However most queries are selecting *objects* that live in the database. For
demonstration purposes, the queries below assume the following schema.

.. code-block:: sdl
    :version-lt: 3.0

    module default {
      abstract type Person {
        required property name -> str { constraint exclusive };
      }

      type Hero extending Person {
        property secret_identity -> str;
        multi link villains := .<nemesis[is Villain];
      }

      type Villain extending Person {
        link nemesis -> Hero;
      }

      type Movie {
        required property title -> str { constraint exclusive };
        required property release_year -> int64;
        multi link characters -> Person;
      }
    }

.. code-block:: sdl

    module default {
      abstract type Person {
        required name: str { constraint exclusive };
      }

      type Hero extending Person {
        secret_identity: str;
        multi link villains := .<nemesis[is Villain];
      }

      type Villain extending Person {
        nemesis: Hero;
      }

      type Movie {
        required title: str { constraint exclusive };
        required release_year: int64;
        multi characters: Person;
      }
    }

Let's start by selecting all ``Villain`` objects in the database. In this
example, there are only three. Remember, ``Villain`` is a :ref:`reference
<ref_eql_set_references>` to the set of all Villain objects.

.. code-block:: edgeql-repl

  db> select Villain;
  {
    default::Villain {id: ea7bad4c...},
    default::Villain {id: 6ddbb04a...},
    default::Villain {id: b233ca98...},
  }

.. note::

  For the sake of readability, the ``id`` values have been truncated.

By default, this only returns the ``id`` of each object. If serialized to JSON,
this result would look like this:

.. code-block::

  [
    {"id": "ea7bad4c-35d6-11ec-9519-0361f8abd380"},
    {"id": "6ddbb04a-3c23-11ec-b81f-7b7516f2a868"},
    {"id": "b233ca98-3c23-11ec-b81f-6ba8c4f0084e"},
  ]

Learn to select objects by trying it in `our interactive object query
tutorial </tutorial/basic-queries/objects>`_.


.. _ref_eql_shapes:

Shapes
------

To specify which properties to select, we attach a **shape** to ``Villain``. A
shape can be attached to any object type expression in EdgeQL.

.. code-block:: edgeql-repl

  db> select Villain { id, name };
  {
    default::Villain { id: ea7bad4c..., name: 'Whiplash' },
    default::Villain { id: 6ddbb04a..., name: 'Green Goblin', },
    default::Villain { id: b233ca98..., name: 'Doc Ock' },
  }

To learn to use shapes by trying them yourself, see `our interactive shapes
tutorial </tutorial/nested-structures/shapes>`_.

Nested shapes
^^^^^^^^^^^^^

Nested shapes can be used to fetch linked objects and their properties. Here we
fetch all ``Villain`` objects and their nemeses.

.. code-block:: edgeql-repl

  db> select Villain {
  ...   name,
  ...   nemesis: { name }
  ... };
  {
    default::Villain {
      name: 'Green Goblin',
      nemesis: default::Hero {name: 'Spider-Man'},
    },
    ...
  }

In the context of EdgeQL, computed links like ``Hero.villains`` are treated
identically to concrete/non-computed links like ``Villain.nemesis``.

.. code-block:: edgeql-repl

  db> select Hero {
  ...   name,
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


.. _ref_eql_select_splats:

Splats
^^^^^^

.. versionadded:: 3.0

Splats allow you to select all properties of a type using the asterisk (``*``)
or all properties of the type and a single level of linked types with a double
asterisk (``**``).

If you have this schema:

.. code-block:: sdl

    module default {
      abstract type Person {
        required name: str { constraint exclusive };
      }

      type Hero extending Person {
        secret_identity: str;
        multi link villains := .<nemesis[is Villain];
      }

      type Villain extending Person {
        nemesis: Hero;
      }

      type Movie {
        required title: str { constraint exclusive };
        required release_year: int64;
        multi characters: Person;
      }
    }

splats will help you more easily select all properties when using the REPL. You
can select all of an object's properties using the single splat:

.. code-block:: edgeql-repl

    db> select Movie {*};
    {
      default::Movie {
        id: 6fe5c3ec-b776-11ed-8bef-3b2fba99fe8a,
        release_year: 2021,
        title: 'Spiderman: No Way Home',
      },
      default::Movie {
        id: 76998656-b776-11ed-8bef-237907a987fa,
        release_year: 2008,
        title: 'Iron Man'
      },
    }

or you can select all of an object's properties and the properties of a single
level of nested objects with the double splat:

.. code-block:: edgeql-repl

    db> select Movie {**};
    {
      default::Movie {
        id: 6fe5c3ec-b776-11ed-8bef-3b2fba99fe8a,
        release_year: 2021,
        title: 'Spiderman: No Way Home',
        characters: {
          default::Hero {
            id: 01d9cc22-b776-11ed-8bef-73f84c7e91e7,
            name: 'Spiderman'
          },
          default::Villain {
            id: efa2c4bc-b777-11ed-99eb-43f835d79384,
            name: 'Electro'
          },
          default::Villain {
            id: f2c99a96-b775-11ed-8f7e-0b4a4a8e433e,
            name: 'Green Goblin'
          },
          default::Villain {
            id: f8ca354a-b775-11ed-8bef-273145019e1d,
            name: 'Doc Ock'
          },
        },
      },
      default::Movie {
        id: 76998656-b776-11ed-8bef-237907a987fa,
        release_year: 2008,
        title: 'Iron Man',
        characters: {
          default::Hero {
            id: 48edcf8c-b776-11ed-8bef-c7d61b6780d2,
            name: 'Iron Man'
          },
          default::Villain {
            id: 335f4104-b777-11ed-81eb-ab4de34e9c36,
            name: 'Obadiah Stane'
          },
        },
      },
    }

.. note::

    Splats are not yet supported in function bodies.

The splat expands all properties defined on the type as well as inherited
properties. Given the same schema shown above:

.. code-block:: edgeql-repl

    db> select Hero {*};
    {
      default::Hero {
        id: 01d9cc22-b776-11ed-8bef-73f84c7e91e7,
        name: 'Spiderman',
        secret_identity: 'Peter Parker'
      },
      default::Hero {
        id: 48edcf8c-b776-11ed-8bef-c7d61b6780d2,
        name: 'Iron Man',
        secret_identity: 'Tony Stark'
      },
    }

The splat here expands the heroes' names even though the ``name`` property is
not defined on the ``Hero`` type but on the ``Person`` type it extends. If we
want to select heroes but get only properties defined on the ``Person`` type,
we can do this instead:

.. code-block:: edgeql-repl

    db> select Hero {Person.*};
    {
      default::Hero {
        id: 01d9cc22-b776-11ed-8bef-73f84c7e91e7,
        name: 'Spiderman'
      },
      default::Hero {
        id: 48edcf8c-b776-11ed-8bef-c7d61b6780d2,
        name: 'Iron Man'
      },
    }

If there are links on our ``Person`` type, we can use ``Person.**`` in a
similar fashion to get all properties and one level of linked object
properties, but only for links and properties that are defined on the
``Person`` type.

You can use the splat to expand properties using a :ref:`type intersection
<ref_eql_types_intersection>`. Maybe we want to select all ``Person`` objects
with their names but also get any properties defined on the ``Hero`` for those
``Person`` objects which are also ``Hero`` objects:

.. code-block:: edgeql-repl

    db> select Person {
    ...   name,
    ...   [is Hero].*
    ... };
    {
      default::Hero {
        id: 01d9cc22-b776-11ed-8bef-73f84c7e91e7,
        name: 'Spiderman'
        secret_identity: 'Peter Parker'
      },
      default::Hero {
        id: 48edcf8c-b776-11ed-8bef-c7d61b6780d2,
        name: 'Iron Man'
        secret_identity: 'Tony Stark'
      },
      default::Villain {
        id: efa2c4bc-b777-11ed-99eb-43f835d79384,
        name: 'Electro'
      },
      default::Villain {
        id: f2c99a96-b775-11ed-8f7e-0b4a4a8e433e,
        name: 'Green Goblin'
      },
      default::Villain {
        id: f8ca354a-b775-11ed-8bef-273145019e1d,
        name: 'Doc Ock'
      },
      default::Villain {
        id: 335f4104-b777-11ed-81eb-ab4de34e9c36,
        name: 'Obadiah Stane'
      },
    }

The double splat also works with type intersection expansion to expand both
properties and links on the specified type.

.. code-block:: edgeql-repl

    db> select Person {
    ...   name,
    ...   [is Hero].**
    ... };
    {
      default::Hero {
        id: 01d9cc22-b776-11ed-8bef-73f84c7e91e7,
        name: 'Spiderman'
        secret_identity: 'Peter Parker',
        villains: {
          default::Villain {
            id: efa2c4bc-b777-11ed-99eb-43f835d79384,
            name: 'Electro'
          },
          default::Villain {
            id: f2c99a96-b775-11ed-8f7e-0b4a4a8e433e,
            name: 'Green Goblin'
          },
          default::Villain {
            id: f8ca354a-b775-11ed-8bef-273145019e1d,
            name: 'Doc Ock'
          }
        }
      },
      default::Hero {
        id: 48edcf8c-b776-11ed-8bef-c7d61b6780d2,
        name: 'Iron Man'
        secret_identity: 'Tony Stark'
        villains: {
          default::Villain {
            id: 335f4104-b777-11ed-81eb-ab4de34e9c36,
            name: 'Obadiah Stane'
          }
        }
      },
      default::Villain {
        id: efa2c4bc-b777-11ed-99eb-43f835d79384,
        name: 'Electro'
      },
      default::Villain {
        id: f2c99a96-b775-11ed-8f7e-0b4a4a8e433e,
        name: 'Green Goblin'
      },
      default::Villain {
        id: f8ca354a-b775-11ed-8bef-273145019e1d,
        name: 'Doc Ock'
      },
      default::Villain {
        id: 335f4104-b777-11ed-81eb-ab4de34e9c36,
        name: 'Obadiah Stane'
      },
    }

With this query, we get ``name`` for each ``Person`` and all the properties and
one level of links on the ``Hero`` objects. We don't get ``Villain`` objects'
nemeses because that link is not covered by our double splat which only
expans ``Hero`` links. If the ``Villain`` type had properties defined on it, we
wouldn't get those with this query either.


.. _ref_eql_select_filter:

Filtering
---------

To filter the set of selected objects, use a ``filter <expr>`` clause. The
``<expr>`` that follows the ``filter`` keyword can be *any boolean expression*.

To reference the ``name`` property of the ``Villain`` objects being selected,
we use ``Villain.name``.

.. code-block:: edgeql-repl

  db> select Villain {id, name}
  ... filter Villain.name = "Doc Ock";
  {default::Villain {id: b233ca98..., name: 'Doc Ock'}}


.. note::

  This query contains two occurrences of ``Villain``. The first
  (outer) is passed as the argument to ``select`` and refers to the set of all
  ``Villain`` objects. However the *inner* occurrence is inside the *scope* of
  the ``select`` statement and refers to the *object being
  selected*.

However, this looks a little clunky, so EdgeQL provides a shorthand: just drop
``Villain`` entirely and simply use ``.name``. Since we are selecting a set of
Villains, it's clear from context that ``.name`` must refer to a link/property
of the ``Villain`` type. In other words, we are in the **scope** of the
``Villain`` type.

.. code-block:: edgeql-repl

  db> select Villain {name}
  ... filter .name = "Doc Ock";
  {default::Villain {name: 'Doc Ock'}}

Learn to filter your queries by trying it in `our interactive filters
tutorial </tutorial/basic-queries/config>`_.

Filtering by ID
^^^^^^^^^^^^^^^

To filter by ``id``, remember to cast the desired ID to :ref:`uuid
<ref_std_uuid>`:

.. code-block:: edgeql-repl

  db> select Villain {id, name}
  ... filter .id = <uuid>"b233ca98-3c23-11ec-b81f-6ba8c4f0084e";
  {
    default::Villain {
      id: 'b233ca98-3c23-11ec-b81f-6ba8c4f0084e',
      name: 'Doc Ock'
    }
  }

Nested filters
^^^^^^^^^^^^^^

Filters can be added at every level of shape nesting. The query below applies a
filter to both the selected ``Hero`` objects and their linked ``villains``.

.. code-block:: edgeql-repl

  db> select Hero {
  ...   name,
  ...   villains: {
  ...     name
  ...   } filter .name ilike "%er"
  ... } filter .name ilike "%man";
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

Note that the *scope* changes inside nested shapes. When we use ``.name`` in
the outer ``filter``, it refers to the name of the hero. But when we use
``.name`` in the nested ``villains`` shape, the scope has changed to
``Villain``.

.. _ref_eql_select_order:

Ordering
--------

Order the result of a query with an ``order by`` clause.

.. code-block:: edgeql-repl

  db> select Villain { name }
  ... order by .name;
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

The expression provided to ``order by`` may be *any* singleton
expression, primitive or otherwise.

.. note::

  In EdgeDB all values are orderable. Objects are compared using their ``id``;
  tuples and arrays are compared element-by-element from left to right. By
  extension, the generic comparison operators :eql:op:`= <eq>`,
  :eql:op:`\< <lt>`, :eql:op:`\> <gt>`, etc. can be used with any two
  expressions of the same type.

You can also order by multiple
expressions and specify the *direction* with an ``asc`` (default) or ``desc``
modifier.

.. note::

  When ordering by multiple expressions, arrays, or tuples, the leftmost
  expression/element is compared. If these elements are the same, the next
  element is used to "break the tie", and so on. If all elements are the same,
  the order is not well defined.

.. code-block:: edgeql-repl

  db> select Movie { title, release_year }
  ... order by
  ...   .release_year desc then
  ...   str_trim(.title) desc;
  {
    default::Movie {title: 'Spider-Man: No Way Home', release_year: 2021},
    ...
    default::Movie {title: 'Iron Man', release_year: 2008},
  }

When ordering by multiple expressions, each expression is separated with the
``then`` keyword. For a full reference on ordering, including how empty values
are handled, see :ref:`Reference > Commands > Select
<ref_reference_select_order>`.


.. _ref_eql_select_pagination:

Pagination
----------

EdgeDB supports ``limit`` and ``offset`` clauses. These are
typically used in conjunction with ``order by`` to maintain a consistent
ordering across pagination queries.

.. code-block:: edgeql-repl

  db> select Villain { name }
  ... order by .name
  ... offset 3
  ... limit 3;
  {
    default::Villain {name: 'Hela'},
    default::Villain {name: 'Justin Hammer'},
    default::Villain {name: 'Kraven the Hunter'},
  }

The expressions passed to ``limit`` and ``offset`` can be any singleton
``int64`` expression. This query fetches all Villains except the last (sorted
by name).

.. code-block:: edgeql-repl

  db> select Villain {name}
  ... order by .name
  ... limit count(Villain) - 1;
  {
    default::Villain {name: 'Abomination'},
    default::Villain {name: 'Doc Ock'},
    ...
    default::Villain {name: 'Winter Soldier'}, # no Zemo
  }


.. _ref_eql_select_computeds:

Computed fields
---------------

Shapes can contain *computed fields*. These are EdgeQL expressions that are
computed on the fly during the execution of the query. As with other clauses,
we can use :ref:`leading dot notation <ref_dot_notation>` (e.g. ``.name``) to
refer to the properties and links of the object type currently *in scope*.


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


.. _ref_eql_select_backlinks:

Backlinks
---------

Fetching backlinks is a common use case for computed fields. To demonstrate
this, let's fetch a list of all movies starring a particular Hero.

.. code-block:: edgeql-repl

  db> select Hero {
  ...   name,
  ...   movies := .<characters[is Movie] { title }
  ... } filter .name = "Iron Man";
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

  The computed backlink ``movies`` is a combination of the *backlink
  operator* ``.<`` and a type intersection ``[is Movie]``. For a full
  reference on backlink syntax, see :ref:`EdgeQL > Paths
  <ref_eql_paths_backlinks>`.

Instead of re-declaring backlinks inside every query where they're needed, it's
common to add them directly into your schema as computed links.

.. code-block:: sdl-diff
    :version-lt: 3.0

      abstract type Person {
        required property name -> str {
          constraint exclusive;
        };
    +   multi link movies := .<characters[is Movie]
      }

.. code-block:: sdl-diff

      abstract type Person {
        required name: str {
          constraint exclusive;
        };
    +   multi link movies := .<characters[is Movie]
      }

.. note::

  In the example above, the ``Person.movies`` is a ``multi`` link. Including
  these keywords is optional, since EdgeDB can infer this from the assigned
  expression ``.<characters[is Movie]``. However, it's a good practice to
  include the explicit keywords to make the schema more readable and "sanity
  check" the cardinality.

This simplifies future queries; ``Person.movies`` can now be traversed in
shapes just like a non-computed link.

.. code-block:: edgeql

  select Hero {
    name,
    movies: { title }
  } filter .name = "Iron Man";



.. _ref_eql_select_subqueries:

Subqueries
----------

There's no limit to the complexity of computed expressions. EdgeQL is designed
to be fully composable; entire queries can be embedded inside each other.
Below, we use a subquery to select all movies containing a villain's nemesis.

.. code-block:: edgeql-repl

  db> select Villain {
  ...   name,
  ...   nemesis_name := .nemesis.name,
  ...   movies_with_nemesis := (
  ...     select Movie { title }
  ...     filter Villain.nemesis in .characters
  ...   )
  ... };
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

.. _ref_eql_select_polymorphic:

Polymorphic queries
-------------------

:index: poly polymorphism nested shapes

All queries thus far have referenced concrete object types: ``Hero`` and
``Villain``. However, both of these types extend the abstract type ``Person``,
from which they inherit the ``name`` property.

To learn how to leverage polymorphism in your queries, see `our interactive
polymorphism tutorial
</tutorial/nested-structures/polymorphism>`_.

Polymorphic sets
^^^^^^^^^^^^^^^^

It's possible to directly query all ``Person`` objects; the resulting set will
be a mix of ``Hero`` and ``Villain`` objects (and possibly other subtypes of
``Person``, should they be declared).

.. code-block:: edgeql-repl

  db> select Person { name };
  {
    default::Villain {name: 'Abomination'},
    default::Villain {name: 'Zemo'},
    default::Hero {name: 'The Hulk'},
    default::Hero {name: 'Iron Man'},
    ...
  }

You may also encounter such "mixed sets" when querying a link that points to an
abstract type (such as ``Movie.characters``) or a :eql:op:`union type
<typeor>`.

.. code-block:: edgeql-repl

  db> select Movie {
  ...   title,
  ...   characters: {
  ...     name
  ...   }
  ... }
  ... filter .title = "Iron Man 2";
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


Polymorphic fields
^^^^^^^^^^^^^^^^^^

We can fetch different properties *conditional* on the subtype of each object
by prefixing property/link references with ``[is <type>]``. This is known as a
**polymorphic query**.

.. code-block:: edgeql-repl

  db> select Person {
  ...   name,
  ...   secret_identity := [is Hero].secret_identity,
  ...   number_of_villains := count([is Hero].villains),
  ...   nemesis := [is Villain].nemesis {
  ...     name
  ...   }
  ... };
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

This syntax might look familiar; it's the :ref:`type intersection
<ref_eql_types_intersection>` again. In effect, this operator conditionally
returns the value of the referenced field only if the object matches a
particular type. If the match fails, an empty set is returned.

The line ``secret_identity := [is Hero].secret_identity`` is a bit redundant,
since the computed property has the same name as the polymorphic field. In
these cases, EdgeQL supports a shorthand.

.. code-block:: edgeql-repl

  db> select Person {
  ...   name,
  ...   [is Hero].secret_identity,
  ...   [is Villain].nemesis: {
  ...     name
  ...   }
  ... };
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

Filtering polymorphic links
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Relatedly, it's possible to filter polymorphic links by subtype. Below, we
exclusively fetch the ``Movie.characters`` of type ``Hero``.

.. code-block:: edgeql-repl

  db> select Movie {
  ...   title,
  ...   characters[is Hero]: {
  ...     secret_identity
  ...   },
  ... };
  {
    default::Movie {
      title: 'Spider-Man: Homecoming',
      characters: {default::Hero {secret_identity: 'Peter Parker'}},
    },
    default::Movie {
      title: 'Iron Man',
      characters: {default::Hero {secret_identity: 'Tony Stark'}},
    },
    ...
  }

.. _ref_eql_select_free_objects:

Free objects
------------

To select several values simultaneously, you can "bundle" them into a "free
object". Free objects are a set of key-value pairs that can contain any
expression. Here, the term "free" is used to indicate that the object in
question is not an instance of a particular *object type*; instead, it's
constructed ad hoc inside the query.

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


Note that the result is a *singleton* but each key corresponds to a set of
values, which may have any cardinality.

.. _ref_eql_select_with:

With block
----------

All top-level EdgeQL statements (``select``, ``insert``, ``update``, and
``delete``) can be prefixed with a ``with`` block. These blocks let you declare
standalone expressions that can be used in your query.

.. code-block:: edgeql-repl

  db> with hero_name := "Iron Man"
  ... select Hero { secret_identity }
  ... filter .name = hero_name;
  {default::Hero {secret_identity: 'Tony Stark'}}


For full documentation on ``with``, see :ref:`EdgeQL > With <ref_eql_with>`.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Reference > Commands > Select <ref_eql_statements_select>`
  * - :ref:`Cheatsheets > Selecting data <ref_cheatsheet_select>`
  * - `Tutorial > Basic Queries > Objects
      </tutorial/basic-queries/objects>`_
  * - `Tutorial > Basic Queries > Filters
      </tutorial/basic-queries/config>`_
  * - `Tutorial > Basic Queries > Aggregates
      </tutorial/basic-queries/aggregate-functions>`_
  * - `Tutorial > Nested Structures > Shapes
      </tutorial/nested-structures/shapes>`_
  * - `Tutorial > Nested Structures > Polymorphism
      </tutorial/nested-structures/polymorphism>`_
