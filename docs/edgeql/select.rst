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
demonstration purposes, the queries below assume the following schema:

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
    :version-lt: 4.0

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

.. code-block:: sdl

    module default {
      abstract type Person {
        required name: str { constraint exclusive };
      }

      type Hero extending Person {
        secret_identity: str;
        multi villains := .<nemesis[is Villain];
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

And the following inserts:

.. code-block:: edgeql-repl

  db> insert Hero {
  ...   name := "Spider-Man",
  ...   secret_identity := "Peter Parker"
  ... };
  {default::Hero {id: 6be1c9c6...}}

  db> insert Hero {
  ...   name := "Iron Man",
  ...   secret_identity := "Tony Stark"
  ... };
  {default::Hero {id: 6bf7115a... }}

  db> for n in { "Sandman", "Electro", "Green Goblin", "Doc Ock" }
  ...   union (
  ...     insert Villain {
  ...     name := n,
  ...     nemesis := (select Hero filter .name = "Spider-Man")
  ...  });
  {
    default::Villain {id: 6c22bdf0...},
    default::Villain {id: 6c22c3d6...},
    default::Villain {id: 6c22c46c...},
    default::Villain {id: 6c22c502...},
  }

  db> insert Villain {
  ...   name := "Obadiah Stane",
  ...   nemesis := (select Hero filter .name = "Iron Man")
  ... };
  {default::Villain {id: 6c42c4ec...}}

  db> insert Movie {
  ...  title := "Spider-Man: No Way Home",
  ...  release_year := 2021,
  ...  characters := (select Person filter .name in
  ...    { "Spider-Man", "Sandman", "Electro", "Green Goblin", "Doc Ock" })
  ...  };
  {default::Movie {id: 6c60c28a...}}

  db> insert Movie {
  ...  title := "Iron Man",
  ...  release_year := 2008,
  ...  characters := (select Person filter .name in
  ...   { "Iron Man", "Obadiah Stane" })
  ...  };
  {default::Movie {id: 6d1f430e...}}

Let's start by selecting all ``Villain`` objects in the database. In this
example, there are only five. Remember, ``Villain`` is a :ref:`reference
<ref_eql_set_references>` to the set of all Villain objects.

.. code-block:: edgeql-repl

  db> select Villain;
  {
    default::Villain {id: 6c22bdf0...},
    default::Villain {id: 6c22c3d6...},
    default::Villain {id: 6c22c46c...},
    default::Villain {id: 6c22c502...},
    default::Villain {id: 6c42c4ec...},
  }

.. note::

  For the sake of readability, the ``id`` values have been truncated.

By default, this only returns the ``id`` of each object. If serialized to JSON,
this result would look like this:

.. code-block::

  [
    {"id": "6c22bdf0-5c03-11ee-99ff-dfaea4d947ce"},
    {"id": "6c22c3d6-5c03-11ee-99ff-734255881e5d"},
    {"id": "6c22c46c-5c03-11ee-99ff-c79f24cf638b"},
    {"id": "6c22c502-5c03-11ee-99ff-cbacc3918129"},
    {"id": "6c42c4ec-5c03-11ee-99ff-872c9906a467"}
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
    default::Villain {id: 6c22bdf0..., name: 'Sandman'},
    default::Villain {id: 6c22c3d6..., name: 'Electro'},
    default::Villain {id: 6c22c46c..., name: 'Green Goblin'},
    default::Villain {id: 6c22c502..., name: 'Doc Ock'},
    default::Villain {id: 6c42c4ec..., name: 'Obadiah Stane'},
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
      name: 'Sandman',
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
        default::Villain {name: 'Sandman'},
        default::Villain {name: 'Electro'},
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

.. edb:youtube-embed:: 9-I1qjIp3KI

Splats will help you more easily select all properties when using the REPL.
You can select all of an object's properties using the single splat:

.. code-block:: edgeql-repl

    db> select Movie {*};
    {
      default::Movie {
        id: 6c60c28a-5c03-11ee-99ff-dfa425012a05,
        release_year: 2021,
        title: 'Spider-Man: No Way Home',
      },
      default::Movie {
        id: 6d1f430e-5c03-11ee-99ff-e731e8da06d9,
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
        id: 6c60c28a-5c03-11ee-99ff-dfa425012a05,
        release_year: 2021,
        title: 'Spider-Man: No Way Home',
        characters: {
          default::Hero {
            id: 6be1c9c6-5c03-11ee-99ff-63b1127d75f2,
            name: 'Spider-Man'
          },
          default::Villain {
            id: 6c22bdf0-5c03-11ee-99ff-dfaea4d947ce,
            name: 'Sandman'
          },
          default::Villain {
            id: 6c22c3d6-5c03-11ee-99ff-734255881e5d,
            name: 'Electro'
          },
          default::Villain {
            id: 6c22c46c-5c03-11ee-99ff-c79f24cf638b,,
            name: 'Green Goblin'
          },
          default::Villain {
            id: 6c22c502-5c03-11ee-99ff-cbacc3918129,
            name: 'Doc Ock'
          },
        },
      },
      default::Movie {
        id: 6d1f430e-5c03-11ee-99ff-e731e8da06d9,
        release_year: 2008,
        title: 'Iron Man',
        characters: {
          default::Hero {
            id: 6bf7115a-5c03-11ee-99ff-c79c07f0e2db,
            name: 'Iron Man'
          },
          default::Villain {
            id: 6c42c4ec-5c03-11ee-99ff-872c9906a467,
            name: 'Obadiah Stane'
          },
        },
      },
    }

.. note::

    Splats are not yet supported in function bodies.

The splat expands all properties defined on the type as well as inherited
properties:

.. code-block:: edgeql-repl

    db> select Hero {*};
    {
      default::Hero {
        id: 6be1c9c6-5c03-11ee-99ff-63b1127d75f2,
        name: 'Spider-Man',
        secret_identity: 'Peter Parker'
      },
      default::Hero {
        id: 6bf7115a-5c03-11ee-99ff-c79c07f0e2db,
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
        id: 6be1c9c6-5c03-11ee-99ff-63b1127d75f2,
        name: 'Spider-Man'
      },
      default::Hero {
        id: 6bf7115a-5c03-11ee-99ff-c79c07f0e2db,
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
        name: 'Spider-Man',
        id: 6be1c9c6-5c03-11ee-99ff-63b1127d75f2,
        secret_identity: 'Peter Parker'
      },
      default::Hero {
        name: 'Iron Man'
        id: 6bf7115a-5c03-11ee-99ff-c79c07f0e2db,
        secret_identity: 'Tony Stark'
      },
      default::Villain {
        name: 'Sandman',
        id: 6c22bdf0-5c03-11ee-99ff-dfaea4d947ce,
        secret_identity: {}
      },
      default::Villain {
        name: 'Electro',
        id: 6c22c3d6-5c03-11ee-99ff-734255881e5d,
        secret_identity: {}
      },
      default::Villain {
        name: 'Green Goblin',
        id: 6c22c46c-5c03-11ee-99ff-c79f24cf638b,
        secret_identity: {}
      },
      default::Villain {
        name: 'Doc Ock',
        id: 6c22c502-5c03-11ee-99ff-cbacc3918129,
        secret_identity: {}
      },
      default::Villain {
        name: 'Obadiah Stane',
        id: 6c42c4ec-5c03-11ee-99ff-872c9906a467,
        secret_identity: {}
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
      default::Villain {
        name: 'Sandman',
        id: 6c22bdf0-5c03-11ee-99ff-dfaea4d947ce,
        secret_identity: {},
        villains: {}
      },
      default::Villain {
        name: 'Electro',
        id: 6c22c3d6-5c03-11ee-99ff-734255881e5d,
        secret_identity: {},
        villains: {}
      },
      default::Villain {
        name: 'Green Goblin',
        id: 6c22c46c-5c03-11ee-99ff-c79f24cf638b,
        secret_identity: {},
        villains: {}
      },
      default::Villain {
        name: 'Doc Ock',
        id: 6c22c502-5c03-11ee-99ff-cbacc3918129,
        secret_identity: {},
        villains: {}
      },
      default::Villain {
        name: 'Obadiah Stane',
        id: 6c42c4ec-5c03-11ee-99ff-872c9906a467,
        secret_identity: {},
        villains: {}
      },
      default::Hero {
        name: 'Spider-Man',
        id: 6be1c9c6-5c03-11ee-99ff-63b1127d75f2,
        secret_identity: 'Peter Parker',
        villains: {
          default::Villain {
            name: 'Electro',
            id: 6c22c3d6-5c03-11ee-99ff-734255881e5d
          },
          default::Villain {
            name: 'Sandman',
            id: 6c22bdf0-5c03-11ee-99ff-dfaea4d947ce
          },
          default::Villain {
            name: 'Doc Ock',
            id: 6c22c502-5c03-11ee-99ff-cbacc3918129
          },
          default::Villain {
            name: 'Green Goblin',
            id: 6c22c46c-5c03-11ee-99ff-c79f24cf638b
          },
        },
      },
    }

With this query, we get ``name`` for each ``Person`` and all the properties and
one level of links on the ``Hero`` objects. We don't get ``Villain`` objects'
nemeses because that link is not covered by our double splat which only
expands ``Hero`` links. If the ``Villain`` type had properties defined on it,
we wouldn't get those with this query either.


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
  {default::Villain {id: 6c22c502..., name: 'Doc Ock'}}


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

.. warning::

    When using comparison operators like ``=`` or ``!=``, or boolean operators
    ``and``, ``or``, and ``not``, keep in mind that these operators will
    produce an empty set if an operand is an empty set. Check out :ref:`our
    boolean cheatsheet <ref_cheatsheet_boolean>` for more info and help on how
    to mitigate this if you know your operands may be an empty set.

Learn to filter your queries by trying it in `our interactive filters
tutorial </tutorial/basic-queries/config>`_.

Filtering by ID
^^^^^^^^^^^^^^^

To filter by ``id``, remember to cast the desired ID to :ref:`uuid
<ref_std_uuid>`:

.. code-block:: edgeql-repl

  db> select Villain {id, name}
  ... filter .id = <uuid>"6c22c502-5c03-11ee-99ff-cbacc3918129";
  {
    default::Villain {
      id: '6c22c502-5c03-11ee-99ff-cbacc3918129',
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
  ...   } filter .name like "%O%"
  ... } filter .name ilike "%man";
  {
    default::Hero {
      name: 'Spider-Man',
      villains: {
        default::Villain {
          name: 'Doc Ock'
        }
      }
    },
    default::Hero {
      name: 'Iron Man',
      villains: {
        default::Villain {
          name: 'Obadiah Stane'
        }
      }
    },
  }

Note that the *scope* changes inside nested shapes. When we use ``.name`` in
the outer ``filter``, it refers to the name of the hero. But when we use
``.name`` in the nested ``villains`` shape, the scope has changed to
``Villain``.

Filtering on a known backlink
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Another handy use for backlinks is using them to filter and find items
when doing a ``select`` (or an ``update`` or other operation, of course).
This can work as a nice shortcut when you have the ID of one object that
links to a second object without a link back to the first.

Spider-Man's villains always have a grudging respect for him, and their names
can be displayed to reflect that if we know the ID of a movie that they
starred in. (Note the ability to :ref:`cast from a uuid <ref_uuid_casting>`
to an object type, which was added in EdgeDB 3.0!)

.. code-block:: edgeql-repl
    
    db> select Villain filter .<characters = 
    ...   <Movie><uuid>'6c60c28a-5c03-11ee-99ff-dfa425012a05' { 
    ...     name := .name ++ ', who got to see Spider-Man!' 
    ...   };
    {
      'Obadiah Stane',
      'Sandman, who got to see Spider-Man!',
      'Electro, who got to see Spider-Man!',
      'Green Goblin, who got to see Spider-Man!',
      'Doc Ock, who got to see Spider-Man!',
    }

In other words, "select every ``Villain`` object that the ``Movie`` object
of this ID links to via a link called ``characters``".

A backlink is naturally not required, however. The same operation without
traversing a backlink would look like this:

.. code-block:: edgeql-repl

    db> with movie := 
    ...   <Movie><uuid>'6c60c28a-5c03-11ee-99ff-dfa425012a05',
    ...     select movie.characters[is Villain] {
    ...       name := .name ++ ', who got to see Spider-Man!'
    ...   };


.. _ref_eql_select_order:

Ordering
--------

Order the result of a query with an ``order by`` clause.

.. code-block:: edgeql-repl

  db> select Villain { name }
  ... order by .name;
  {
    default::Villain {name: 'Doc Ock'},
    default::Villain {name: 'Electro'},
    default::Villain {name: 'Green Goblin'},
    default::Villain {name: 'Obadiah Stane'},
    default::Villain {name: 'Sandman'},
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
  ... offset 2
  ... limit 2;
  {
    default::Villain {name: 'Obadiah Stane'},
    default::Villain {name: 'Sandman'},
  }

The expressions passed to ``limit`` and ``offset`` can be any singleton
``int64`` expression. This query fetches all Villains except the last (sorted
by name).

.. code-block:: edgeql-repl

  db> select Villain {name}
  ... order by .name
  ... limit count(Villain) - 1;
  {
    default::Villain {name: 'Doc Ock'},
    default::Villain {name: 'Electro'},
    default::Villain {name: 'Green Goblin'},
    default::Villain {name: 'Obadiah Stane'}, # no Sandman
  }

You may pass the empty set to ``limit`` or ``offset``. Passing the empty set is
effectively the same as excluding ``limit`` or ``offset`` from your query
(i.e., no limit or no offset). This is useful if you need to parameterize
``limit`` and/or ``offset`` but may still need to execute your query without
providing one or the other.

.. code-block:: edgeql-repl

  db> select Villain {name}
  ... order by .name
  ... offset <optional int64>$offset
  ... limit <optional int64>$limit;
  Parameter <int64>$offset (Ctrl+D for empty set `{}`):
  Parameter <int64>$limit (Ctrl+D for empty set `{}`):
  {
    default::Villain {name: 'Doc Ock'},
    default::Villain {name: 'Electro'},
    ...
  }

.. note::

    If you parameterize ``limit`` and ``offset`` and want to reserve the option
    to pass the empty set, make sure those parameters are ``optional`` as shown
    in the example above.


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
      id: 6c22bdf0...,
      name: 'Sandman',
      name_upper: 'SANDMAN',
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
      id: 6c22bdf0...,
      name: 'Sandman',
      name_upper: 'SANDMAN',
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
        default::Movie {title: 'Iron Man'}
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
    :version-lt: 4.0

      abstract type Person {
        required name: str {
          constraint exclusive;
        };
    +   multi link movies := .<characters[is Movie]
      }

.. code-block:: sdl-diff

      abstract type Person {
        required name: str {
          constraint exclusive;
        };
    +   multi movies := .<characters[is Movie]
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
      name: 'Sandman',
      nemesis_name: 'Spider-Man',
      movies_with_nemesis: {
        default::Movie {title: 'Spider-Man: No Way Home'}
      }
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
    default::Hero {name: 'Spider-Man'},
    default::Hero {name: 'Iron Man'},
    default::Villain {name: 'Doc Ock'},
    default::Villain {name: 'Obadiah Stane'},
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
      title: 'Iron Man',
      characters: {
        default::Villain {name: 'Obadiah Stane'},
        default::Hero {name: 'Iron Man'}
      }
    }
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
    ...
    default::Villain {
      name: 'Obadiah Stane',
      secret_identity: {},
      number_of_villains: 0,
      nemesis: default::Hero {
        name: 'Iron Man'
      }
    },
    default::Hero {
      name: 'Spider-Man',
      secret_identity: 'Peter Parker',
      number_of_villains: 4,
      nemesis: {}
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
    ...
    default::Villain {
      name: 'Obadiah Stane',
      secret_identity: {},
      nemesis: default::Hero {name: 'Iron Man'}
    },
    default::Hero {
      name: 'Spider-Man',
      secret_identity: 'Peter Parker',
      nemesis: {}
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
      title: 'Spider-Man: No Way Home',
      characters: {default::Hero {secret_identity: 'Peter Parker'}},
    },
    default::Movie {
      title: 'Iron Man',
      characters: {default::Hero {secret_identity: 'Tony Stark'}},
    },
    ...
  }

Accessing types in polymorphic queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

While the type of an object is displayed alongside the results of polymorphic
queries run in the REPL, this is simply a convenience of the REPL and not a
property that can be accessed. This is particularly noticeable if you cast an
object to ``json``, making it impossible to determine the type if the query is
polymorphic. First, the result of a query as the REPL presents it with type
annotations displayed:

.. code-block:: edgeql-repl

    db> select Person limit 1;
    {default::Villain {id: 6c22bdf0-5c03-11ee-99ff-dfaea4d947ce}}

Note the type ``default::Villain``, which is displayed for the user's
convenience but is not actually part of the data returned. This is the same
query with the result cast as ``json`` to show only the data returned:

.. code-block:: edgeql-repl

    db> select <json>Person limit 1;
    {Json("{\"id\": \"6c22bdf0-5c03-11ee-99ff-dfaea4d947ce\"}")}

.. note::

    We will continue to cast subesequent examples in this section to ``json``,
    not because this is required for any of the functionality being
    demonstrated, but to remove the convenience type annotations provided by
    the REPL and make it easier to see what data is actually being returned by
    the query.

The type of an object is found inside ``__type__`` which is a link that
carries various information about the object's type, including its ``name``.

.. code-block:: edgeql-repl

    db> select <json>Person {
    ...  __type__: {
    ...    name
    ...    }
    ...  } limit 1;
    {Json("{\"__type__\": {\"name\": \"default::Villain\"}}")}

This information can be pulled into the top level by assigning a name to
the ``name`` property inside ``__type__``:

.. code-block:: edgeql-repl

    db> select <json>Person { type := .__type__.name } limit 1;
    {Json("{\"type\": \"default::Villain\"}")}

There is nothing magical about ``__type__``; it is a simple link to an object
of the type ``ObjectType`` which contains all of the possible information to
know about the type of the current object. The splat operator can be used to
see this object's makeup, while the double splat operator produces too much
output to show on this page. Playing around with the splat and double splat
operator inside ``__type__`` is a quick way to get some insight into the
internals of EdgeDB.

.. code-block:: edgeql-repl

    db> select Person.__type__ {*} limit 1;
    {
      schema::ObjectType {
        id: 48be3a94-5bf3-11ee-bd60-0b44b607e31d,
        name: 'default::Hero',
        internal: false,
        builtin: false,
        computed_fields: [],
        final: false,
        is_final: false,
        abstract: false,
        is_abstract: false,
        inherited_fields: [],
        from_alias: false,
        is_from_alias: false,
        expr: {},
        compound_type: false,
        is_compound_type: false,
      },
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
        default::Hero {name: 'Spider-Man'},
        default::Hero {name: 'Iron Man'},
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
