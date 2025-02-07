.. _edgedb-js-for:


For Loops
=========

``for`` loops let you iterate over any set of values.

.. code-block:: typescript

  const query = e.for(e.set(1, 2, 3, 4), (number) => {
    return e.op(2, '^', number);
  });
  const result = query.run(client);
  // [2, 4, 8, 16]

.. _edgedb-js-for-bulk-inserts:

Bulk inserts
------------

It's common to use ``for`` loops to perform bulk inserts. The raw data is
passed in as a ``json`` parameter, converted to a set of ``json`` objects with
``json_array_unpack``, then passed into a ``for`` loop for insertion.

.. code-block:: typescript

  const query = e.params({ items: e.json }, (params) => {
    return e.for(e.json_array_unpack(params.items), (item) => {
      return e.insert(e.Movie, {
        title: e.cast(e.str, item.title),
        release_year: e.cast(e.int64, item.release_year),
      });
    });
  });

  const result = await query.run(client, {
    items: [
      { title: "Deadpool", release_year: 2016 },
      { title: "Deadpool 2", release_year: 2018 },
      { title: "Deadpool 3", release_year: 2024 },
      { title: "Deadpool 4", release_year: null },
    ],
  });

Note that any optional properties values must be explicitly set to ``null``.
They cannot be set to ``undefined`` or omitted; doing so will cause a runtime
error.

.. _edgedb-js-for-bulk-inserts-conflicts:

Handling conflicts in bulk inserts
----------------------------------

Here's a more complex example, demonstrating how to complete a nested insert
with conflicts on the inner items. First, take a look at the schema for this
database:

.. code-block:: sdl

    module default {
      type Character {
        required name: str {
          constraint exclusive;
        }
        portrayed_by: str;
        multi movies: Movie;
      }

      type Movie {
        required title: str {
          constraint exclusive;
        };
        release_year: int64;
      }
    }

Note that the ``Movie`` type's ``title`` property has an exclusive constraint.

Here's the data we want to bulk insert:

.. code-block:: js

    [
      {
        portrayed_by: "Robert Downey Jr.",
        name: "Iron Man",
        movies: ["Iron Man", "Iron Man 2", "Iron Man 3"]
      },
      {
        portrayed_by: "Chris Evans",
        name: "Captain America",
        movies: [
          "Captain America: The First Avenger",
          "The Avengers",
          "Captain America: The Winter Soldier",
        ]
      },
      {
        portrayed_by: "Mark Ruffalo",
        name: "The Hulk",
        movies: ["The Avengers", "Iron Man 3", "Avengers: Age of Ultron"]
      }
    ]

This is potentially a problem because some of the characters appear in the same
movies. We can't just naively insert all the movies because we'll eventually
hit a conflict. Since we're going to write this as a single query, chaining
``.unlessConflict`` on our query won't help. It only handles conflicts with
objects that existed *before* the current query.

Let's look at a query that can accomplish this insert, and then we'll break it
down.

.. code-block:: typescript

  const query = e.params(
    {
      characters: e.array(
        e.tuple({
          portrayed_by: e.str,
          name: e.str,
          movies: e.array(e.str),
        })
      ),
    },
    (params) => {
      const movies = e.for(
        e.op(
          "distinct",
          e.array_unpack(e.array_unpack(params.characters).movies)
        ),
        (movieTitle) => {
          return e
            .insert(e.Movie, {
              title: movieTitle,
            })
            .unlessConflict((movie) => ({
              on: movie.title,
              else: movie,
            }));
        }
      );
      return e.with(
        [movies],
        e.for(e.array_unpack(params.characters), (character) => {
          return e.insert(e.Character, {
            name: character.name,
            portrayed_by: character.portrayed_by,
            movies: e.assert_distinct(
              e.select(movies, (movie) => ({
                filter: e.op(movie.title, "in", e.array_unpack(character.movies)),
              }))
            ),
          });
        })
      );
    }
  );

.. _edgedb-js-for-bulk-inserts-conflicts-params:

Structured params
~~~~~~~~~~~~~~~~~

.. code-block:: typescript

  const query = e.params(
    {
      characters: e.array(
        e.tuple({
          portrayed_by: e.str,
          name: e.str,
          movies: e.array(e.str),
        })
      ),
    },
    (params) => { ...

In raw EdgeQL, you can only have scalar types as parameters. We could mirror
that here with something like this: ``e.params({characters: e.json})``, but
this would then require us to cast all the values inside the JSON like
``portrayed_by`` and ``name``.

By doing it this way — typing ``characters`` with ``e.array`` and the character
objects as named tuples by passing an object to ``e.tuple`` — all the data in
the array will be properly cast for us. It will also better type check the data
you pass to the query's ``run`` method.

.. _edgedb-js-for-bulk-inserts-conflicting-data:

Inserting the inner conflicting data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: typescript

  ...
  (params) => {
    const movies = e.for(
      e.op("distinct", e.array_unpack(e.array_unpack(params.characters).movies)),
      (movie) => {
        return e
          .insert(e.Movie, {
            title: movie,
          })
          .unlessConflict((movie) => ({
            on: movie.title,
            else: movie,
          }));
      }
    );
  ...

We need to separate this movie insert query so that we can use ``distinct`` on
it. We could just nest an insert inside our character insert if movies weren't
duplicated across characters (e.g., two characters have "The Avengers" in
``movies``). Even though the query is separated from the character inserts
here, it will still be built as part of a single EdgeDB query using ``with``
which we'll get to a bit later.

The ``distinct`` operator can only operate on sets. We use ``array_unpack`` to
make these arrays into sets. We need to call it twice because
``params.characters`` is an array and ``.movies`` is an array nested inside
each character.

Chaining ``unlessConflict`` takes care of any movies that already exist in the
database *before* we run this query, but it won't handle conflicts that come
about over the course of this query. The ``distinct`` operator we used earlier
pro-actively eliminates any conflicts we might have had among this data.

.. _edgedb-js-for-bulk-inserts-outer-data:

Inserting the outer data
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: typescript

  ...
  return e.with(
    [movies],
    e.for(e.array_unpack(params.characters), (character) => {
      return e.insert(e.Character, {
        name: character.name,
        portrayed_by: character.portrayed_by,
        movies: e.assert_distinct(
          e.select(movies, (movie) => ({
            filter: e.op(movie.title, "in", e.array_unpack(character.movies)),
          }))
        ),
      });
    })
  );
  ...

The query builder will try to automatically use EdgeQL's ``with``, but in this
instance, it doesn't know where to place the ``with``. By using ``e.with``
explicitly, we break our movie insert out to the top-level of the query. By
default, it would be scoped *inside* the query, so our ``distinct`` operator
would be applied only to each character's movies instead of to all of the
movies. This would have caused the query to fail.

The rest of the query is relatively straightforward. We unpack
``params.characters`` to a set so that we can pass it to ``e.for`` to iterate
over the characters. For each character, we build an ``insert`` query with
their ``name`` and ``portrayed_by`` values.

For the character's ``movies``, we ``select`` everything in the
``movies`` insert query we wrote previously, filtering for those with titles
that match values in the ``character.movies`` array.

All that's left is to run the query, passing the data to the query's ``run``
method!

.. _edgedb-js-for-bulk-updates:

Bulk updates
^^^^^^^^^^^^

Just like with inserts, you can run bulk updates using a ``for`` loop. Pass in
your data, iterate over it, and build an ``update`` query for each item.

In this example, we use ``name`` to filter for the character to be updated
since ``name`` has an exclusive constraint in the schema (meaning a given name
will correspond to, at most, a single object). That filtering is done using the
``filter_single`` property of the object returned from your ``update``
callback. Then the ``last_appeared`` value is updated by including it in the
nested ``set`` object.

.. code-block:: typescript

    const query = e.params(
      {
        characters: e.array(
          e.tuple({
            name: e.str,
            last_appeared: e.int64,
          })
        ),
      },
      (params) => {
        return e.for(e.array_unpack(params.characters), (character) => {
          return e.update(e.Character, () => ({
            filter_single: { name: character.name },
            set: {
              last_appeared: character.last_appeared,
            },
          }));
        });
      }
    );

    await query.run(client, {
      characters: [
        { name: "Iron Man", last_appeared: 2019 },
        { name: "Captain America", last_appeared: 2019 },
        { name: "The Hulk", last_appeared: 2021 },
      ],
    });

e.for vs JS for or .forEach
^^^^^^^^^^^^^^^^^^^^^^^^^^^

You may be tempted to use JavaScript's ``for`` or the JavaScript array's
``.forEach`` method to avoid having to massage your data into a set for
consumption by ``e.for``. This approach comes at a cost of performance.

If you use ``for`` or ``.forEach`` to iterate over a standard JavaScript data
structure and run separate queries for each item in your iterable, you're doing
just that: running separate queries for each item in your iterable. By
iterating inside your query using ``e.for``, you're guaranteed everything will
happen in a single query.

In addition to the performance implications, a single query means that either
everything succeeds or everything fails. You will never end up with only some
of your data inserted. This ensures your data integrity is maintained. You
could achieve this yourself by wrapping your batch queryies with :ref:`a
transaction <edgedb-js-qb-transaction>`, but a single query is already atomic
without any additional work on your part.

Using ``e.for`` to run a single query is generally the best approach. When
dealing with extremely large datasets, it may become more practical to batch
queries and run them individually.
