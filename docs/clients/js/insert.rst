.. _edgedb-js-insert:

Insert
------

Insert new data with ``e.insert``.

.. code-block:: typescript

  e.insert(e.Movie, {
    title: e.str("Spider-Man: No Way Home"),
    release_year: e.int64(2021)
  });

For convenience, the second argument of ``e.insert`` function can also accept
plain JS data or a named tuple.

.. code-block:: typescript

  e.insert(e.Movie, {
    title: "Spider-Man: No Way Home",
    actors: e.select(e.Person, person => ({
      filter: e.op(person.name, "=", "Robert Downey Jr."),
      '@character_name': e.str("Iron Man")
    }))
  });


.. code-block:: typescript

  e.params({
    movie: e.tuple({
      title: e.str,
      release_year: e.int64
    })
  }, $ =>
    e.insert(e.Movie, $.movie)
  );


Link properties
^^^^^^^^^^^^^^^

As in EdgeQL, link properties are inserted inside the shape of a subquery.

.. code-block:: typescript

  const query = e.insert(e.Movie, {
    title: "Iron Man",
    actors: e.select(e.Person, person => ({
      filter_single: {name: "Robert Downey Jr."},
      "@character_name": e.str("Tony Stark")

      // link props must correspond to expressions
      "@character_name": "Tony Stark"  // invalid
    ))
  });


.. note::

  For technical reasons, link properties must correspond to query
  builder expressions, not plain JS data.

Similarly you can directly include link properties inside nested ``e.insert``
queries:

.. code-block:: typescript

  const query = e.insert(e.Movie, {
    title: "Iron Man",
    release_year: 2008,
    actors: e.insert(e.Person, {
      name: "Robert Downey Jr.",
      "@character_name": e.str("Tony Start")
    }),
  });

Handling conflicts
^^^^^^^^^^^^^^^^^^
:index: querybuilder unlessconflict unless conflict constraint

In EdgeQL, "upsert" functionality is achieved by handling **conflicts** on
``insert`` statements with the ``unless conflict`` clause. In the query
builder, this is possible with the ``.unlessConflict`` method (available only
on ``insert`` expressions).

In the simplest case, adding ``.unlessConflict`` (no arguments) will prevent
EdgeDB from throwing an error if the insertion would violate an exclusivity
contstraint. Instead, the query returns an empty set (``null``).

.. code-block:: typescript

  e.insert(e.Movie, {
    title: "Spider-Man: No Way Home",
    release_year: 2021
  }).unlessConflict();
  // => null


Provide an ``on`` clause to "catch" conflicts only on a specific property/link.

.. code-block:: typescript

  e.insert(e.Movie, {
    title: "Spider-Man: No Way Home",
    release_year: 2021
  }).unlessConflict(movie => ({
    on: movie.title, // can be any expression
  }));


You can also provide an ``else`` expression which will be executed and returned
in case of a conflict. You must specify an ``on`` clause in order to use ``else``.

The following query simply returns the pre-existing (conflicting) object.

.. code-block:: typescript

  e.insert(e.Movie, {
    title: "Spider-Man: Homecoming",
    release_year: 2021
  }).unlessConflict(movie => ({
    on: movie.title,
    else: movie
  }));


Or you can perform an upsert operation with an ``e.update`` in the ``else``.

.. code-block:: typescript

  e.insert(e.Movie, {
    title: "Spider-Man: Homecoming",
    release_year: 2021
  }).unlessConflict(movie => ({
    on: movie.title,
    else: e.update(movie, () => ({
      set: {
        release_year: 2021
      }
    })),
  }));


If the constraint you're targeting is a composite constraint, wrap the
properties in a tuple.

.. code-block:: typescript

  e.insert(e.Movie, {
    title: "Spider-Man: No Way Home",
    release_year: 2021
  }).unlessConflict(movie => ({
    on: e.tuple([movie.title, movie.release_year])
  }));

Bulk inserts
^^^^^^^^^^^^

You can use a :ref:`for loop <edgedb-js-for>` to perform :ref:`bulk inserts
<edgedb-js-for-bulk-inserts>`.
