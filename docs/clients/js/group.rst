.. _edgedb-js-group:

Group
=====

.. note::

  The ``group`` statement is only available in EdgeDB 2.0 or later.

The ``group`` statement provides a powerful mechanism for categorizing a set
of objects (e.g., movies) into *groups*. You can group by properties,
expressions, or combinatations thereof.

.. note::

  This page does not aim to describe how the ``group`` statement works, merely
  the syntax for writing ``e.group`` statements with the query builder. For
  full documentation, refer to :ref:`EdgeQL > Group <ref_eql_group>`.

Simple grouping
---------------

Sort a set of objects by a simple property.

.. tabs::

  .. code-tab:: typescript

    e.group(e.Movie, movie => {
      return {
        by: {release_year: movie.release_year}
      }
    });
    /*
      [
        {
          key: {release_year: 2008},
          grouping: ["release_year"],
          elements: [{id: "..."}, {id: "..."}]
        },
        {
          key: { release_year: 2009 },
          grouping: ["release_year"],
          elements: [{id: "..."}, {id: "..."}]
        },
        // ...
      ]
    */

  .. code-tab:: edgeql

    group Movie
    by .release_year

Add a shape that will be applied to ``elements``. The ``by`` key is a special
key, similar to ``filter``, etc. in ``e.select``. All other keys are
interpreted as *shape elements* and support the same functionality as
``e.select`` (nested shapes, computeds, etc.).

.. tabs::

  .. code-tab:: typescript

    e.group(e.Movie, movie => {
      return {
        title: true,
        actors: {name: true},
        num_actors: e.count(movie.characters),
        by: {release_year: movie.release_year}
      }
    });
    /* [
      {
        key: {release_year: 2008},
        grouping: ["release_year"],
        elements: [{
          title: "Iron Man",
          actors: [...],
          num_actors: 5
        }, {
          title: "The Incredible Hulk",
          actors: [...],
          num_actors: 3
        }]
      },
      // ...
    ] */

  .. code-tab:: edgeql

    group Movie {
      title,
      num_actors := count(.actors)
    }
    by .release_year

Group by a tuple of properties.

.. tabs::

  .. code-tab:: typescript

    e.group(e.Movie, movie => {
      const release_year = movie.release_year;
      const first_letter = movie.title[0];
      return {
        title: true,
        by: {release_year, first_letter}
      };
    });
    /*
      [
        {
          key: {release_year: 2008, first_letter: "I"},
          grouping: ["release_year", "first_letter"],
          elements: [{title: "Iron Man"}]
        },
        {
          key: {release_year: 2008, first_letter: "T"},
          grouping: ["release_year", "first_letter"],
          elements: [{title: "The Incredible Hulk"}]
        },
        // ...
      ]
    */

  .. code-tab:: edgeql

    group Movie { title }
    using first_letter := .title[0]
    by .release_year, first_letter

Using grouping sets to group by several expressions simultaneously.

.. tabs::

  .. code-tab:: typescript

    e.group(e.Movie, movie => {
      const release_year = movie.release_year;
      const first_letter = movie.title[0];
      return {
        title: true,
        by: e.group.set({release_year, first_letter})
      };
    });
    /* [
      {
        key: {release_year: 2008},
        grouping: ["release_year"],
        elements: [{title: "Iron Man"}, {title: "The Incredible Hulk"}]
      },
      {
        key: {first_letter: "I"},
        grouping: ["first_letter"],
        elements: [{title: "Iron Man"}, {title: "Iron Man 2"}, {title: "Iron Man 3"}],
      },
      // ...
    ] */

  .. code-tab:: edgeql

    group Movie { title }
    using first_letter := .title[0]
    by {.release_year, first_letter}


Using a combination of tuples and grouping sets.

.. tabs::

  .. code-tab:: typescript

    e.group(e.Movie, movie => {
      const release_year = movie.release_year;
      const first_letter = movie.title[0];
      const cast_size = e.count(movie.actors);
      return {
        title: true,
        by: e.group.tuple(release_year, e.group.set({first_letter, cast_size}))
        // by .release_year, { first_letter, cast_size }
        // equivalent to
        // by (.release_year, first_letter), (.release_year, cast_size),
      };
    });
    /* [
      {
        key: {release_year: 2008, first_letter: "I"},
        grouping: ["release_year", "first_letter"],
        elements: [{title: "Iron Man"}]
      },
      {
        key: {release_year: 2008, cast_size: 3},
        grouping: ["release_year", "cast_size"],
        elements: [{title: "The Incredible Hulk"}]
      },
      // ...
    ] */

  .. code-tab:: edgeql

    group Movie { title }
    using
      first_letter := .title[0],
      cast_size := count(.actors)
    by .release_year, {first_letter, cast_size}



The ``group`` statement provides a syntactic sugar for defining certain common
grouping sets: ``cube`` and ``rollup``. Here's a quick primer on how they work:

.. code-block::

  ROLLUP (a, b, c)
  is equivalent to
  {(), (a), (a, b), (a, b, c)}

  CUBE (a, b)
  is equivalent to
  {(), (a), (b), (a, b)}

To use these in the query builder use the ``e.group.cube`` and
``e.group.rollup`` functions.


.. tabs::

  .. code-tab:: typescript

    e.group(e.Movie, movie => {
      const release_year = movie.release_year;
      const first_letter = movie.title[0];
      const cast_size = e.count(movie.actors);
      return {
        title: true,
        by: e.group.rollup({release_year, first_letter, cast_size})
      };
    });

  .. code-tab:: edgeql

    group Movie { title }
    using
      first_letter := .title[0],
      cast_size := count(.actors)
    by rollup(.release_year, first_letter, cast_size)

.. tabs::

  .. code-tab:: typescript

    e.group(e.Movie, movie => {
      const release_year = movie.release_year;
      const first_letter = movie.title[0];
      const cast_size = e.count(movie.actors);
      return {
        title: true,
        by: e.group.cube({release_year, first_letter, cast_size})
      };
    });

  .. code-tab:: edgeql

    group Movie { title }
    using
      first_letter := .title[0],
      cast_size := count(.actors)
    by cube(.release_year, first_letter, cast_size)
