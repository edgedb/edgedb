.. _edgedb-js-with:

With Blocks
-----------

During the query rendering step, the number of occurrences of each expression
are tracked. If an expression occurs more than once it is automatically
extracted into a ``with`` block.

.. code-block:: typescript

  const x = e.int64(3);
  const y = e.select(e.op(x, '^', x));

  y.toEdgeQL();
  // with x := 3
  // select x ^ x

  const result = await y.run(client);
  // => 27

This hold for expressions of arbitrary complexity.

.. code-block:: typescript

  const robert = e.insert(e.Person, {
    name: "Robert Pattinson"
  });
  const colin = e.insert(e.Person, {
    name: "Colin Farrell"
  });
  const newMovie = e.insert(e.Movie, {
    title: "The Batman",
    actors: e.set(colin, robert)
  });

  /*
  with
    robert := (insert Person {  name := "Robert Pattinson"}),
    colin := (insert Person {  name := "Colin Farrell"}),
  insert Movie {
    title := "The Batman",
    actors := {robert, colin}
  }
  */

Note that ``robert`` and ``colin`` were pulled out into a top-level with
block. To force these variables to occur in an internal ``with`` block, you
can short-circuit this logic with ``e.with``.


.. code-block:: typescript

  const robert = e.insert(e.Person, {
    name: "Robert Pattinson"
  });
  const colin = e.insert(e.Person, {
    name: "Colin Farrell"
  });
  const newMovie = e.insert(e.Movie, {
    actors: e.with([robert, colin], // list "dependencies"
      e.select(e.set(robert, colin))
    )
  })

  /*
  insert Movie {
    title := "The Batman",
    actors := (
      with
        robert := (insert Person {  name := "Robert Pattinson"}),
        colin := (insert Person {  name := "Colin Farrell"})
      select {robert, colin}
    )
  }
  */


.. note::

  It's an error to pass an expression into multiple
  ``e.with``\ s, or use an expression passed to ``e.with`` outside of that
  block.

To explicitly create a detached "alias" of another expression, use ``e.alias``.

.. code-block:: typescript

  const a = e.set(1, 2, 3);
  const b = e.alias(a);

  const query = e.select(e.op(a, '*', b))
  // WITH
  //   a := {1, 2, 3},
  //   b := a
  // SELECT a + b

  const result = await query.run(client);
  // => [1, 2, 3, 2, 4, 6, 3, 6, 9]

