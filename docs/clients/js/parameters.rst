.. _edgedb-js-parameters:

Parameters
----------

You can pass strongly-typed parameters into your query with ``e.params``.

.. code-block:: typescript

  const helloQuery = e.params({name: e.str}, (params) =>
    e.op('Yer a wizard, ', '++', params.name)
  );
  /*  with name := <str>$name
      select name;
  */


The first argument is an object defining the parameter names and their
corresponding types. The second argument is a closure that returns an
expression; use the ``params`` argument to construct the rest of your query.

Passing parameter data
^^^^^^^^^^^^^^^^^^^^^^

To executing a query with parameters, pass the parameter data as the second
argument to ``.run()``; this argument is *fully typed*!

.. code-block:: typescript

  await helloQuery.run(client, { name: "Harry Styles" })
  // => "Yer a wizard, Harry Styles"

  await helloQuery.run(client, { name: 16 })
  // => TypeError: number is not assignable to string

Top-level usage
^^^^^^^^^^^^^^^

Note that you must call ``.run`` on the result of ``e.params``; in other
words, you can only use ``e.params`` at the *top level* of your query, not as
an expression inside a larger query.

.. code-block:: typescript

  // ❌ TypeError
  const wrappedQuery = e.select(helloQuery);
  wrappedQuery.run(client, {name: "Harry Styles"});


.. _edgedb-js-optional-parameters:

Optional parameters
^^^^^^^^^^^^^^^^^^^

A type can be made optional with the ``e.optional`` function.

.. code-block:: typescript

  const query = e.params(
    {
      title: e.str,
      duration: e.optional(e.duration),
    },
    (params) => {
      return e.insert(e.Movie, {
        title: params.title,
        duration: params.duration,
      });
    }
  );

  // works with duration
  const result = await query.run(client, {
    title: 'The Eternals',
    duration: Duration.from({hours: 2, minutes: 3})
  });

  // or without duration
  const result = await query.run(client, {title: 'The Eternals'});

Complex types
^^^^^^^^^^^^^

In EdgeQL, parameters can only be primitives or arrays of primitives. That's
not true with the query builder! Parameter types can be arbitrarily complex.
Under the hood, the query builder serializes the parameters to JSON and
deserializes them on the server.

.. code-block:: typescript

  const insertMovie = e.params(
    {
      title: e.str,
      release_year: e.int64,
      actors: e.array(
        e.tuple({
          name: e.str,
        })
      ),
    },
    (params) =>
      e.insert(e.Movie, {
        title: params.title,
      })
  );

  await insertMovie.run(client, {
    title: 'Dune',
    release_year: 2021,
    actors: [{name: 'Timmy'}, {name: 'JMo'}],
  });

