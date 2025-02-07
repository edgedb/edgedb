.. _edgedb-js-update:

Update
------

Update objects with the ``e.update`` function.

.. code-block:: typescript

  e.update(e.Movie, () => ({
    filter_single: { title: "Avengers 4" },
    set: {
      title: "Avengers: Endgame"
    }
  }))


You can reference the current value of the object's properties.

.. code-block:: typescript

  e.update(e.Movie, (movie) => ({
    filter: e.op(movie.title[0], '=', ' '),
    set: {
      title: e.str_trim(movie.title)
    }
  }))

You can conditionally update a property by using an :ref:`optional parameter
<edgedb-js-optional-parameters>` and the :ref:`coalescing infix operator
<edgedb-js-funcops-infix>`.

.. code-block:: typescript

  e.params({ id: e.uuid, title: e.optional(e.str) }, (params) =>
    e.update(e.Movie, (movie) => ({
      filter_single: { id: params.id },
      set: {
        title: e.op(params.title, "??", movie.title),
      }
    }))
  );

Note that ``e.update`` will return just the ``{ id: true }`` of the updated object. If you want to select further properties, you can wrap the update in a ``e.select`` call. This is still just a single query to the database.

.. code-block:: typescript

  e.params({ id: e.uuid, title: e.optional(e.str) }, (params) => {
    const updated = e.update(e.Movie, (movie) => ({
      filter_single: { id: params.id },
      set: {
        title: e.op(params.title, "??", movie.title),
      },
    }));
    return e.select(updated, (movie) => ({
      title: movie.title,
    }));
  });



Updating links
^^^^^^^^^^^^^^

EdgeQL supports some convenient syntax for appending to, subtracting from, and
overwriting links.

.. code-block:: edgeql

  update Movie set {
    # overwrite
    actors := Person,

    # add to link
    actors += Person,

    # subtract from link
    actors -= Person
  }

In the query builder this is represented with the following syntax.

**Overwrite a link**

.. code-block:: typescript

  const actors = e.select(e.Person, ...);
  e.update(e.Movie, movie => ({
    filter_single: {title: 'The Eternals'},
    set: {
      actors: actors,
    }
  }))

**Add to a link**

.. code-block:: typescript

  const actors = e.select(e.Person, ...);
  e.update(e.Movie, movie => ({
    filter_single: {title: 'The Eternals'},
    set: {
      actors: { "+=": actors },
    }
  }))


**Subtract from a link**

.. code-block:: typescript

  const actors = e.select(e.Person, ...);
  e.update(e.Movie, movie => ({
    filter_single: {title: 'The Eternals'},
    set: {
      actors: { "-=": actors },
    }
  }))

**Updating a single link property**

.. code-block:: typescript

  e.update(e.Movie, (movie) => ({
    filter_single: { title: "The Eternals" },
    set: {
      actors: {
        "+=": e.select(movie.actors, (actor) => ({
          "@character_name": e.str("Sersi"),
          filter: e.op(actor.name, "=", "Gemma Chan")
        }))
      }
    }
  }));

**Updating many link properties**

.. code-block:: typescript

  const q = e.params(
    {
      cast: e.array(e.tuple({ name: e.str, character_name: e.str })),
    },
    (params) =>
      e.update(e.Movie, (movie) => ({
        filter_single: { title: "The Eternals" },
        set: {
          actors: {
            "+=": e.for(e.array_unpack(params.cast), (cast) =>
              e.select(movie.characters, (character) => ({
                "@character_name": cast.character_name,
                filter: e.op(cast.name, "=", character.name),
              })),
            ),
          },
        },
      })),
  ).run(client, {
    cast: [
      { name: "Gemma Chan", character_name: "Sersi" },
      { name: "Richard Madden", character_name: "Ikaris" },
      { name: "Angelina Jolie", character_name: "Thena" },
      { name: "Salma Hayek", character_name: "Ajak" },
    ],
  });

Bulk updates
^^^^^^^^^^^^

You can use a :ref:`for loop <edgedb-js-for>` to perform :ref:`bulk updates
<edgedb-js-for-bulk-inserts>`.
