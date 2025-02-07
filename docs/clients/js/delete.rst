.. _edgedb-js-delete:

Delete
------

Delete objects with ``e.delete``.

.. code-block:: typescript

  e.delete(e.Movie, movie => ({
    filter: e.op(movie.release_year, ">", 2000),
    filter_single: { id: "abc..." },
    order_by: movie.title,
    offset: 10,
    limit: 10
  }));

The only supported keys are ``filter``, ``filter_single``, ``order_by``,
``offset``, and ``limit``.
