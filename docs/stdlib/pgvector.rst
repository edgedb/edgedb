.. versionadded:: 3.0

.. _ref_ext_pgvector:

=============
ext::pgvector
=============

This can be used to store and efficiently retrieve text embeddings,
such as those produced by OpenAI.

The Postgres that comes packaged with the EdgeDB 3.0+ server includes
``pgvector``, as does EdgeDB Cloud. It you are using a separate
Postgres backend, you will need to arrange for it to be installed.

To activate this new functionality you can use the :ref:`extension
<ref_datamodel_extensions>` mechanism:

.. code-block:: sdl

    using extension pgvector;

That will give you access to the ``ext::pgvector`` module where you may find
the ``ext::pgvector::vector`` type as well as the following functions:

* ``euclidean_distance(a: vector, b: vector) -> std::float64``
* ``neg_inner_product(a: vector, b: vector) -> std::float64``
* ``cosine_distance(a: vector, b: vector) -> std::float64``
* ``euclidean_norm(a: vector, b: vector) -> std::float64``

You also get access to the following three indexes, each corresponding to one
of the vector distance functions:

* ``index ivfflat_euclidean(named only lists: int64)``
* ``index ivfflat_ip(named only lists: int64)``
* ``index ivfflat_cosine(named only lists: int64)``

.. versionadded:: 5.0

    ``ext::pgvector`` now also includes Hierarchical Navigable Small Worlds
    (HNSW) indexes:

    * ``index hnsw_euclidean``
    * ``index hnsw_ip``
    * ``index hnsw_cosine``

When defining a new type, you can now add vector properties. However, in order
to be able to use indexes, the vectors in question need to be of fixed
length. This can be achieved by creating a custom scalar ``extending`` the
vector and specifying the desired length in angle brackets:

.. code-block:: sdl

    scalar type v3 extending ext::pgvector::vector<3>;

    type Item {
        embedding: v3
    }

To populate your data, you can cast an array of any of the numeric types into
``ext::pgvector::vector`` or simply assign that array directly:

.. code-block:: edgeql-repl

    edgedb> insert Item {embedding := <v3>[1.2, 3, 4.5]};
    {default::Item {id: f119d64e-0995-11ee-8804-ff8cd739d8b7}}
    edgedb> insert Item {embedding := [-0.1, 7, 0]};
    {default::Item {id: f410c844-0995-11ee-8804-176f28167dd1}}

You can also cast the vectors into an ``array<float32>>``:

.. code-block:: edgeql-repl

    edgedb> select <array<float32>>Item.embedding;
    {[1.2, 3, 4.5], [-0.1, 7, 0]}

You can query the nearest neighbour by ordering based on
``euclidean_distance``:

.. code-block:: edgeql-repl

  edgedb> select Item {*}
  ....... order by ext::pgvector::euclidean_distance(
  .......   .embedding, <v3>[3, 1, 2])
  ....... empty last
  ....... limit 1;
  {
    default::Item {
      id: f119d64e-0995-11ee-8804-ff8cd739d8b7,
      embedding: [1.2, 3, 4.5],
    },
  }

You can also just retrieve all results within a certain distance:

.. code-block:: edgeql-repl

  edgedb> select Item {*}
  ....... filter ext::pgvector::euclidean_distance(
  .......   .embedding, <v3>[3, 1, 2]) < 5;
  {
    default::Item {
      id: f119d64e-0995-11ee-8804-ff8cd739d8b7,
      embedding: [1.2, 3, 4.5],
    },
  }

The functions mentioned earlier can be used to calculate various useful vector
distances:

.. code-block:: edgeql-repl

  edgedb> select Item {
  .......   id,
  .......   distance := ext::pgvector::euclidean_distance(
  .......     .embedding, <v3>[3, 1, 2]),
  .......   inner_product := -ext::pgvector::neg_inner_product(
  .......     .embedding, <v3>[3, 1, 2]),
  .......   cosine_similarity := 1 - ext::pgvector::cosine_distance(
  .......     .embedding, <v3>[3, 1, 2]),
  ....... };
  {
    default::Item {
      id: f119d64e-0995-11ee-8804-ff8cd739d8b7,
      distance: 3.6728735110725803,
      inner_product: 15.600000143051147,
      cosine_similarity: 0.7525964057358976,
    },
    default::Item {
      id: f410c844-0995-11ee-8804-176f28167dd1,
      distance: 7.043436619202443,
      inner_product: 6.699999988079071,
      cosine_similarity: 0.2557810894509498,
    },
  }

To speed up queries three slightly different IVFFlat indexes can be added to
the type, each of them optimizing one of the distance calculating functions:

.. code-block:: sdl

    type Item {
        embedding: v3;

      index ext::pgvector::ivfflat_euclidean(lists := 10) on (.embedding);
      index ext::pgvector::ivfflat_ip(lists := 10) on (.embedding);
      index ext::pgvector::ivfflat_cosine(lists := 10) on (.embedding);
    }

In order to take advantage of an index, your query must:

1) Use ``order by`` using the function that corresponds to the index
2) Specify ``empty last`` as part of the ``order by`` clause
3) Provide a ``limit`` clause specifying how many results to return

Note that unlike normal indexes, hitting an IVFFlat index changes the
query behavior: it does a (hopefully fast) approximate search instead
of (usually slow) exact one.

As per the `pgvector <pgvector_>`_ recommendations, the keys to achieving good
recall are:

1) Create the index after the table has some data
2) Choose an appropriate number of lists - a good place to start is objects /
   1000 for up to 1M objects and sqrt(objects) for over 1M objects
3) When querying, specify an appropriate number of probes (higher is better
   for recall, lower is better for speed) - a good place to start is sqrt(
   lists). The number of probes can be set by ``ext::pgvector::set_probes()``
   function.

Use our newly introduced ``analyze`` feature to debug query performance and
make sure that the indexes are being used.

The ``ext::pgvector::set_probes()`` function configures the number of
probes to use in approximate index searches. It is scoped to the
current transaction, so if you call it from within a transaction, it
persists until the transaction is finished. The recommended way to use
it, however, is to take advantage of the implicit transactions provided
by multi-statement queries:


.. code-block:: python

  result = client.query("""
      select set_probes(10);
      select Item { id, name }
      order by ext::pgvector::euclidean_distance(
	.embedding, <v3>$vector)
      empty last
      limit 1;
  """, vector=vector)


.. versionadded:: 5.0

    We have updated the mechanism for tuning all of the indexes provided in
    this extension. The ``probes`` (for IVFFlat) and ``ef_search`` (for HNSW)
    parameters can now be accessed via the ``ext::pgvector::Config`` object.

    Examine the ``extensions`` link of the ``cfg::Config`` object to check the
    current config values:

    .. code-block:: edgeql-repl

        db> select cfg::Config.extensions[is ext::pgvector::Config]{*};
        {
          ext::pgvector::Config {
            id: 12b5c70f-0bb8-508a-845f-ca3d41103b6f,
            probes: 1,
            ef_search: 40,
          },
        }

    .. note::

        In order to see the specific extension config properties you need to
        use the type filter :eql:op:`[is ext::pgvector::Config] <isintersect>`

    Update the value using the ``configure session`` or the ``configure current
    branch`` command depending on the scope you prefer:

    .. code-block:: edgeql-repl

        db> configure session
        ... set ext::pgvector::Config::probes := 5;
        OK: CONFIGURE SESSION

    You may also restore the default config value using ``configure session
    reset`` if you set it on the session or ``configure current branch reset``
    if you set it on the branch:

    .. code-block:: edgeql-repl

        db> configure session reset ext::pgvector::Config::probes;
        OK: CONFIGURE SESSION



.. _pgvector:
    https://github.com/pgvector/pgvector
