.. _ref_rust_client:

Using the client
----------------

Creating a new EdgeDB client can be done in a single line:

.. code-block:: rust

  let client = edgedb_tokio::create_client().await?;

Under the hood, this will create a ``Builder``, look for environment variables
and/or an ``edgedb.toml`` file, and return an ``Ok(Self)`` if successful.
This ``Builder`` can be used on its own instead of ``create_client()``
if you need a more customized setup.

Queries with the client
-----------------------

Here are the simplified signatures of the client methods used for querying:

.. note::
    ``R`` here means a type that implements ``QueryResult``.
    (See more on ``QueryResult`` and ``QueryArgs`` on the 
    `edgedb-protocol documentation`_.)

.. code-block:: rust

  fn query -> Result<Vec<R>, Error>
  fn query_json -> Result<Json, Error>

  fn query_single -> Result<Option<R>, Error>
  fn query_single_json -> Result<Option<Json>>

  fn query_required_single -> Result<R, Error>
  fn query_required_single_json -> Result<Json, Error>

  fn execute -> Result<(), Error>

Note the difference between the ``_single`` and the
``_required_single`` methods:

- The ``_required_single`` methods return empty results as a ``NoDataError``
  which allows propagating errors normally through an application.
- The ``_single`` methods will simply give you an ``Ok(None)`` in this case.

These methods all take a *query* (a ``&str``) and *arguments* (something that
implements the ``QueryArgs`` trait).

The ``()`` unit type implements ``QueryArgs`` and is used when no arguments
are present so ``&()`` is a pretty common sight when using the Rust client.

.. code-block:: rust

  // Without arguments: just add &() after the query
  let query_res: String = 
      client.query_required_single("select 'Just a string'", &()).await?;

  // With arguments, same output as the previous example
  let a = " a ";
  let b = "string";
  let query_res: String = client
      .query_required_single("select 'Just' ++ <str>$0 ++ <str>$1", &(a, b))
      .await?;

For more, see the section on :ref:`passing in arguments <ref_rust_arguments>`.

These methods take two generic parameters which can be specified with the
turbofish syntax:

.. code-block:: rust

  let query_res = client
       .query_required_single::<String, ()>("select 'Just a string'", &())
       .await?;
  // or
  let query_res = client
       .query_required_single::<String, _>("select 'Just a string'", &())
       .await?;
    
But declaring the final expected type upfront tends to look neater.

.. code-block:: rust

  let query_res: String = client
      .query_required_single("select 'Just a string'", &())
      .await?;

When cardinality is guaranteed to be 1
--------------------------------------

Using the ``.query()`` method works fine for any cardinality, but returns a
``Vec`` of results. This query with a cardinality of 1 returns a
``Result<Vec<String>>`` which becomes a ``Vec<String>`` after the error
is handled:

.. code-block:: rust

  let query = "select 'Just a string'";
  let query_res: Vec<String> = client.query(query, &()).await?;

But if you know that only a single result will be returned, using 
``.query_required_single()`` or ``.query_single()`` will be more ergonomic:

.. code-block:: rust

  let query = "select 'Just a string'";
  let query_res: String = client
      .query_required_single(query, &()).await?;
  let query_res_opt: Option<String> = client
      .query_single(query, &()).await?;

.. _`edgedb-protocol documentation`: https://docs.rs/edgedb-protocol/