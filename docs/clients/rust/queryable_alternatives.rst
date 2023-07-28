.. _ref_rust_queryable_alternatives:

Alternatives to the Queryable macro
-----------------------------------

The ``Queryable`` macro is the recommended way to make EdgeDB queries in
Rust, but some alternatives exist.

The ``Value`` enum
------------------

The ``Value`` enum can be found in the `edgedb-protocol`_ crate. A ``Value``
represents anything returned from EdgeDB. This means you can always return
a ``Value`` from any of the query methods without needing to deserialize
into a Rust type, and the enum can be instructive in getting to know
the protocol. On the other hand, returning a ``Value`` leads to
pattern matching to get to the inner value and is not the most ergonomic way
to work with results from EdgeDB.

.. code-block:: rust

  pub enum Value {
      Nothing,
      Uuid(Uuid),
      Str(String),
      Bytes(Vec<u8>),
      Int16(i16),
      Int32(i32),
      Int64(i64),
      Float32(f32),
      Float64(f64),
      BigInt(BigInt),
      // ... and so on
  }

Most variants of the ``Value`` enum correspond to a Rust type from the Rust
standard library, while some are from the ``edgedb-protocol`` crate and must
be constructed. For example, this query expecting an EdgeDB ``bigint`` will
return an error as it receives a ``20``, which is an ``i32``:

.. code-block:: rust

  let query = "select <bigint>$0";
  let arg = 20;
  let query_res: Result<Value, _> = 
      client.query_required_single(query, &(arg,)).await;
  assert!(format!("{query_res:?}").contains("expected std::int32"));

Instead, first construct a ``BigInt`` from the ``i32`` and pass that in
as an argument:

.. code-block:: rust

  use edgedb_protocol::model::BigInt;

  let query = "select <bigint>$0";
  let arg = BigInt::from(20);
  let query_res: Result<Value, _> = 
      client.query_required_single(query, &(arg,)).await;
  assert_eq!(
      format!("{query_res:?}"),
      "Ok(BigInt(BigInt { negative: false, weight: 0, digits: [20] }))"
  );

Using JSON
----------

EdgeDB can cast any type to JSON with ``<json>``, but the ``*_json`` methods
don't require this cast in the query. This result can be turned into a
``String`` and used to respond to some JSON API request directly, unpacked 
into a struct using ``serde`` and ``serde_json``, etc.

.. code-block:: rust

  #[derive(Debug, Deserialize)]
  pub struct Account {
      pub username: String,
      pub id: Uuid,
  }

  // No need for <json> cast here
  let query = "select Account { 
      username,
      id
      } filter .username = <str>$0;";

  // Can use query_single_json if we know there will only be one result;
  // otherwise query_json which returns a map of json
  let json_res = client
      .query_single_json(query, &("SomeUserName",))
      .await?
      .unwrap();

  // Format:
  // {"username" : "SomeUser1", 
  // "id" : "7093944a-fd3a-11ed-a013-c7de12ffe7a9"}
  let as_string = json_res.to_string();
  let as_account: Account = serde_json::from_str(&json_res)?;


.. _`edgedb-protocol`: https://docs.rs/edgedb-protocol