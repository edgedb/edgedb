.. _ref_rust_index:

====
Rust
====

:edb-alt-title: EdgeDB Rust Client

EdgeDB maintains a client library for Rust. View the `full documentation`_.

The "hello world" of the Rust EdgeDB client is as follows:

.. code-block:: rust

  #[tokio::main]
  async fn main() {
      let conn = edgedb_tokio::create_client()
          .await
          .expect("Client should have initiated");
      let val: i64 = conn
          .query_required_single("select 7*8", &())
          .await
          .expect("Query should have worked");
      println!("7*8 is: {val}");
  }

.. _`full documentation`: https://docs.rs/edgedb-tokio/latest/edgedb_tokio/

.. toctree::
    :maxdepth: 2
    :hidden:

    getting_started
    client
    queryable
    arguments
    queryable_alternatives
    execute
    transactions
    client_config

