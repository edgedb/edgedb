====
Rust
====

:edb-alt-title: EdgeDB Rust Client

EdgeDB maintains an client library for Rust. View the `full documentation
<https://docs.rs/edgedb-tokio/latest/edgedb_tokio/>`_.

.. code-block:: rust

  #[tokio::main]
  async fn main() -> anyhow::Result<()> {
      let conn = edgedb_tokio::create_client().await?;
      let val = conn.query_required_single::<i64, _>(
          "SELECT 7*8",
          &(),
      ).await?;
      println!("7*8 is: {}", val);
      Ok(())
  }
