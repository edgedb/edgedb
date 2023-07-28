.. _ref_rust_client_config:

Client configuration
--------------------

The client can be configured after initialization via the ``with_*`` methods
(``with_retry_options``, ``with_transaction_options``, etc.) that create a
shallow copy of the client with adjusted options.

.. code-block:: rust

  // Take a schema with matching Rust structs:
  //
  // module default {
  //   type User {
  //     required name: str;
  //   }
  // }

  // module test {
  //   type User {
  //     required name: str;
  //   }
  // };

  // The regular client will query from module 'default' by default
  let client = edgedb_tokio::create_client().await?;

  // This client will query from module 'test' by default
  // The original client is unaffected
  let test_client = client.with_default_module(Some("test"));
        
  // Each client queries separately with different behavior
  let query = "select User {name};";
  let users: Vec<User> = client.query(query, &()).await?;
  let test_users: Vec<TestUser> = test_client.query(query, &()).await?;

  // Many other clients can be created with different options,
  // all independent of the main client:
  let transaction_opts = TransactionOptions::default().read_only(true);
  let _read_only_client = client
      .with_transaction_options(transaction_opts);

  let retry_opts = RetryOptions::default().with_rule(
      RetryCondition::TransactionConflict,
      // No. of retries
      1,
      // Retry immediately, instead of default with increasing backoff
      |_| std::time::Duration::from_millis(0),
  );
  let _one_immediate_retry_client = client.with_retry_options(retry_opts);
