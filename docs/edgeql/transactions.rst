.. _ref_eql_transactions:

Transactions
============

EdgeQL supports atomic transactions. The transaction API consists
of several commands:

:eql:stmt:`start transaction`
  Start a transaction, specifying the isolation level, access mode (``read
  only`` vs ``read write``), and deferrability.

:eql:stmt:`declare savepoint`
  Establish a new savepoint within the current transaction. A savepoint is a
  intermediate point in a transaction flow that provides the ability to
  partially rollback a transaction.

:eql:stmt:`release savepoint`
  Destroys a savepoint previously defined in the current transaction.

:eql:stmt:`rollback to savepoint`
  Rollback to the named savepoint. All changes made after the savepoint
  are discarded. The savepoint remains valid and can be rolled back
  to again later, if needed.

:eql:stmt:`rollback`
  Rollback the entire transaction. All updates made within the transaction are
  discarded.

:eql:stmt:`commit`
  Commit the transaction. All changes made by the transaction become visible to
  others and will persist if a crash occurs.


Client libraries
----------------

There is rarely a reason to use these commands directly. All EdgeDB client
libraries provide dedicated transaction APIs that handle transaction creation
under the hood.

TypeScript/JS
^^^^^^^^^^^^^

Using an EdgeQL query string:

.. code-block:: typescript

  client.transaction(async tx => {
    await tx.execute(`insert Fish { name := 'Wanda' };`);
  });

Using the querybuilder:

.. code-block:: typescript

  const query = e.insert(e.Fish, {
    name: 'Wanda'
  });
  client.transaction(async tx => {
    await query.run(tx);
  });

Full documentation at `Client Libraries > TypeScript/JS
</docs/clients/01_js/index>`_;

Python
^^^^^^

.. code-block:: python

  async for tx in client.transaction():
      async with tx:
          await tx.execute("insert Fish { name := 'Wanda' };")

Full documentation at `Client Libraries > Python
</docs/clients/00_python/index>`_;

Golang
^^^^^^

.. code-block:: go

	err := client.Tx(ctx, func(ctx context.Context, tx *Tx) error {
		query := "insert Fish { name := 'Wanda' };"
		if e := tx.Execute(ctx, query); e != nil {
			return e
		}
	})

Full documentation at `Client Libraries > Go </docs/clients/02_go/index>`_.

Rust
^^^^

.. code-block:: rust

  #[derive(Debug, Deserialize, Queryable)]
  pub struct BankCustomer {
      pub name: String,
      pub bank_balance: i32,
  }
  // Customer1 has an account with 110 cents in it.
  // Customer2 has an account with 90 cents in it.
  // Customer1 is going to send 10 cents to Customer 2. This will be a transaction
  // because we don't want the case to ever occur - even for a split second -
  // where one account has sent money while the other has not received it yet.

  // After the transaction is over, each customer should have 100 cents.

  fn main() {
      let client = edgedb_tokio::create_client().await.expect("Client should initialize");

      let sender_name = "Customer1";
      let receiver_name = "Customer2";
      let balance_check_query = "select BankCustomer { name, bank_balance } 
          filter .name = <str>$0";
      let balance_change_query = "update BankCustomer 
              filter .name = <str>$0
              set { bank_balance := .bank_balance + <int32>$1 }";
      let send_amount = 10;

      client
          .transaction(|mut conn| async move {
              let sender: BankCustomer = conn
                  .query_required_single(balance_check_query, &(sender_name,))
                  .await?;
              if sender.bank_balance < send_amount {
                  println!("Not enough money to send, bailing from transaction");
                  return Ok(());
              };
              conn.execute(balance_change_query, &(sender_name, send_amount.neg()))
                  .await
                  .expect("Execute should work");
              conn.execute(balance_change_query, &(receiver_name, send_amount))
                  .await
                  .expect("Execute should work");
              Ok(())
          })
          .await
          .expect("Transaction should succeed");
  }

Full documentation at `Client Libraries > Rust </docs/clients/03_rust/index>`_.