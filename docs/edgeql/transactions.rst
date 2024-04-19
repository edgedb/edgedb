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
  Commit the transaction. All changes made by the transaction become visible
  to others and will persist if a crash occurs.


Client libraries
----------------

There is rarely a reason to use these commands directly. All EdgeDB client
libraries provide dedicated transaction APIs that handle transaction creation
under the hood.

Examples below show a transaction that sends 10 cents from the account
of a ``BankCustomer`` called ``'Customer1'`` to ``BankCustomer`` called
``'Customer2'``. The equivalent EdgeDB schema and queries are:

.. code-block::

  module default {
    type BankCustomer {
      required name: str;
      required balance: int64;
    }
  }
  update BankCustomer
      filter .name = 'Customer1'
      set { bank_balance := .bank_balance -10 };
  update BankCustomer
      filter .name = 'Customer2'
      set { bank_balance := .bank_balance +10 }

TypeScript/JS
^^^^^^^^^^^^^

Using an EdgeQL query string:

.. code-block:: typescript

  client.transaction(async tx => {
    await tx.execute(`update BankCustomer
      filter .name = 'Customer1'
      set { bank_balance := .bank_balance -10 }`);
    await tx.execute(`update BankCustomer
      filter .name = 'Customer2'
      set { bank_balance := .bank_balance +10 }`);
  });

Using the querybuilder:

.. code-block:: typescript

  const query1 = e.update(e.BankCustomer, () => ({
    filter_single: { name: "Customer1" },
    set: {
      bank_balance: { "-=":  10 }
    },
  }));
  const query2 = e.update(e.BankCustomer, () => ({
    filter_single: { name: "Customer2" },
    set: {
      bank_balance: { "+=":  10 }
    },
  }));

  client.transaction(async (tx) => {
    await query1.run(tx);
    await query2.run(tx);
  });

Full documentation at :ref:`Client Libraries > TypeScript/JS <edgedb-js-intro>`;

Python
^^^^^^

.. code-block:: python

  async for tx in client.transaction():
      async with tx:
          await tx.execute("""update BankCustomer
              filter .name = 'Customer1'
              set { bank_balance := .bank_balance -10 };""")
          await tx.execute("""update BankCustomer
              filter .name = 'Customer2'
              set { bank_balance := .bank_balance +10 };""")

Full documentation at :ref:`Client Libraries > Python <edgedb-python-intro>`;

Golang
^^^^^^

.. code-block:: go

	err = client.Tx(ctx, func(ctx context.Context, tx *edgedb.Tx) error {
		query1 := `update BankCustomer
			filter .name = 'Customer1'
			set { bank_balance := .bank_balance -10 };`
		if e := tx.Execute(ctx, query1); e != nil {
			return e
		}
		query2 := `update BankCustomer
			filter .name = 'Customer2'
			set { bank_balance := .bank_balance +10 };`
		if e := tx.Execute(ctx, query2); e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

Full documentation at :ref:`Client Libraries > Go <edgedb-go-intro>`.

Rust
^^^^

.. code-block:: rust

  let balance_change_query = "update BankCustomer
    filter .name = <str>$0
    set { bank_balance := .bank_balance + <int32>$1 }";

  client
      .transaction(|mut conn| async move {
          conn.execute(balance_change_query, &("Customer1", -10))
              .await
              .expect("Execute should have worked");
          conn.execute(balance_change_query, &("Customer2", 10))
              .await
              .expect("Execute should have worked");
          Ok(())
      })
      .await
      .expect("Transaction should have worked");

Full documentation at :ref:`Client Libraries > Rust <ref_rust_index>`.
