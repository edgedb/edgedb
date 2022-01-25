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

.. code-block:: typescript

  client.transaction(async tx => {
    await tx.execute(`insert Fish { name := 'Wanda' };`);
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
