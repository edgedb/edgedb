.. _ref_rust_transactions:

Transactions
------------

The client also has a ``.transaction()`` method that
allows for atomic :ref:`transactions <ref_eql_transactions>`.

Wikipedia has a good example of a scenario requiring a transaction which we
can then implement:

*An example of an atomic transaction is a monetary transfer from bank account A
to account B. It consists of two operations, withdrawing the money from account
A and saving it to account B. Performing these operations in an atomic
transaction ensures that the database remains in a consistent state, that is,
money is neither lost nor created if either of those two operations fails.*

A transaction removing 10 cents from one customer's account and placing it in
another's would look like this:

.. code-block:: rust

  #[derive(Debug, Deserialize, Queryable)]
  pub struct BankCustomer {
      pub name: String,
      pub bank_balance: i32,
  }
  // Customer1 has an account with 110 cents in it.
  // Customer2 has an account with 90 cents in it.
  // Customer1 is going to send 10 cents to Customer 2. This will be a 
  // transaction as we don't want the case to ever occur - even for a 
  // split second - where one account has sent money while the other 
  // has not received it yet.

  // After the transaction is over, each customer should have 100 cents.

  let sender_name = "Customer1";
  let receiver_name = "Customer2";
  let balance_check = "select BankCustomer { name, bank_balance } 
      filter .name = <str>$0";
  let balance_change = "update BankCustomer 
          filter .name = <str>$0
          set { bank_balance := .bank_balance + <int32>$1 }";
  let send_amount = 10;

  client
      .transaction(|mut conn| async move {
          let sender: BankCustomer = conn
              .query_required_single(balance_check, &(sender_name,))
              .await?;
          if sender.bank_balance < send_amount {
              println!("Not enough money, bailing from transaction");
              return Ok(());
          };
          conn.execute(balance_change, &(sender_name, send_amount.neg()))
            .await?;
          conn.execute(balance_change, &(receiver_name, send_amount))
            .await?;
          Ok(())
      })
      .await?;

.. note::

    What often may seem to require an atomic transaction can instead be
    achieved with links and :ref:`backlinks <ref_eql_paths_backlinks>` which
    are both idiomatic and easy to use in EdgeDB.
    For example, if one object holds a ``required link`` to two
    other objects and each of these two objects has a single backlink to the
    first one, simply updating the first object will effectively change the
    state of the other two instantaneously.
