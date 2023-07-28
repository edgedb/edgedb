.. _ref_rust_arguments:

Passing in arguments
--------------------

A regular EdgeQL query without arguments looks like this:

.. code-block:: edgeql

  with 
      message1 := 'Hello there', 
      message2 := 'General Kenobi', 
  select message1 ++ ' ' ++ message2;

And the same query with arguments:

.. code-block:: edgeql

  with 
      message1 := <str>$0, 
      message2 := <str>$1, 
  select message1 ++ ' ' ++ message2;

In the EdgeQL REPL you are prompted to enter arguments:

.. code-block:: edgeql-repl

  db> with
  ... message1 := <str>$0,
  ... message2 := <str>$1,
  ... select message1 ++ ' ' ++ message2;
  Parameter <str>$0: Hello there
  Parameter <str>$1: General Kenobi
  {'Hello there General Kenobi'}

But when using the Rust client, there is no prompt to do so. At present,
arguments also have to be in the order ``$0``, ``$1``, and so on, while in
the REPL they can be named (e.g. ``$message`` and ``$person`` instead of
``$0`` and ``$1``). The arguments in the client are then passed to the 
appropriate query method as a tuple:

.. code-block:: rust

  let args = ("Nice movie", 2023);
  let query = "with
  movie := (insert Movie {
  title := <str>$0,
  release_year := <int32>$1
  })
  select  {
      title,
      release_year,
      id
  }";
  let query_res: Value = client.query_required_single(query, &(args)).await?;

A note on the casting syntax: EdgeDB requires arguments to have a cast in the
same way that Rust requires a type declaration in function signatures.
As such, arguments in queries are used as type specification for the EdgeDB
compiler, not to cast from queries from the Rust side. Take this query
as an example:

.. code-block:: rust

  let query = "select <int32>$0";

This simply means "select an argument that must be an ``int32``", not 
"take the received argument and cast it into an ``int32``".

As such, this will return an error:

.. code-block:: rust

  let query = "select <int32>$0";
  let arg = 9i16; // Rust client will expect an int16
  let query_res: Result<Value, _> = 
    client.query_required_single(query, &(arg,)).await;
  assert!(query_res
      .unwrap_err()
      .to_string()
      .contains("expected std::int16"));