.. _ref_rust_execute:

Execute
-------

The ``execute`` method doesn't return anything — a successful execute returns
an ``Ok(())`` — which is convenient for things like updates or commands where
we don't care about getting output if it works:

.. code-block:: rust

  client.execute("update Account set {username := .username ++ '!'};", &())
    .await?;
  client.execute("create superuser role project;", &())
    .await?;
  client.execute("alter role project set password := 'GOODpassword';", &())
    .await?;

  // Returns Ok(()) upon success but error info will be returned
  let command = client.execute("create type MyType {};", &()).await;
  let command_string = command.unwrap_err().to_string();
  assert!(command_string.contains("bare DDL statements are not allowed"));