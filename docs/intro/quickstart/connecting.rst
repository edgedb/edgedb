.. _ref_quickstart_connecting:

==========================
Connecting to the database
==========================

.. edb:split-section::

  Before diving into the application, let's take a quick look at how to connect to the database from your code. We will intialize a client and use it to make a simple, static query to the database, and log the result to the console.

  .. note::

    Notice that the ``createClient`` function isn't being passed any connection details. How does Gel know how to connect to the database you set up earlier? When we ran ``npx gel project init`` earlier, the CLI created credentials for the local database and stored them in a well-known location. When you initialize your client with ``createClient()``, Gel will check the places it knows about for connection details.

    With Gel, you do not need to come up with your own scheme for how to build the correct database connection credentials and worry about leaking them into your code. You simply use Gel "projects" for local development, and set the appropriate environment variables when you're ready to deploy, and the client knows what to do!

  .. edb:split-point::

  .. code-block:: typescript
    :caption: ./test.ts

    import { createClient } from "gel";

    const client = createClient();

    async function main() {
      console.log(await client.query("select 'Hello from Gel!';"));
    }

    main().then(
      () => process.exit(0),
      (err) => {
        console.error(err);
        process.exit(1);
      }
    );


  .. code-block:: sh

    $ npx tsx test.ts
    [ 'Hello from Gel!' ]

.. edb:split-section::

  Moving beyond this simple query, use the query builder API to insert a few ``Deck`` objects into the database, and then select them back.

  .. edb:split-point::

  .. code-block:: typescript-diff
    :caption: ./test.ts

      import { createClient } from "gel";
    + import e from "@/dbschema/edgeql-js";

      const client = createClient();

      async function main() {
        console.log(await client.query("select 'Hello from Gel!';"));

    +   await e.insert(e.Deck, { name: "I am one" }).run(client);
    +
    +   await e.insert(e.Deck, { name: "I am two" }).run(client);
    +
    +   const decks = await e
    +     .select(e.Deck, () => ({
    +       id: true,
    +       name: true,
    +     }))
    +     .run(client);
    +
    +   console.table(decks);
    +
    +   await e.delete(e.Deck).run(client);
      }

      main().then(
        () => process.exit(0),
        (err) => {
          console.error(err);
          process.exit(1);
        }
      );

  .. code-block:: sh

    $ npx tsx test.ts
    [ 'Hello from Gel!' ]
    ┌─────────┬────────────────────────────────────────┬────────────┐
    │ (index) │ id                                     │ name       │
    ├─────────┼────────────────────────────────────────┼────────────┤
    │ 0       │ 'f4cd3e6c-ea75-11ef-83ec-037350ea8a6e' │ 'I am one' │
    │ 1       │ 'f4cf27ae-ea75-11ef-83ec-3f7b2fceab24' │ 'I am two' │
    └─────────┴────────────────────────────────────────┴────────────┘