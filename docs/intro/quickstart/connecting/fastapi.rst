.. _ref_quickstart_fastapi_connecting:

==========================
Connecting to the database
==========================

.. edb:split-section::

  Before diving into the application, let's take a quick look at how to connect to the database from your code. We will intialize a client and use it to make a simple, static query to the database, and log the result to the console.

  .. note::

    Notice that the ``create_async_client`` function isn't being passed any connection details. With |Gel|, you do not need to come up with your own scheme for how to build the correct database connection credentials and worry about leaking them into your code. You simply use |Gel| "projects" for local development, and set the appropriate environment variables in your deployment environments, and the ``create_async_client`` function knows what to do!

  .. edb:split-point::

  .. code-block:: python
    :caption: ./test.py

    import gel
    import asyncio

    async def main():
        client = gel.create_async_client()
        result = await client.query_single("select 'Hello from Gel!';")
        print(result)

    asyncio.run(main())

  .. code-block:: sh

    $ python test.py
    Hello from Gel!

.. edb:split-section::

  In Python, we write EdgeQL queries directly as strings. This gives us the full power and expressiveness of EdgeQL while maintaining type safety through Gel's strict schema. Let's try inserting a few ``Deck`` objects into the database and then selecting them back.

  .. edb:split-point::

  .. code-block:: python-diff
    :caption: ./test.py

      import gel
      import asyncio

      async def main():
          client = gel.create_async_client()
    -     result = await client.query_single("select 'Hello from Gel!';")
    -     print(result)
    +     await client.query("""
    +         insert Deck { name := "I am one" }
    +     """)
    +
    +     await client.query("""
    +         insert Deck { name := "I am two" }
    +     """)
    +
    +     decks = await client.query("""
    +         select Deck {
    +             id,
    +             name
    +         }
    +     """)
    +
    +     for deck in decks:
    +         print(f"ID: {deck.id}, Name: {deck.name}")
    +
    +     await client.query("delete Deck")

      asyncio.run(main())

  .. code-block:: sh

    $ python test.py
    Hello from Gel!
    ID: f4cd3e6c-ea75-11ef-83ec-037350ea8a6e, Name: I am one
    ID: f4cf27ae-ea75-11ef-83ec-3f7b2fceab24, Name: I am two
