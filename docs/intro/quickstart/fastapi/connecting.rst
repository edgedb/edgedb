.. _ref_quickstart_connecting:

==========================
Connecting to the database
==========================

.. edb:split-section::

  Before diving into the application, let's take a quick look at how to connect to the database from your code. We will intialize a client and use it to make a simple, static query to the database, and log the result to the console.

  .. note::

    Notice that the ``create_client`` function isn't being passed any connection details. How does Gel know how to connect to the database you set up earlier? When we ran ``gel project init`` earlier, the CLI created credentials for the local database and stored them in a well-known location. When you initialize your client with ``create_client()``, Gel will check the places it knows about for connection details.

    With Gel, you do not need to come up with your own scheme for how to build the correct database connection credentials and worry about leaking them into your code. You simply use Gel "projects" for local development, and set the appropriate environment variables when you're ready to deploy, and the client knows what to do!

  .. edb:split-point::

  .. code-block:: python
    :caption: ./test.py

    import gel

    client = gel.create_client()

    result = client.query_single("select 'Hello from Gel!';")
    print(result)

  .. code-block:: sh

    $ python test.py
    Hello from Gel!

.. edb:split-section::

  In Python, we write EdgeQL queries directly as strings. This gives us the full power and expressiveness of EdgeQL while maintaining type safety through Gel's strict schema. Let's try inserting a few ``Deck`` objects into the database and then selecting them back.

  .. edb:split-point::

  .. code-block:: python-diff
    :caption: ./test.py

      import gel

      client = gel.create_client()

    - result = client.query_single("select 'Hello from Gel!';")
    - print(result)
    + client.query("""
    +     insert Deck { name := "I am one" }
    + """)
    +
    + client.query("""
    +     insert Deck { name := "I am two" }
    + """)
    +
    + decks = client.query("""
    +     select Deck {
    +         id,
    +         name
    +     }
    + """)
    +
    + for deck in decks:
    +     print(f"ID: {deck.id}, Name: {deck.name}")
    +
    + client.query("delete Deck")


  .. code-block:: sh

    $ python test.py
    Hello from Gel!
    ID: f4cd3e6c-ea75-11ef-83ec-037350ea8a6e, Name: I am one
    ID: f4cf27ae-ea75-11ef-83ec-3f7b2fceab24, Name: I am two
