.. _edgedb-python-examples:

Basic Usage
===========

To start using EdgeDB in Python, create an :py:class:`edgedb.Client` instance
using :py:func:`edgedb.create_client`:

.. code-block:: python

    import datetime
    import edgedb

    client = edgedb.create_client()

    client.query("""
        INSERT User {
            name := <str>$name,
            dob := <cal::local_date>$dob
        }
    """, name="Bob", dob=datetime.date(1984, 3, 1))

    user_set = client.query(
        "SELECT User {name, dob} FILTER .name = <str>$name", name="Bob")
    # *user_set* now contains
    # Set{Object{name := 'Bob', dob := datetime.date(1984, 3, 1)}}

    client.close()

When used with asyncio, this should be replaced with
:py:func:`edgedb.create_async_client` which creates an instance of the
:py:class:`~edgedb.AsyncIOClient`:

.. code-block:: python

    import asyncio
    import datetime
    import edgedb

    client = edgedb.create_async_client()

    async def main():
        await client.query("""
            INSERT User {
                name := <str>$name,
                dob := <cal::local_date>$dob
            }
        """, name="Bob", dob=datetime.date(1984, 3, 1))

        user_set = await client.query(
            "SELECT User {name, dob} FILTER .name = <str>$name", name="Bob")
        # *user_set* now contains
        # Set{Object{name := 'Bob', dob := datetime.date(1984, 3, 1)}}

        await client.aclose()

    asyncio.run(main())


Connect to EdgeDB
-----------------

The examples above only work under an :ref:`EdgeDB project
<ref_guide_using_projects>`. You could also provide your own connection
parameters, refer to the :ref:`Client Library Connection
<edgedb_client_connection>` docs for details.


Type conversion
---------------

edgedb-python automatically converts EdgeDB types to the corresponding Python
types and vice versa.  See :ref:`edgedb-python-datatypes` for details.


.. _edgedb-python-connection-pool:

Client connection pools
-----------------------

For server-type applications that handle frequent requests and need
the database connection for a short period of time while handling a request,
the use of a connection pool is recommended. Both :py:class:`edgedb.Client`
and :py:class:`edgedb.AsyncIOClient` come with such a pool.

For :py:class:`edgedb.Client`, all methods are thread-safe. You can share the
same client instance safely across multiple threads, and run queries
concurrently. Likewise, :py:class:`~edgedb.AsyncIOClient` is designed to be
shared among different :py:class:`asyncio.Task`/coroutines for concurrency.

Below is an example of a web API server running `aiohttp
<https://docs.aiohttp.org/>`_:

.. code-block:: python

    import asyncio
    import edgedb
    from aiohttp import web


    async def handle(request):
        """Handle incoming requests."""
        client = request.app['client']
        username = request.match_info.get('name')

        # Execute the query on any pool connection
        result = await client.query_single_json(
            '''
                SELECT User {first_name, email, bio}
                FILTER .name = <str>$username
            ''', username=username)
        return web.Response(
            text=result,
            content_type='application/json')


    def init_app():
        """Initialize the application server."""
        app = web.Application()
        # Create a database client
        app['client'] = edgedb.create_async_client(
            database='my_service',
            user='my_service')
        # Configure service routes
        app.router.add_route('GET', '/user/{name}', handle)
        return app


    loop = asyncio.get_event_loop()
    app = init_app()
    web.run_app(app)

Note that the client is created synchronously. Pool connections are created
lazily as they are needed. If you want to explicitly connect to the
database in ``init_app()``, use the ``ensure_connected()`` method on the client.

For more information, see API documentation of :ref:`the blocking client
<edgedb-python-blocking-api-client>` and :ref:`the asynchronous client
<edgedb-python-async-api-client>`.


Transactions
------------

The most robust way to create a
:ref:`transaction <edgedb-python-asyncio-api-transaction>` is the
``transaction()`` method:

* :py:meth:`AsyncIOClient.transaction() <edgedb.AsyncIOClient.transaction>`
* :py:meth:`Client.transaction() <edgedb.Client.transaction>`


Example:

.. code-block:: python

    for tx in client.transaction():
        with tx:
            tx.execute("INSERT User {name := 'Don'}")

or, if using the async API:

.. code-block:: python

    async for tx in client.transaction():
        async with tx:
            await tx.execute("INSERT User {name := 'Don'}")

.. note::

   When not in an explicit transaction block, any changes to the database
   will be applied immediately.

For more information, see API documentation of transactions for :ref:`the
blocking client <edgedb-python-blocking-api-transaction>` and :ref:`the
asynchronous client <edgedb-python-asyncio-api-transaction>`.
