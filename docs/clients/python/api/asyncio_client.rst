.. _edgedb-python-asyncio-api-reference:

===========
AsyncIO API
===========

.. py:currentmodule:: edgedb


.. _edgedb-python-async-api-client:

Client
======

.. py:function:: create_async_client(dsn=None, *, \
            host=None, port=None, \
            user=None, password=None, \
            secret_key=None, \
            database=None, \
            timeout=60, \
            concurrency=None)

    Create an asynchronous client with a lazy connection pool.

    The connection parameters may be specified either as a connection
    URI in *dsn*, or as specific keyword arguments, or both.
    If both *dsn* and keyword arguments are specified, the latter
    override the corresponding values parsed from the connection URI.

    If no connection parameter is specified, the client will try to search in
    environment variables and then the current project, see :ref:`Client
    Library Connection <edgedb_client_connection>` docs for more information.

    Returns a new :py:class:`AsyncIOClient` object.

    :param str dsn:
        If this parameter does not start with ``edgedb://`` then this is
        interpreted as the :ref:`name of a local instance
        <ref_reference_connection_instance_name>`.

        Otherwise it specifies a single string in the following format:
        ``edgedb://user:password@host:port/database?option=value``.
        The following options are recognized: host, port,
        user, database, password. For a complete reference on DSN, see
        the :ref:`DSN Specification <ref_dsn>`.

    :param host:
        Database host address as an IP address or a domain name;

        If not specified, the following will be tried, in order:

        - host address(es) parsed from the *dsn* argument,
        - the value of the ``EDGEDB_HOST`` environment variable,
        - ``"localhost"``.

    :param port:
        Port number to connect to at the server host. If multiple host
        addresses were specified, this parameter may specify a
        sequence of port numbers of the same length as the host sequence,
        or it may specify a single port number to be used for all host
        addresses.

        If not specified, the value parsed from the *dsn* argument is used,
        or the value of the ``EDGEDB_PORT`` environment variable, or ``5656``
        if neither is specified.

    :param user:
        The name of the database role used for authentication.

        If not specified, the value parsed from the *dsn* argument is used,
        or the value of the ``EDGEDB_USER`` environment variable, or the
        operating system name of the user running the application.

    :param database:
        The name of the database to connect to.

        If not specified, the value parsed from the *dsn* argument is used,
        or the value of the ``EDGEDB_DATABASE`` environment variable, or the
        operating system name of the user running the application.

    :param password:
        Password to be used for authentication, if the server requires
        one.  If not specified, the value parsed from the *dsn* argument
        is used, or the value of the ``EDGEDB_PASSWORD`` environment variable.
        Note that the use of the environment variable is discouraged as
        other users and applications may be able to read it without needing
        specific privileges.

    :param secret_key:
        Secret key to be used for authentication, if the server requires one.
        If not specified, the value parsed from the *dsn* argument is used,
        or the value of the ``EDGEDB_SECRET_KEY`` environment variable.
        Note that the use of the environment variable is discouraged as
        other users and applications may be able to read it without needing
        specific privileges.

    :param float timeout:
        Connection timeout in seconds.

    :param int concurrency:
        Max number of connections in the pool. If not set, the suggested
        concurrency value provided by the server is used.

    :return: An instance of :py:class:`AsyncIOClient`.

    The APIs on the returned client instance can be safely used by different
    :py:class:`asyncio.Task`/coroutines, because under the hood they are
    checking out different connections from the pool to run the queries:

    * :py:meth:`AsyncIOClient.query()`
    * :py:meth:`AsyncIOClient.query_single()`
    * :py:meth:`AsyncIOClient.query_required_single()`
    * :py:meth:`AsyncIOClient.query_json()`
    * :py:meth:`AsyncIOClient.query_single_json()`
    * :py:meth:`AsyncIOClient.query_required_single_json()`
    * :py:meth:`AsyncIOClient.execute()`
    * :py:meth:`AsyncIOClient.transaction()`

    .. code-block:: python

        client = edgedb.create_async_client()
        await client.query('SELECT {1, 2, 3}')

    The same for transactions:

    .. code-block:: python

        client = edgedb.create_async_client()
        async for tx in client.transaction():
            async with tx:
                await tx.query('SELECT {1, 2, 3}')



.. py:class:: AsyncIOClient()

    An asynchronous client with a connection pool, safe for concurrent use.

    Async clients are created by calling
    :py:func:`~edgedb.create_async_client`.

    .. py:coroutinemethod:: query(query, *args, **kwargs)

        Acquire a connection and use it to run a query and return the results
        as an :py:class:`edgedb.Set` instance. The temporary
        connection is automatically returned back to the pool.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            An instance of :py:class:`edgedb.Set` containing
            the query result.

        Note that positional and named query arguments cannot be mixed.


    .. py:coroutinemethod:: query_single(query, *args, **kwargs)

        Acquire a connection and use it to run an optional singleton-returning
        query and return its element. The temporary connection is automatically
        returned back to the pool.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            Query result.

        The *query* must return no more than one element.  If the query returns
        more than one element, an ``edgedb.ResultCardinalityMismatchError``
        is raised, if it returns an empty set, ``None`` is returned.

        Note, that positional and named query arguments cannot be mixed.


    .. py:coroutinemethod:: query_required_single(query, *args, **kwargs)

        Acquire a connection and use it to run a singleton-returning query
        and return its element. The temporary connection is automatically
        returned back to the pool.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            Query result.

        The *query* must return exactly one element.  If the query returns
        more than one element, an ``edgedb.ResultCardinalityMismatchError``
        is raised, if it returns an empty set, an ``edgedb.NoDataError``
        is raised.

        Note, that positional and named query arguments cannot be mixed.


    .. py:coroutinemethod:: query_json(query, *args, **kwargs)

        Acquire a connection and use it to run a query and
        return the results as JSON. The temporary connection is automatically
        returned back to the pool.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            A JSON string containing an array of query results.

        Note, that positional and named query arguments cannot be mixed.

        .. note::

            Caution is advised when reading ``decimal`` values using
            this method. The JSON specification does not have a limit
            on significant digits, so a ``decimal`` number can be
            losslessly represented in JSON. However, the default JSON
            decoder in Python will read all such numbers as ``float``
            values, which may result in errors or precision loss. If
            such loss is unacceptable, then consider casting the value
            into ``str`` and decoding it on the client side into a
            more appropriate type, such as ``Decimal``.


    .. py:coroutinemethod:: query_single_json(query, *args, **kwargs)

        Acquire a connection and use it to run an optional singleton-returning
        query and return its element in JSON. The temporary connection is
        automatically returned back to the pool.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            Query result encoded in JSON.

        The *query* must return no more than one element.  If the query returns
        more than one element, an ``edgedb.ResultCardinalityMismatchError``
        is raised, if it returns an empty set, ``"null"`` is returned.

        Note, that positional and named query arguments cannot be mixed.

        .. note::

            Caution is advised when reading ``decimal`` values using
            this method. The JSON specification does not have a limit
            on significant digits, so a ``decimal`` number can be
            losslessly represented in JSON. However, the default JSON
            decoder in Python will read all such numbers as ``float``
            values, which may result in errors or precision loss. If
            such loss is unacceptable, then consider casting the value
            into ``str`` and decoding it on the client side into a
            more appropriate type, such as ``Decimal``.


    .. py:coroutinemethod:: query_required_single_json(query, *args, **kwargs)

        Acquire a connection and use it to run a singleton-returning
        query and return its element in JSON. The temporary connection is
        automatically returned back to the pool.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            Query result encoded in JSON.

        The *query* must return exactly one element.  If the query returns
        more than one element, an ``edgedb.ResultCardinalityMismatchError``
        is raised, if it returns an empty set, an ``edgedb.NoDataError``
        is raised.

        Note, that positional and named query arguments cannot be mixed.

        .. note::

            Caution is advised when reading ``decimal`` values using
            this method. The JSON specification does not have a limit
            on significant digits, so a ``decimal`` number can be
            losslessly represented in JSON. However, the default JSON
            decoder in Python will read all such numbers as ``float``
            values, which may result in errors or precision loss. If
            such loss is unacceptable, then consider casting the value
            into ``str`` and decoding it on the client side into a
            more appropriate type, such as ``Decimal``.


    .. py:coroutinemethod:: execute(query)

        Acquire a connection and use it to execute an EdgeQL command
        (or commands).  The temporary connection is automatically
        returned back to the pool.

        :param str query: Query text.

        The commands must take no arguments.

        Example:

        .. code-block:: pycon

            >>> await con.execute('''
            ...     CREATE TYPE MyType {
            ...         CREATE PROPERTY a -> int64
            ...     };
            ...     FOR x IN {100, 200, 300}
            ...     UNION INSERT MyType { a := x };
            ... ''')

        .. note::
            If the results of *query* are desired, :py:meth:`query`,
            :py:meth:`query_single` or :py:meth:`query_required_single`
            should be used instead.

    .. py:method:: transaction()

        Open a retryable transaction loop.

        This is the preferred method of initiating and running a database
        transaction in a robust fashion.  The ``transaction()``
        transaction loop will attempt to re-execute the transaction loop body
        if a transient error occurs, such as a network error or a transaction
        serialization error.

        Returns an instance of :py:class:`AsyncIORetry`.

        See :ref:`edgedb-python-asyncio-api-transaction` for more details.

        Example:

        .. code-block:: python

            async for tx in con.transaction():
                async with tx:
                    value = await tx.query_single("SELECT Counter.value")
                    await tx.execute(
                        "UPDATE Counter SET { value := <int64>$value }",
                        value=value + 1,
                    )

        Note that we are executing queries on the ``tx`` object rather
        than on the original connection.

        .. note::
            The transaction starts lazily. A connection is only acquired from
            the pool when the first query is issued on the transaction instance.


    .. py:coroutinemethod:: aclose()

        Attempt to gracefully close all connections in the pool.

        Wait until all pool connections are released, close them and
        shut down the pool.  If any error (including cancellation) occurs
        in ``aclose()`` the pool will terminate by calling
        :py:meth:`~edgedb.AsyncIOClient.terminate`.

        It is advisable to use :py:func:`python:asyncio.wait_for` to set
        a timeout.

    .. py:method:: terminate()

        Terminate all connections in the pool.


    .. py:coroutinemethod:: ensure_connected()

        If the client does not yet have any open connections in its pool,
        attempts to open a connection, else returns immediately.

        Since the client lazily creates new connections as needed (up to the
        configured ``concurrency`` limit), the first connection attempt will
        only occur when the first query is run on a client. ``ensureConnected``
        can be useful to catch any errors resulting from connection
        mis-configuration by triggering the first connection attempt
        explicitly.

    .. py:method:: with_transaction_options(options=None)

        Returns a shallow copy of the client with adjusted transaction options.

        :param TransactionOptions options:
            Object that encapsulates transaction options.

        See :ref:`edgedb-python-transaction-options` for details.

    .. py:method:: with_retry_options(options=None)

        Returns a shallow copy of the client with adjusted retry options.

        :param RetryOptions options: Object that encapsulates retry options.

        See :ref:`edgedb-python-retry-options` for details.

    .. py:method:: with_state(state)

        Returns a shallow copy of the client with adjusted state.

        :param State state: Object that encapsulates state.

        See :ref:`edgedb-python-state` for details.

    .. py:method:: with_default_module(module=None)

        Returns a shallow copy of the client with adjusted default module.

        This is equivalent to using the ``set module`` command, or using the
        ``reset module`` command when giving ``None``.

        :type module: str or None
        :param module: Adjust the *default module*.

        See :py:meth:`State.with_default_module` for details.

    .. py:method:: with_module_aliases(aliases_dict=None, /, **aliases)

        Returns a shallow copy of the client with adjusted module aliases.

        This is equivalent to using the ``set alias`` command.

        :type aliases_dict: dict[str, str] or None
        :param aliases_dict: This is an optional positional-only argument.

        :param dict[str, str] aliases:
            Adjust the module aliases after applying ``aliases_dict`` if set.

        See :py:meth:`State.with_module_aliases` for details.

    .. py:method:: without_module_aliases(*aliases)

        Returns a shallow copy of the client without specified module aliases.

        This is equivalent to using the ``reset alias`` command.

        :param tuple[str] aliases: Module aliases to reset.

        See :py:meth:`State.without_module_aliases` for details.

    .. py:method:: with_config(config_dict=None, /, **config)

        Returns a shallow copy of the client with adjusted session config.

        This is equivalent to using the ``configure session set`` command.

        :type config_dict: dict[str, object] or None
        :param config_dict: This is an optional positional-only argument.

        :param dict[str, object] config:
            Adjust the config settings after applying ``config_dict`` if set.

        See :py:meth:`State.with_config` for details.

    .. py:method:: without_config(*config_names)

        Returns a shallow copy of the client without specified session config.

        This is equivalent to using the ``configure session reset`` command.

        :param tuple[str] config_names: Config to reset.

        See :py:meth:`State.without_config` for details.

    .. py:method:: with_globals(globals_dict=None, /, **globals_)

        Returns a shallow copy of the client with adjusted global values.

        This is equivalent to using the ``set global`` command.

        :type globals_dict: dict[str, object] or None
        :param globals_dict: This is an optional positional-only argument.

        :param dict[str, object] globals_:
            Adjust the global values after applying ``globals_dict`` if set.

        See :py:meth:`State.with_globals` for details.

    .. py:method:: without_globals(*global_names)

        Returns a shallow copy of the client without specified globals.

        This is equivalent to using the ``reset global`` command.

        :param tuple[str] global_names: Globals to reset.

        See :py:meth:`State.without_globals` for details.


.. _edgedb-python-asyncio-api-transaction:

Transactions
============

The most robust way to execute transactional code is to use
the ``transaction()`` loop API:

.. code-block:: python

    async for tx in client.transaction():
        async with tx:
            await tx.execute("INSERT User { name := 'Don' }")

Note that we execute queries on the ``tx`` object in the above
example, rather than on the original ``client`` object.

The ``tx`` object stores a connection acquired from the pool, so that all
queries can be executed on the same connection in the same transaction.
Transaction start is lazy. ``async for tx`` or ``async with tx`` won't acquire
the connection and start the transaction. It's only done when executing the
first query on the ``tx`` object. That connection is pinned to the ``tx``
object even when a reconnection is needed, until leaving the final
``async with`` transaction block.

The ``transaction()`` API guarantees that:

1. Transactions are executed atomically;
2. If a transaction is failed for any of the number of transient errors (i.e.
   a network failure or a concurrent update error), the transaction would
   be retried;
3. If any other, non-retryable exception occurs, the transaction is rolled
   back, and the exception is propagated, immediately aborting the
   ``transaction()`` block.

The key implication of retrying transactions is that the entire
nested code block can be re-run, including any non-querying
Python code. Here is an example:

.. code-block:: python

    async for tx in client.transaction():
        async with tx:
            user = await tx.query_single(
                "SELECT User { email } FILTER .login = <str>$login",
                login=login,
            )
            data = await httpclient.get(
                'https://service.local/email_info',
                params=dict(email=user.email),
            )
            user = await tx.query_single('''
                    UPDATE User FILTER .login = <str>$login
                    SET { email_info := <json>$data}
                ''',
                login=login,
                data=data,
            )

In the above example, the execution of the HTTP request would be retried
too. The core of the issue is that whenever a transaction is interrupted
the user's email might have been changed (as the result of a concurrent
transaction), so we have to redo all the work done.

Generally it's recommended to not execute any long running
code within the transaction unless absolutely necessary.

Transactions allocate expensive server resources, and having
too many concurrent long-running transactions will
negatively impact the performance of the DB server.

To rollback a transaction that is in progress raise an exception.

.. code-block:: python

   class RollBack(Exception):
       "A user defined exception."

   try:
       async for tx in client.transaction():
           async with tx:
               raise RollBack
   except RollBack:
       pass

See also:

* RFC1004_
* :py:meth:`AsyncIOClient.transaction()`



.. py:class:: AsyncIORetry

    Represents a wrapper that yields :py:class:`AsyncIOTransaction`
    object when iterating.

    See :py:meth:`AsyncIOClient.transaction()`
    method for an example.

    .. py:coroutinemethod:: __anext__()

        Yields :py:class:`AsyncIOTransaction` object every time transaction
        has to be repeated.

.. py:class:: AsyncIOTransaction

    Represents a transaction.

    Instances of this type are yielded by a :py:class:`AsyncIORetry` iterator.

    .. describe:: async with c:

        Start and commit/rollback the transaction
        automatically when entering and exiting the code inside the
        context manager block.

    .. py:coroutinemethod:: query(query, *args, **kwargs)

        Acquire a connection if the current transaction doesn't have one yet,
        and use it to run a query and return the results
        as an :py:class:`edgedb.Set` instance. The temporary
        connection is automatically returned back to the pool when exiting the
        transaction block.

        See :py:meth:`AsyncIOClient.query()
        <edgedb.AsyncIOClient.query>` for details.

    .. py:coroutinemethod:: query_single(query, *args, **kwargs)

        Acquire a connection if the current transaction doesn't have one yet,
        and use it to run an optional singleton-returning
        query and return its element. The temporary connection is automatically
        returned back to the pool when exiting the transaction block.

        See :py:meth:`AsyncIOClient.query_single()
        <edgedb.AsyncIOClient.query_single>` for details.

    .. py:coroutinemethod:: query_required_single(query, *args, **kwargs)

        Acquire a connection if the current transaction doesn't have one yet,
        and use it to run a singleton-returning query
        and return its element. The temporary connection is automatically
        returned back to the pool when exiting the transaction block.

        See :py:meth:`AsyncIOClient.query_required_single()
        <edgedb.AsyncIOClient.query_required_single>` for details.

    .. py:coroutinemethod:: query_json(query, *args, **kwargs)

        Acquire a connection if the current transaction doesn't have one yet,
        and use it to run a query and
        return the results as JSON. The temporary connection is automatically
        returned back to the pool when exiting the transaction block.

        See :py:meth:`AsyncIOClient.query_json()
        <edgedb.AsyncIOClient.query_json>` for details.

    .. py:coroutinemethod:: query_single_json(query, *args, **kwargs)

        Acquire a connection if the current transaction doesn't have one yet,
        and use it to run an optional singleton-returning
        query and return its element in JSON. The temporary connection is
        automatically returned back to the pool when exiting the transaction
        block.

        See :py:meth:`AsyncIOClient.query_single_json()
        <edgedb.AsyncIOClient.query_single_json>` for details.

    .. py:coroutinemethod:: query_required_single_json(query, *args, **kwargs)

        Acquire a connection if the current transaction doesn't have one yet,
        and use it to run a singleton-returning
        query and return its element in JSON. The temporary connection is
        automatically returned back to the pool when exiting the transaction
        block.

        See :py:meth:`AsyncIOClient.query_requried_single_json()
        <edgedb.AsyncIOClient.query_required_single_json>` for details.

    .. py:coroutinemethod:: execute(query)

        Acquire a connection if the current transaction doesn't have one yet,
        and use it to execute an EdgeQL command
        (or commands).  The temporary connection is automatically
        returned back to the pool when exiting the transaction block.

        See :py:meth:`AsyncIOClient.execute()
        <edgedb.AsyncIOClient.execute>` for details.

.. _RFC1004: https://github.com/edgedb/rfcs/blob/master/text/1004-transactions-api.rst
