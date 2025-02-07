.. _edgedb-js-api-reference:

#########
Reference
#########

.. _edgedb-js-api-client:

Client
======

.. js:function:: createClient( \
        options: string | ConnectOptions | null \
    ): Client

    Creates a new :js:class:`Client` instance.

    :param options:
        This is an optional parameter. When it is not specified the client
        will connect to the current EdgeDB Project instance.

        If this parameter is a string it can represent either a
        DSN or an instance name:

        * when the string does not start with ``edgedb://`` it is a
          :ref:`name of an instance <ref_reference_connection_instance_name>`;

        * otherwise it specifies a single string in the connection URI format:
          ``edgedb://user:password@host:port/database?option=value``.

          See the :ref:`Connection Parameters <ref_reference_connection>`
          docs for full details.

        Alternatively the parameter can be a ``ConnectOptions`` config;
        see the documentation of valid options below.

    :param string options.dsn:
        Specifies the DSN of the instance.

    :param string options.credentialsFile:
        Path to a file containing credentials.

    :param string options.host:
        Database host address as either an IP address or a domain name.

    :param number options.port:
        Port number to connect to at the server host.

    :param string options.user:
        The name of the database role used for authentication.

    :param string options.database:
        The name of the database to connect to.

    :param string options.password:
        Password to be used for authentication, if the server requires one.

    :param string options.tlsCAFile:
        Path to a file containing the root certificate of the server.

    :param boolean options.tlsSecurity:
        Determines whether certificate and hostname verification is enabled.
        Valid values are ``'strict'`` (certificate will be fully validated),
        ``'no_host_verification'`` (certificate will be validated, but
        hostname may not match), ``'insecure'`` (certificate not validated,
        self-signed certificates will be trusted), or ``'default'`` (acts as
        ``strict`` by default, or ``no_host_verification`` if ``tlsCAFile``
        is set).

    The above connection options can also be specified by their corresponding
    environment variable. If none of ``dsn``, ``credentialsFile``, ``host`` or
    ``port`` are explicitly specified, the client will connect to your
    linked project instance, if it exists. For full details, see the
    :ref:`Connection Parameters <ref_reference_connection>` docs.


    :param number options.timeout:
        Connection timeout in milliseconds.

    :param number options.waitUntilAvailable:
        If first connection fails, the number of milliseconds to keep retrying
        to connect (Defaults to 30 seconds). Useful if your development
        instance and app are started together, to allow the server time to
        be ready.

    :param number options.concurrency:
        The maximum number of connection the ``Client`` will create in it's
        connection pool. If not specified the concurrency will be controlled
        by the server. This is recommended as it allows the server to better
        manage the number of client connections based on it's own available
        resources.

    :returns:
        Returns an instance of :js:class:`Client`.

    Example:

    .. code-block:: js

        // Use the Node.js assert library to test results.
        const assert = require("assert");
        const edgedb = require("edgedb");

        async function main() {
          const client = edgedb.createClient();

          const data = await client.querySingle("select 1 + 1");

          // The result is a number 2.
          assert(typeof data === "number");
          assert(data === 2);
        }

        main();


.. js:class:: Client

    A ``Client`` allows you to run queries on an EdgeDB instance.

    Since opening connections is an expensive operation, ``Client`` also
    maintains a internal pool of connections to the instance, allowing
    connections to be automatically reused, and you to run multiple queries
    on the client simultaneously, enhancing the performance of
    database interactions.

    :js:class:`Client` is not meant to be instantiated directly;
    :js:func:`createClient` should be used instead.


    .. _edgedb-js-api-async-optargs:

    .. note::

        Some methods take query arguments as an *args* parameter. The type of
        the *args* parameter depends on the query:

        * If the query uses positional query arguments, the *args* parameter
          must be an ``array`` of values of the types specified by each query
          argument's type cast.
        * If the query uses named query arguments, the *args* parameter must
          be an ``object`` with property names and values corresponding to
          the query argument names and type casts.

        If a query argument is defined as ``optional``, the key/value can be
        either omitted from the *args* object or be a ``null`` value.

    .. js:method:: execute(query: string, args?: QueryArgs): Promise<void>

        Execute an EdgeQL command (or commands).

        :param query: Query text.

        This method takes :ref:`optional query arguments
        <edgedb-js-api-async-optargs>`.

        Example:

        .. code-block:: js

            await client.execute(`
                CREATE TYPE MyType {
                    CREATE PROPERTY a -> int64
                };

                for x in {100, 200, 300}
                union (insert MyType { a := x });
            `)

    .. js:method:: query<T>(query: string, args?: QueryArgs): Promise<T[]>

        Run an EdgeQL query and return the results as an array.
        This method **always** returns an array.

        This method takes :ref:`optional query arguments
        <edgedb-js-api-async-optargs>`.

    .. js:method:: queryRequired<T>( \
            query: string, \
            args?: QueryArgs \
        ): Promise<[T, ...T[]]>

        Run a query that returns at least one element and return the result as an
        array.

        This method takes :ref:`optional query arguments
        <edgedb-js-api-async-optargs>`.

        The *query* must return at least one element. If the query less than one
        element, a ``ResultCardinalityMismatchError`` error is thrown.

    .. js:method:: querySingle<T>( \
            query: string, \
            args?: QueryArgs \
        ): Promise<T | null>

        Run an optional singleton-returning query and return the result.

        This method takes :ref:`optional query arguments
        <edgedb-js-api-async-optargs>`.

        The *query* must return no more than one element. If the query returns
        more than one element, a ``ResultCardinalityMismatchError`` error is
        thrown.

    .. js:method:: queryRequiredSingle<T>( \
            query: string, \
            args?: QueryArgs \
        ): Promise<T>

        Run a singleton-returning query and return the result.

        This method takes :ref:`optional query arguments
        <edgedb-js-api-async-optargs>`.

        The *query* must return exactly one element. If the query returns
        more than one element, a ``ResultCardinalityMismatchError`` error is
        thrown. If the query returns an empty set, a ``NoDataError`` error is
        thrown.

    .. js:method:: queryJSON(query: string, args?: QueryArgs): Promise<string>

        Run a query and return the results as a JSON-encoded string.

        This method takes :ref:`optional query arguments
        <edgedb-js-api-async-optargs>`.

        .. note::

            Caution is advised when reading ``decimal`` or ``bigint``
            values using this method. The JSON specification does not
            have a limit on significant digits, so a ``decimal`` or a
            ``bigint`` number can be losslessly represented in JSON.
            However, JSON decoders in JavaScript will often read all
            such numbers as ``number`` values, which may result in
            precision loss. If such loss is unacceptable, then
            consider casting the value into ``str`` and decoding it on
            the client side into a more appropriate type, such as
            BigInt_.

    .. js:method:: queryRequiredJSON( \
            query: string, \
            args?: QueryArgs \
        ): Promise<string>

        Run a query that returns at least one element and return the result as a
        JSON-encoded string.

        This method takes :ref:`optional query arguments
        <edgedb-js-api-async-optargs>`.

        The *query* must return at least one element. If the query less than one
        element, a ``ResultCardinalityMismatchError`` error is thrown.

        .. note::

            Caution is advised when reading ``decimal`` or ``bigint``
            values using this method. The JSON specification does not
            have a limit on significant digits, so a ``decimal`` or a
            ``bigint`` number can be losslessly represented in JSON.
            However, JSON decoders in JavaScript will often read all
            such numbers as ``number`` values, which may result in
            precision loss. If such loss is unacceptable, then
            consider casting the value into ``str`` and decoding it on
            the client side into a more appropriate type, such as
            BigInt_.

    .. js:method:: querySingleJSON( \
            query: string, \
            args?: QueryArgs \
        ): Promise<string>

        Run an optional singleton-returning query and return its element
        as a JSON-encoded string.

        This method takes :ref:`optional query arguments
        <edgedb-js-api-async-optargs>`.

        The *query* must return at most one element.  If the query returns
        more than one element, an ``ResultCardinalityMismatchError`` error
        is thrown.

        .. note::

            Caution is advised when reading ``decimal`` or ``bigint``
            values using this method. The JSON specification does not
            have a limit on significant digits, so a ``decimal`` or a
            ``bigint`` number can be losslessly represented in JSON.
            However, JSON decoders in JavaScript will often read all
            such numbers as ``number`` values, which may result in
            precision loss. If such loss is unacceptable, then
            consider casting the value into ``str`` and decoding it on
            the client side into a more appropriate type, such as
            BigInt_.

    .. js:method:: queryRequiredSingleJSON( \
            query: string, \
            args?: QueryArgs \
        ): Promise<string>

        Run a singleton-returning query and return its element as a
        JSON-encoded string.

        This method takes :ref:`optional query arguments
        <edgedb-js-api-async-optargs>`.

        The *query* must return exactly one element.  If the query returns
        more than one element, a ``ResultCardinalityMismatchError`` error
        is thrown. If the query returns an empty set, a ``NoDataError`` error
        is thrown.

        .. note::

            Caution is advised when reading ``decimal`` or ``bigint``
            values using this method. The JSON specification does not
            have a limit on significant digits, so a ``decimal`` or a
            ``bigint`` number can be losslessly represented in JSON.
            However, JSON decoders in JavaScript will often read all
            such numbers as ``number`` values, which may result in
            precision loss. If such loss is unacceptable, then
            consider casting the value into ``str`` and decoding it on
            the client side into a more appropriate type, such as
            BigInt_.

    .. js:method:: executeSQL(query: string, args?: unknown[]): Promise<void>

        Execute a SQL command.

        :param query: SQL query text.

        This method takes optional query arguments.

        Example:

        .. code-block:: js

            await client.executeSQL(`
              INSERT INTO "MyType"(prop) VALUES ("value");
            `)

    .. js:method:: querySQL<T>(query: string, args?: unknown[]): Promise<T[]>

        Run a SQL query and return the results as an array.
        This method **always** returns an array.

        The array will contain the returned rows. By default, rows are
        ``Objects`` with columns addressable by name.

        This can controlled with ``client.withSQLRowMode('array' | 'object')``
        API.

        This method takes optional query arguments.

        Example:

        .. code-block:: js

            let vals = await client.querySQL(`SELECT 1 as foo`)
            console.log(vals); // [{'foo': 1}]

            vals = await client
              .withSQLRowMode('array')
              .querySQL(`SELECT 1 as foo`);

            console.log(vals); // [[1]]

    .. js:method:: transaction<T>( \
            action: (tx: Transaction) => Promise<T> \
        ): Promise<T>

        Execute a retryable transaction. The ``Transaction`` object passed to
        the action function has the same ``execute`` and ``query*`` methods
        as ``Client``.

        This is the preferred method of initiating and running a database
        transaction in a robust fashion.  The ``transaction()`` method
        will attempt to re-execute the transaction body if a transient error
        occurs, such as a network error or a transaction serialization error.
        The number of times ``transaction()`` will attempt to execute the
        transaction, and the backoff timeout between retries can be
        configured with :js:meth:`Client.withRetryOptions`.

        See :ref:`edgedb-js-api-transaction` for more details.

        Example:

        .. code-block:: js

            await client.transaction(async tx => {
              const value = await tx.querySingle("select Counter.value")
              await tx.execute(
                `update Counter set { value := <int64>$value }`,
                {value: value + 1},
              )
            });

        Note that we are executing queries on the ``tx`` object rather
        than on the original ``client``.

    .. js:method:: ensureConnected(): Promise<Client>

        If the client does not yet have any open connections in its pool,
        attempts to open a connection, else returns immediately.

        Since the client lazily creates new connections as needed (up to the
        configured ``concurrency`` limit), the first connection attempt will
        only occur when the first query is run a client. ``ensureConnected``
        can be useful to catch any errors resulting from connection
        mis-configuration by triggering the first connection attempt
        explicitly.

        Example:

        .. code-block:: js

            import {createClient} from 'edgedb';

            async function getClient() {
              try {
                return await createClient('custom_instance').ensureConnected();
              } catch (err) {
                // handle connection error
              }
            }

            function main() {
              const client = await getClient();

              await client.query('select ...');
            }

    .. js:method:: withGlobals(globals: {[name: string]: any}): Client

        Returns a new ``Client`` instance with the specified global values.
        The ``globals`` argument object is merged with any existing globals
        defined on the current client instance.

        Equivalent to using the ``set global`` command.

        Example:

        .. code-block:: js

            const user = await client.withGlobals({
              userId: '...'
            }).querySingle(`
              select User {name} filter .id = global userId
            `);

    .. js:method:: withModuleAliases(aliases: {[name: string]: string}): Client

        Returns a new ``Client`` instance with the specified module aliases.
        The ``aliases`` argument object is merged with any existing module
        aliases defined on the current client instance.

        If the alias ``name`` is ``module`` this is equivalent to using
        the ``set module`` command, otherwise it is equivalent to the
        ``set alias`` command.

        Example:

        .. code-block:: js

            const user = await client.withModuleAliases({
              module: 'sys'
            }).querySingle(`
              select get_version_as_str()
            `);
            // "2.0"

    .. js:method:: withConfig(config: {[name: string]: any}): Client

        Returns a new ``Client`` instance with the specified client session
        configuration. The ``config`` argument object is merged with any
        existing session config defined on the current client instance.

        Equivalent to using the ``configure session`` command. For available
        configuration parameters refer to the
        :ref:`Config documentation <ref_std_cfg>`.

    .. js:method:: withRetryOptions(opts: { \
            attempts?: number \
            backoff?: (attempt: number) => number \
        }): Client

        Returns a new ``Client`` instance with the specified retry attempts
        number and backoff time function (the time that retrying methods will
        wait between retry attempts, in milliseconds), where options not given
        are inherited from the current client instance.

        The default number of attempts is ``3``. The default backoff
        function returns a random time between 100 and 200ms multiplied by
        ``2 ^ attempt number``.

        .. note::

            The new client instance will share the same connection pool as the
            client it's created from, so calling the ``ensureConnected``,
            ``close`` and ``terminate`` methods will affect all clients
            sharing the pool.

        Example:

        .. code-block:: js

            import {createClient} from 'edgedb';

            function main() {
              const client = createClient();

              // By default transactions will retry if they fail
              await client.transaction(async tx => {
                // ...
              });

              const nonRetryingClient = client.withRetryOptions({
                attempts: 1
              });

              // This transaction will not retry
              await nonRetryingClient.transaction(async tx => {
                // ...
              });
            }

    .. js:method:: close(): Promise<void>

        Close the client's open connections gracefully. When a client is
        closed, all its underlying connections are awaited to complete their
        pending operations, then closed. A warning is produced if the pool
        takes more than 60 seconds to close.

        .. note::

            Clients will not prevent Node.js from exiting once all of it's
            open connections are idle and Node.js has no further tasks it is
            awaiting on, so it is not necessary to explicitly call ``close()``
            if it is more convenient for your application.

            (This does not apply to Deno, since Deno is missing the
            required API's to ``unref`` idle connections)

    .. js:method:: isClosed(): boolean

        Returns true if ``close()`` has been called on the client.

    .. js:method:: terminate(): void

        Terminate all connections in the client, closing all connections non
        gracefully. If the client is already closed, return without doing
        anything.


.. _edgedb-js-datatypes:

Type conversion
===============

The client automatically converts EdgeDB types to the corresponding JavaScript
types and vice versa.

The table below shows the correspondence between EdgeDB and JavaScript types.


.. list-table::

  * - **EdgeDB Type**
    - **JavaScript Type**
  * - ``multi`` set
    - ``Array``
  * - ``array<anytype>``
    - ``Array``
  * - ``anytuple``
    - ``Array``
  * - ``anyenum``
    - ``string``
  * - ``Object``
    - ``object``
  * - ``bool``
    - ``boolean``
  * - ``bytes``
    - ``Uint8Array``
  * - ``str``
    - ``string``
  * - ``float32``,  ``float64``, ``int16``, ``int32``, ``int64``
    - ``number``
  * - ``bigint``
    - ``BigInt``
  * - ``decimal``
    - n/a
  * - ``json``
    - ``unknown``
  * - ``uuid``
    - ``string``
  * - ``datetime``
    - ``Date``
  * - ``cal::local_date``
    - :js:class:`LocalDate`
  * - ``cal::local_time``
    - :js:class:`LocalTime`
  * - ``cal::local_datetime``
    - :js:class:`LocalDateTime`
  * - ``duration``
    - :js:class:`Duration`
  * - ``cal::relative_duration``
    - :js:class:`RelativeDuration`
  * - ``cal::date_duration``
    - :js:class:`DateDuration`
  * - ``range<anytype>``
    - :js:class:`Range`
  * - ``cfg::memory``
    - :js:class:`ConfigMemory`


.. note::

    Inexact single-precision ``float`` values may have a different
    representation when decoded into a JavaScript number.  This is inherent
    to the implementation of limited-precision floating point types.
    If you need the decimal representation to match, cast the expression
    to ``float64`` in your query.

.. note::

    Due to precision limitations the ``decimal`` type cannot be decoded to a
    JavaScript number. Use an explicit cast to ``float64`` if the precision
    degradation is acceptable or a cast to ``str`` for an exact decimal
    representation.


Arrays
======

EdgeDB ``array``  maps onto the JavaScript ``Array``.

.. code-block:: js

    // Use the Node.js assert library to test results.
    const assert = require("assert");
    const edgedb = require("edgedb");

    async function main() {
      const client = edgedb.createClient("edgedb://edgedb@localhost/");

      const data = await client.querySingle("select [1, 2, 3]");

      // The result is an Array.
      assert(data instanceof Array);
      assert(typeof data[0] === "number");
      assert(data.length === 3);
      assert(data[2] === 3);
    }

    main();

.. _edgedb-js-types-object:

Objects
=======

``Object`` represents an object instance returned from a query. The value of an
object property or a link can be accessed through a corresponding object key:

.. code-block:: js

    // Use the Node.js assert library to test results.
    const assert = require("assert");
    const edgedb = require("edgedb");

    async function main() {
      const client = edgedb.createClient("edgedb://edgedb@localhost/");

      const data = await client.querySingle(`
        select schema::Property {
            name,
            annotations: {name, @value}
        }
        filter .name = 'listen_port'
            and .source.name = 'cfg::Config'
        limit 1
      `);

      // The property 'name' is accessible.
      assert(typeof data.name === "string");
      // The link 'annotaions' is accessible and is a Set.
      assert(typeof data.annotations === "object");
      assert(data.annotations instanceof edgedb.Set);
      // The Set of 'annotations' is array-like.
      assert(data.annotations.length > 0);
      assert(data.annotations[0].name === "cfg::system");
      assert(data.annotations[0]["@value"] === "true");
    }

    main();

Tuples
======

A regular EdgeDB ``tuple`` becomes an ``Array`` in JavaScript.

.. code-block:: js

    // Use the Node.js assert library to test results.
    const assert = require("assert");
    const edgedb = require("edgedb");

    async function main() {
      const client = edgedb.createClient("edgedb://edgedb@localhost/");

      const data = await client.querySingle(`
        select (1, 'a', [3])
      `);

      // The resulting tuple is an Array.
      assert(data instanceof Array);
      assert(data.length === 3);
      assert(typeof data[0] === "number");
      assert(typeof data[1] === "string");
      assert(data[2] instanceof Array);
    }

    main();

Named Tuples
============

A named EdgeDB ``tuple`` becomes an ``Array``-like ``object`` in JavaScript,
where the elements are accessible either by their names or indexes.

.. code-block:: js

    // Use the Node.js assert library to test results.
    const assert = require("assert");
    const edgedb = require("edgedb");

    async function main() {
      const client = edgedb.createClient("edgedb://edgedb@localhost/");

      const data = await client.querySingle(`
        select (a := 1, b := 'a', c := [3])
      `);

      // The resulting tuple is an Array.
      assert(data instanceof Array);
      assert(data.length === 3);
      assert(typeof data[0] === "number");
      assert(typeof data[1] === "string");
      assert(data[2] instanceof Array);
      // Elements can be accessed by their names.
      assert(typeof data.a === "number");
      assert(typeof data["b"] === "string");
      assert(data.c instanceof Array);
    }

    main();


Local Date
==========

.. js:class:: LocalDate(\
        year: number, \
        month: number, \
        day: number)

    A JavaScript representation of an EdgeDB ``local_date`` value. Implements
    a subset of the `TC39 Temporal Proposal`_ ``PlainDate`` type.

    Assumes the calendar is always `ISO 8601`_.

    .. js:attribute:: year: number

        The year value of the local date.

    .. js:attribute:: month: number

        The numerical month value of the local date.

        .. note::

            Unlike the JS ``Date`` object, months in ``LocalDate`` start at 1.
            ie. Jan = 1, Feb = 2, etc.

    .. js:attribute:: day: number

        The day of the month value of the local date (starting with 1).

    .. js:attribute:: dayOfWeek: number

        The weekday number of the local date. Returns a value between 1 and 7
        inclusive, where 1 = Monday and 7 = Sunday.

    .. js:attribute:: dayOfYear: number

        The ordinal day of the year of the local date. Returns a value between
        1 and 365 (or 366 in a leap year).

    .. js:attribute:: weekOfYear: number

        The ISO week number of the local date. Returns a value between 1 and
        53, where ISO week 1 is defined as the week containing the first
        Thursday of the year.

    .. js:attribute:: daysInWeek: number

        The number of days in the week of the local date. Always returns 7.

    .. js:attribute:: daysInMonth: number

        The number of days in the month of the local date. Returns a value
        between 28 and 31 inclusive.

    .. js:attribute:: daysInYear: number

        The number of days in the year of the local date. Returns either 365 or
        366 if the year is a leap year.

    .. js:attribute:: monthsInYear: number

        The number of months in the year of the local date. Always returns 12.

    .. js:attribute:: inLeapYear: boolean

        Return whether the year of the local date is a leap year.

    .. js:method:: toString(): string

        Get the string representation of the ``LocalDate`` in the
        ``YYYY-MM-DD`` format.

    .. js:method:: toJSON(): number

        Same as :js:meth:`~LocalDate.toString`.

    .. js:method:: valueOf(): never

        Always throws an Error. ``LocalDate`` objects are not comparable.


Local Time
==========

.. js:class:: LocalTime(\
        hour: number = 0, \
        minute: number = 0, \
        second: number = 0, \
        millisecond: number = 0, \
        microsecond: number = 0, \
        nanosecond: number = 0)

    A JavaScript representation of an EdgeDB ``local_time`` value. Implements
    a subset of the `TC39 Temporal Proposal`_ ``PlainTime`` type.

    .. note::

        The EdgeDB ``local_time`` type only has microsecond precision, any
        nanoseconds specified in the ``LocalTime`` will be ignored when
        encoding to an EdgeDB ``local_time``.

    .. js:attribute:: hour: number

        The hours component of the local time in 0-23 range.

    .. js:attribute:: minute: number

        The minutes component of the local time in 0-59 range.

    .. js:attribute:: second: number

        The seconds component of the local time in 0-59 range.

    .. js:attribute:: millisecond: number

        The millisecond component of the local time in 0-999 range.

    .. js:attribute:: microsecond: number

        The microsecond component of the local time in 0-999 range.

    .. js:attribute:: nanosecond: number

        The nanosecond component of the local time in 0-999 range.

    .. js:method:: toString(): string

        Get the string representation of the ``local_time`` in the ``HH:MM:SS``
        24-hour format.

    .. js:method:: toJSON(): string

        Same as :js:meth:`~LocalTime.toString`.

    .. js:method:: valueOf(): never

        Always throws an Error. ``LocalTime`` objects are not comparable.


Local Date and Time
===================

.. js:class:: LocalDateTime(\
        year: number, \
        month: number, \
        day: number, \
        hour: number = 0, \
        minute: number = 0, \
        second: number = 0, \
        millisecond: number = 0, \
        microsecond: number = 0, \
        nanosecond: number = 0) extends LocalDate, LocalTime

    A JavaScript representation of an EdgeDB ``local_datetime`` value.
    Implements a subset of the `TC39 Temporal Proposal`_ ``PlainDateTime``
    type.

    Inherits all properties from the :js:class:`~LocalDate` and
    :js:class:`~LocalTime` types.

    .. js:method:: toString(): string

        Get the string representation of the ``local_datetime`` in the
        ``YYYY-MM-DDTHH:MM:SS`` 24-hour format.

    .. js:method:: toJSON(): string

        Same as :js:meth:`~LocalDateTime.toString`.

    .. js:method:: valueOf(): never

        Always throws an Error. ``LocalDateTime`` objects are not comparable.


Duration
========

.. js:class:: Duration(\
        years: number = 0, \
        months: number = 0, \
        weeks: number = 0, \
        days: number = 0, \
        hours: number = 0, \
        minutes: number = 0, \
        seconds: number = 0, \
        milliseconds: number = 0, \
        microseconds: number = 0, \
        nanoseconds: number = 0)

    A JavaScript representation of an EdgeDB ``duration`` value. This class
    attempts to conform to the `TC39 Temporal Proposal`_ ``Duration`` type as
    closely as possible.

    No arguments may be infinite and all must have the same sign.
    Any non-integer arguments will be rounded towards zero.

    .. note::

        The Temporal ``Duration`` type can contain both absolute duration
        components, such as hours, minutes, seconds, etc. and relative
        duration components, such as years, months, weeks, and days, where
        their absolute duration changes depending on the exact date they are
        relative to (eg. different months have a different number of days).

        The EdgeDB ``duration`` type only supports absolute durations, so any
        ``Duration`` with non-zero years, months, weeks, or days will throw
        an error when trying to encode them.

    .. note::

        The EdgeDB ``duration`` type only has microsecond precision, any
        nanoseconds specified in the ``Duration`` will be ignored when
        encoding to an EdgeDB ``duration``.

    .. note::

        Temporal ``Duration`` objects can be unbalanced_, (ie. have a greater
        value in any property than it would naturally have, eg. have a seconds
        property greater than 59), but EdgeDB ``duration`` objects are always
        balanced.

        Therefore in a round-trip of a ``Duration`` object to EdgeDB and back,
        the returned object, while being an equivalent duration, may not
        have exactly the same property values as the sent object.

    .. js:attribute:: years: number

        The number of years in the duration.

    .. js:attribute:: months: number

        The number of months in the duration.

    .. js:attribute:: weeks: number

        The number of weeks in the duration.

    .. js:attribute:: days: number

        The number of days in the duration.

    .. js:attribute:: hours: number

        The number of hours in the duration.

    .. js:attribute:: minutes: number

        The number of minutes in the duration.

    .. js:attribute:: seconds: number

        The number of seconds in the duration.

    .. js:attribute:: milliseconds: number

        The number of milliseconds in the duration.

    .. js:attribute:: microseconds: number

        The number of microseconds in the duration.

    .. js:attribute:: nanoseconds: number

        The number of nanoseconds in the duration.

    .. js:attribute:: sign: number

        Returns -1, 0, or 1 depending on whether the duration is negative,
        zero or positive.

    .. js:attribute:: blank: boolean

        Returns ``true`` if the duration is zero.

    .. js:method:: toString(): string

        Get the string representation of the duration in `ISO 8601 duration`_
        format.

    .. js:method:: toJSON(): number

        Same as :js:meth:`~Duration.toString`.

    .. js:method:: valueOf(): never

        Always throws an Error. ``Duration`` objects are not comparable.


RelativeDuration
================

.. js:class:: RelativeDuration(\
        years: number = 0, \
        months: number = 0, \
        weeks: number = 0, \
        days: number = 0, \
        hours: number = 0, \
        minutes: number = 0, \
        seconds: number = 0, \
        milliseconds: number = 0, \
        microseconds: number = 0)

  A JavaScript representation of an EdgeDB
  :eql:type:`cal::relative_duration` value. This type represents a
  non-definite span of time such as "2 years 3 days". This cannot be
  represented as a :eql:type:`duration` because a year has no absolute
  duration; for instance, leap years are longer than non-leap years.

  This class attempts to conform to the `TC39 Temporal Proposal`_
  ``Duration`` type as closely as possible.

  Internally, a ``cal::relative_duration`` value is represented as an
  integer number of months, days, and seconds. During encoding, other units
  will be normalized to these three. Sub-second units like ``microseconds``
  will be ignored.

  .. js:attribute:: years: number

      The number of years in the relative duration.

  .. js:attribute:: months: number

      The number of months in the relative duration.

  .. js:attribute:: weeks: number

      The number of weeks in the relative duration.

  .. js:attribute:: days: number

      The number of days in the relative duration.

  .. js:attribute:: hours: number

      The number of hours in the relative duration.

  .. js:attribute:: minutes: number

      The number of minutes in the relative duration.

  .. js:attribute:: seconds: number

      The number of seconds in the relative duration.

  .. js:attribute:: milliseconds: number

      The number of milliseconds in the relative duration.

  .. js:attribute:: microseconds: number

      The number of microseconds in the relative duration.

  .. js:method:: toString(): string

      Get the string representation of the duration in `ISO 8601 duration`_
      format.

  .. js:method:: toJSON(): string

      Same as :js:meth:`~Duration.toString`.

  .. js:method:: valueOf(): never

      Always throws an Error. ``RelativeDuration`` objects are not
      comparable.


DateDuration
============

.. js:class:: DateDuration( \
      years: number = 0, \
      months: number = 0, \
      weeks: number = 0, \
      days: number = 0, \
    )

  A JavaScript representation of an EdgeDB
  :eql:type:`cal::date_duration` value. This type represents a
  non-definite span of time consisting of an integer number of *months* and
  *days*.

  This type is primarily intended to simplify logic involving
  :eql:type:`cal::local_date` values.

  .. code-block:: edgeql-repl

    db> select <cal::date_duration>'5 days';
    {<cal::date_duration>'P5D'}
    db> select <cal::local_date>'2022-06-25' + <cal::date_duration>'5 days';
    {<cal::local_date>'2022-06-30'}
    db> select <cal::local_date>'2022-06-30' - <cal::local_date>'2022-06-25';
    {<cal::date_duration>'P5D'}

  Internally, a ``cal::relative_duration`` value is represented as an
  integer number of months and days. During encoding, other units will be
  normalized to these two.

  .. js:attribute:: years: number

      The number of years in the relative duration.

  .. js:attribute:: months: number

      The number of months in the relative duration.

  .. js:attribute:: weeks: number

      The number of weeks in the relative duration.

  .. js:attribute:: days: number

      The number of days in the relative duration.

  .. js:method:: toString(): string

      Get the string representation of the duration in `ISO 8601 duration`_
      format.

  .. js:method:: toJSON(): string

      Same as :js:meth:`~Duration.toString`.

  .. js:method:: valueOf(): never

      Always throws an Error. ``DateDuration`` objects are not comparable.


Memory
======

.. js:class:: ConfigMemory(bytes: BigInt)

  A JavaScript representation of an EdgeDB ``cfg::memory`` value.

  .. js:attribute:: bytes: number

      The memory value in bytes (B).

      .. note::

          The EdgeDB ``cfg::memory`` represents a number of bytes stored as
          an ``int64``. Since JS the ``number`` type is a ``float64``, values
          above ``~8191TiB`` will lose precision when represented as a JS
          ``number``. To keep full precision use the ``bytesBigInt``
          property.

  .. js::attribute:: bytesBigInt: BigInt

      The memory value in bytes represented as a ``BigInt``.

  .. js:attribute:: kibibytes: number

      The memory value in kibibytes (KiB).

  .. js:attribute:: mebibytes: number

      The memory value in mebibytes (MiB).

  .. js:attribute:: gibibytes: number

      The memory value in gibibytes (GiB).

  .. js:attribute:: tebibytes: number

      The memory value in tebibytes (TiB).

  .. js:attribute:: pebibytes: number

      The memory value in pebibytes (PiB).

  .. js:method:: toString(): string

      Get the string representation of the memory value. Format is the same
      as returned by string casting a ``cfg::memory`` value in EdgeDB.

Range
=====

.. js:class:: Range(\
        lower: T | null, \
        upper: T | null, \
        incLower: boolean = true, \
        incUpper: boolean = false \
    )

  A JavaScript representation of an EdgeDB ``std::range`` value. This is a generic TypeScript class with the following type signature.

  .. code-block:: typescript

      class Range<
          T extends number | Date | LocalDate | LocalDateTime | Duration
      >{
          // ...
      }

  .. js:attribute:: lower: T

      The lower bound of the range value.

  .. js:attribute:: upper: T

      The upper bound of the range value.

  .. js:attribute:: incLower: boolean

      Whether the lower bound is inclusive.

  .. js:attribute:: incUpper: boolean

      Whether the upper bound is inclusive.

  .. js:attribute:: empty: boolean

      Whether the range is empty.

  .. js:method:: toJSON(): { \
        lower: T | null; \
        upper: T | null; \
        inc_lower: boolean; \
        inc_upper: boolean; \
        empty?: undefined; \
      }

      Returns a JSON-encodable representation of the range.

  .. js:method:: empty(): Range

      A static method to declare an empty range (no bounds).

      .. code-block:: typescript

          Range.empty();




.. _BigInt:
    https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/BigInt
.. _TC39 Temporal Proposal: https://tc39.es/proposal-temporal/docs/
.. _ISO 8601: https://en.wikipedia.org/wiki/ISO_8601#Dates
.. _ISO 8601 duration: https://en.wikipedia.org/wiki/ISO_8601#Durations
.. _unbalanced: https://tc39.es/proposal-temporal/docs/balancing.html
