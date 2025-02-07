.. _edgedb-js-driver:


Client
======

The ``Client`` class implements the basic functionality required to establish a
connection to your database and execute queries.

.. _edgedb-js-create-client:

Creating clients
----------------

A *client* represents a connection to your database and provides methods for
executing queries.

.. note::

  In actuality, the client maintains a *pool* of connections under the hood.
  When your server is under load, queries will be run in parallel across many
  connections, instead of being bottlenecked by a single connection.

To create a client:

.. code-block:: js

    const edgedb = require("edgedb");

    const client = edgedb.createClient();


If you're using TypeScript or have ES modules enabled, you can use
``import`` syntax instead:

.. code-block:: js

    import * as edgedb from "edgedb";

    const client = edgedb.createClient();


Connections
^^^^^^^^^^^

Notice we didn't pass any arguments into ``createClient``. That's intentional.

**In development**, we recommend using ``edgedb project init`` to create an
instance and link it to your project directory. As long as you're inside this
directory, ``createClient()`` with auto-detect the project and connect to the
associated instance automatically.

**In production** you should use environment variables to provide connection
information to ``createClient``. See the :ref:`Connection parameters
<ref_reference_connection>` docs for details.

Configuring clients
^^^^^^^^^^^^^^^^^^^

Clients can be configured using a set of *immutable* methods that start with
``with``.

.. note::

  These methods return a *new Client instance* that *shares a connection pool*
  with the original client! This is important. Each call to ``createClient``
  instantiates a new connection pool.

The code example below demonstrates all available configuration settings. The
value specified below is the *default value* for that setting.

.. code-block:: typescript

  import {createClient, Duration, IsolationLevel} from "edgedb";

  const baseClient = createClient();
  const client = baseClient
    .withConfig({
      // 10 seconds
      session_idle_transaction_timeout: Duration.from({seconds: 10}),
      // 0 seconds === no timeout
      query_execution_timeout: Duration.from({seconds: 0}),
      allow_bare_ddl: "NeverAllow",
      allow_user_specified_id: false,
      apply_access_policies: true,
    })
    .withRetryOptions({
      attempts: 3,
      backoff: (attemptNo: number) => {
        // exponential backoff
        return 2 ** attemptNo * 100 + Math.random() * 100;
      },
    })
    .withTransactionOptions({
      isolation: IsolationLevel.Serializable, // only supported value
      deferrable: false,
      readonly: false,
    });

Running queries
---------------

To execute a basic query:

.. code-block:: js

  const edgedb = require("edgedb");

  const client = edgedb.createClient();

  async function main() {
    const result = await client.query(`select 2 + 2;`);
    console.log(result); // [4]
  }


.. _edgedb-js-typescript:

In TypeScript, you can supply a type hint to receive a strongly typed result.

.. code-block:: js

  const result = await client.query<number>(`select 2 + 2;`);
  // number[]

``.query`` method
^^^^^^^^^^^^^^^^^

The ``.query`` method always returns an array of results. It places no
constraints on cardinality.

.. code-block:: js

  await client.query(`select 2 + 2;`); // [4]
  await client.query(`select [1, 2, 3];`); // [[1, 2, 3]]
  await client.query(`select <int64>{};`); // []
  await client.query(`select {1, 2, 3};`); // [1, 2, 3]

``.querySingle`` method
^^^^^^^^^^^^^^^^^^^^^^^

If you know your query will only return a single element, you can tell EdgeDB
to expect a *singleton result* by using the ``.querySingle`` method. This is
intended for queries that return *zero or one* elements. If the query returns
a set with more than one elements, the ``Client`` will throw a runtime error.

.. note::

  Note that if you're selecting an array or tuple, the returned value may
  still be an array.

.. code-block:: js

  await client.querySingle(`select 2 + 2;`); // 4
  await client.querySingle(`select [1, 2, 3];`); // [1, 2, 3]
  await client.querySingle(`select <int64>{};`); // null
  await client.querySingle(`select {1, 2, 3};`); // Error

``.queryRequiredSingle`` method
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use ``queryRequiredSingle`` for queries that return *exactly one* element. If
the query returns an empty set or a set with multiple elements, the ``Client``
will throw a runtime error.

.. code-block:: js

  await client.queryRequiredSingle(`select 2 + 2;`); // 4
  await client.queryRequiredSingle(`select [1, 2, 3];`); // [1, 2, 3]
  await client.queryRequiredSingle(`select <int64>{};`); // Error
  await client.queryRequiredSingle(`select {1, 2, 3};`); // Error

TypeScript
^^^^^^^^^^

The TypeScript signatures of these methods reflects their behavior.

.. code-block:: typescript

  await client.query<number>(`select 2 + 2;`);
  // number[]

  await client.querySingle<number>(`select 2 + 2;`);
  // number | null

  await client.queryRequiredSingle<number>(`select 2 + 2;`);
  // number


Type conversion
---------------

The client converts EdgeDB types into a corresponding JavaScript data
structure. Some EdgeDB types like ``duration`` don't have a corresponding type
in the JavaScript type system, so we've implemented classes like
:js:class:`Duration` to represent them.

.. list-table::

  * - **EdgeDB type**
    - **JavaScript type**
  * - Sets
    - ``Array``
  * - Arrays
    - ``Array``
  * - Tuples ``tuple<x, y, ...>``
    - ``Array``
  * - Named tuples ``tuple<foo: x, bar: y, ...>``
    - ``object``
  * - Enums
    - ``string``
  * - ``Object``
    - ``object``
  * - ``str``
    - ``string``
  * - ``bool``
    - ``boolean``
  * - ``float32`` ``float64`` ``int16`` ``int32`` ``int64``
    - ``number``
  * - ``json``
    - ``string``
  * - ``uuid``
    - ``string``
  * - ``bigint``
    - ``BigInt``
  * - ``decimal``
    - ``string``
  * - ``bytes``
    - ``Uint8Array``
  * - ``datetime``
    - ``Date``
  * - ``duration``
    - :js:class:`Duration`
  * - ``e.cal.relative_duration``
    - :js:class:`RelativeDuration`
  * - ``e.cal.date_duration``
    - :js:class:`DateDuration`
  * - ``cal::local_date``
    - :js:class:`LocalDate`
  * - ``cal::local_time``
    - :js:class:`LocalTime`
  * - ``cal::local_datetime``
    - :js:class:`LocalDateTime`
  * - ``cfg::memory``
    - :js:class:`ConfigMemory`
  * - Ranges ``range<x>``
    - :js:class:`Range`


To learn more about the client's built-in type classes, refer to the reference
documentation.

- :js:class:`Duration`
- :js:class:`RelativeDuration`
- :js:class:`DateDuration`
- :js:class:`LocalDate`
- :js:class:`LocalTime`
- :js:class:`LocalDateTime`
- :js:class:`ConfigMemory`
- :js:class:`Range`


.. .. note::

..   **A message for query builder users**

..   Everything below this point isn't necessary/applicable for query builder users. Continue to the :ref:`Query Builder <edgedb-js-qb>` docs.


JSON results
------------

Client provide additional methods for running queries and retrieving results
as a *serialized JSON string*. This serialization happens inside the database
and is typically more performant than running ``JSON.stringify`` yourself.

.. code-block:: js

  await client.queryJSON(`select {1, 2, 3};`);
  // "[1, 2, 3]"

  await client.querySingleJSON(`select <int64>{};`);
  // "null"

  await client.queryRequiredSingleJSON(`select 3.14;`);
  // "3.14"

Non-returning queries
---------------------

To execute a query without retrieving a result, use the ``.execute`` method.
This is especially useful for mutations, where there's often no need for the
query to return a value.

.. code-block:: js

  await client.execute(`insert Movie {
    title := "Avengers: Endgame"
  };`);

With EdgeDB 2.0 or later, you can execute a "script" consisting of multiple
semicolon-separated statements in a single ``.execute`` call.

.. code-block:: js

  await client.execute(`
    insert Person { name := "Robert Downey Jr." };
    insert Person { name := "Scarlett Johansson" };
    insert Movie {
      title := <str>$title,
      actors := (
        select Person filter .name in {
          "Robert Downey Jr.",
          "Scarlett Johansson"
        }
      )
    }
  `, { title: "Iron Man 2" });

Parameters
----------

If your query contains parameters (e.g. ``$foo``), you can pass in values as
the second argument. This is true for all ``query*`` methods and ``execute``.

.. code-block:: js

  const INSERT_MOVIE = `insert Movie {
    title := <str>$title
  }`
  const result = await client.querySingle(INSERT_MOVIE, {
    title: "Iron Man"
  });
  console.log(result);
  // {id: "047c5893..."}

Remember that :ref:`parameters <ref_eql_params>` can only be *scalars* or
*arrays of scalars*.

Scripts
-------

Both ``execute`` and the ``query*`` methods support scripts (queries
containing multiple statements). The statements are run in an implicit
transaction (unless already in an explicit transaction), so the whole script
remains atomic. For the ``query*`` methods only the result of the final
statement in the script will be returned.

.. code-block:: js

  const result = await client.query(`
    insert Movie {
      title := <str>$title
    };
    insert Person {
      name := <str>$name
    };
  `, {
    title: "Thor: Ragnarok",
    name: "Anson Mount"
  });
  // [{id: "5dd2557b..."}]

For more fine grained control of atomic exectution of multiple statements, use
the ``transaction()`` API.

Checking connection status
--------------------------

The client maintains a dynamically sized *pool* of connections under the hood.
These connections are initialized *lazily*, so no connection will be
established until the first time you execute a query.

If you want to explicitly ensure that the client is connected without running
a query, use the ``.ensureConnected()`` method.

.. code-block:: js

  const edgedb = require("edgedb");

  const client = edgedb.createClient();

  async function main() {
    await client.ensureConnected();
  }

.. _edgedb-js-api-transaction:

Transactions
------------

The most robust way to execute transactional code is to use
the ``transaction()`` API:

.. code-block:: js

    await client.transaction(tx => {
      await tx.execute("insert User {name := 'Don'}");
    });

Note that we execute queries on the ``tx`` object in the above
example, rather than on the original ``client`` object.

The ``transaction()`` API guarantees that:

1. Transactions are executed atomically;
2. If a transaction fails due to retryable error (like
   a network failure or a concurrent update error), the transaction
   would be retried;
3. If any other, non-retryable error occurs, the transaction is rolled
   back and the ``transaction()`` block throws.

The *transaction* object exposes ``query()``, ``execute()``, ``querySQL()``,
``executeSQL()``, and other ``query*()`` methods that *clients* expose, with
the only difference that queries will run within the current transaction
and can be retried automatically.

The key implication of retrying transactions is that the entire
nested code block can be re-run, including any non-querying
JavaScript code. Here is an example:

.. code-block:: js

    const email = "timmy@edgedb.com"

    await client.transaction(async tx => {
      await tx.execute(
        `insert User { email := <str>$email }`,
        { email },
      )

      await sendWelcomeEmail(email);

      await tx.execute(
        `insert LoginHistory {
          user := (select User filter .email = <str>$email),
          timestamp := datetime_current()
        }`,
        { email },
      )
    })

In the above example, the welcome email may be sent multiple times if the
transaction block is retried. Generally, the code inside the transaction block
shouldn't have side effects or run for a significant amount of time.

.. note::

  Transactions allocate expensive server resources and having
  too many concurrently running long-running transactions will
  negatively impact the performance of the DB server.

.. note::

  * RFC1004_
  * :js:meth:`Client.transaction\<T\>`

  .. _RFC1004: https://github.com/edgedb/rfcs/blob/master/text/1004-transactions-api.rst


Next up
-------

If you're a TypeScript user and want autocompletion and type inference, head
over to the :ref:`Query Builder docs <edgedb-js-qb>`. If you're using plain
JavaScript that likes writing queries with composable code-first syntax, you
should check out the query builder too! If you're content writing queries as
strings, the vanilla Client API will meet your needs.
