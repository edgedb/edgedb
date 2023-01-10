.. _ref_reference_http_api:

HTTP API
========

Using HTTP, you may check the health of your EdgeDB instance, check metrics on
your instance, and make queries.

Your instance's URL takes the form of ``http://<hostname>:<port>/``. For
queries, you will append ``db/<database-name>/edgeql``.

.. note::

    Here's how to determine your local EdgeDB instance's HTTP server URL:

    - The ``hostname`` will be ``localhost``
    - Find the ``port`` by running ``edgedb instance list``. This will print a
      table of all EdgeDB instances on your machine, including their associated
      port number.
    - In most cases, ``database_name`` will be ``edgedb``. An EdgeDB *instance*
      can contain multiple databases. On initialization, a default database
      called ``edgedb`` is created; all queries are executed against this
      database unless otherwise specified.

    To determine the URL of a remote instance you have linked with the CLI, you
    can get both the hostname and port of the instance from the "Port" column
    of the ``edgedb instance list`` table (formatted as ``<hostname>:<port>``).
    The same guidance on local database names applies here.

.. _ref_reference_health_checks:

Health Checks
-------------

EdgeDB exposes endpoints to check for aliveness and readiness of your database
instance.

Aliveness
^^^^^^^^^

Check if your instance is alive.

.. code-block::

    http://<hostname>:<port>/server/status/alive

If your instance is alive, it will respond with a ``200`` status code and
``"OK"`` as the payload. Otherwise, it will respond with a ``50x`` or a network
error.

Readiness
^^^^^^^^^

Check if your instance is ready to receive queries.

.. code-block::

    http://<hostname>:<port>/server/status/ready

If your instance is ready, it will respond with a ``200`` status code and
``"OK"`` as the payload. Otherwise, it will respond with a ``50x`` or a network
error.


.. _ref_observability:

Observability
-------------

Retrieve instance metrics.

.. code-block::

    http://<hostname>:<port>/metrics

All EdgeDB instances expose a Prometheus-compatible endpoint available via GET
request. The following metrics are made available.

Processes
^^^^^^^^^

``compiler_process_spawns_total``
  **Counter.** Total number of compiler processes spawned.

``compiler_processes_current``
  **Gauge.** Current number of active compiler processes.

Backend connections and performance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
``backend_connections_total``
  **Counter.**
  Total number of backend connections established.

``backend_connections_current``
  **Gauge.** Current number of active backend connections.

``backend_connection_establishment_errors_total``
  **Counter.** Number of times the server could not establish a backend
  connection.

``backend_connection_establishment_latency``
  **Histogram.** Time it takes to establish a backend connection, in seconds.

``backend_query_duration``
  **Histogram.** Time it takes to run a query on a backend connection, in
  seconds.

Client connections
^^^^^^^^^^^^^^^^^^

``client_connections_total``
  **Counter.** Total number of clients.

``client_connections_current``
  **Gauge.** Current number of active clients.

``client_connections_idle_total``
  **Counter.** Total number of forcefully closed idle client connections.

Query compilation
^^^^^^^^^^^^^^^^^

``edgeql_query_compilations_total``
  **Counter.** Number of compiled/cached queries or scripts.

``edgeql_query_compilation_duration``
  **Histogram.** Time it takes to compile an EdgeQL query or script, in
  seconds.

Errors
^^^^^^

``background_errors_total``
  **Counter.** Number of unhandled errors in background server routines.

.. _ref_reference_http_querying:

Querying
--------

Before querying over HTTP, you must first enable the HTTP extension in your
schema. Add this to your schema, outside any ``module``:

.. code-block:: sdl

    using extension edgeql_http;

Then create a new migration and apply it using
:ref:`ref_cli_edgedb_migration_create` and
:ref:`ref_cli_edgedb_migrate`, respectively.

Your instance is now able to receive EdgeQL queries over HTTP.

.. note::

    Enabling the HTTP extension is only required for querying over HTTP. It is
    *not* required for health checks or observability.

Making a query request
^^^^^^^^^^^^^^^^^^^^^^

Make a query to your EdgeDB database using this URL:

.. code-block::

    http://<hostname>:<port>/db/<database-name>/edgeql

You may make queries via either the POST or GET HTTP method. Query requests can
take the following fields:

- ``query`` - contains the EdgeQL query string
- ``variables``- contains a JSON object where the keys are the parameter names
  from the query and the values are the arguments to be used in this execution
  of the query.

When using the GET method, supply ``query`` and ``variables`` as query
parameters. For a POST request, use the ``application/json`` content type and
submit a JSON payload with ``query`` and ``variables`` as top-level keys in
that payload as in this example:

Here's an example query you might want to run to insert a new person in your
database, as executed from the EdgeDB REPL:

.. code-block:: edgeql-repl

    db> insert Person { name := <str>$name };
    Parameter <str>$name: Pat
    {default::Person {id: e9009b00-8d4e-11ed-a556-c7b5bdd6cf7a}}

The query inserts a ``Person`` object. The object's ``name`` value is
parameterized in the query as ``$name``.

This GET request would run the same query (assuming the instance is local and
the database is named ``edgedb``):

.. lint-off

.. code-block::

    GET http://localhost:<port>/db/edgedb/edgeql?query=insert%20Person%20%7B%20name%20%3A%3D%20%3Cstr%3E$name%20%7D%3B&variables=%7B%22name%22%3A%20%22Pat%22%7D

.. lint-on

As you can see with even this simple query, URL encoding can quickly become
onerous with queries over GET.

Here's the JSON payload of a POST request to execute the query:

.. code-block::

    {
      "query": "insert Person { name := <str>$name };",
      "variables": { "name": "Pat" }
    }

Response
^^^^^^^^

The response format is the same for both methods. The body of the
response is JSON of the following form::

    {
      "data": [ ... ],
      "error": {
        "message": "Error message",
        "type": "ErrorType",
        "code": 123456
      }
    }

The ``data`` response field will contain the response set serialized
as a JSON array.

Note that the ``error`` field will only be present if an error
actually occurred. The ``error`` will further contain the ``message``
field with the error message string, the ``type`` field with the name
of the type of error and the ``code`` field with an integer
:ref:`error code <ref_protocol_error_codes>`.

.. note::

    Caution is advised when reading ``decimal`` or ``bigint`` values
    using the HTTP protocol because the results are provided in JSON
    format. The JSON specification does not have a limit on
    significant digits, so a ``decimal`` or a ``bigint`` number can be
    losslessly represented in JSON. However, JSON decoders in many
    languages will read all such numbers as some kind of of 32- or
    64-bit number type, which may result in errors or precision loss.
    If such loss is unacceptable, then consider casting the value into
    ``str`` and decoding it on the client side into a more appropriate
    type.
