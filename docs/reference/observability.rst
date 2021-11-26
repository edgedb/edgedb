.. _ref_observability:

Observability
=============

All EdgeDB instances expose a Prometheus-compatible ``/metrics`` endpoint. The
following metrics are made available.


Processes
^^^^^^^^^

``compiler_process_spawns_total``
  **Counter.** Total number of compiler processes spawned.

``compiler_processes_current``
  **Gauge.** Current number of active compiler processes.

Backend connections and performance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
``backend_connections_total``
  **Counter**
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
