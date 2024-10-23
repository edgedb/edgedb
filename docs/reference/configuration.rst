.. _ref_admin_config:

====================
Server configuration
====================

EdgeDB exposes a number of configuration parameters that affect its
behavior.  In this section we review the ways to change the server
configuration, as well as detail each available configuration parameter.


Configuring the server
======================

EdgeQL
------

The :eql:stmt:`configure` command can be used to set the
configuration parameters using EdgeQL. For example:

.. code-block:: edgeql-repl

  edgedb> configure instance set listen_addresses := {'127.0.0.1', '::1'};
  CONFIGURE: OK

CLI
---

The :ref:`ref_cli_edgedb_configure` command allows modifying the system
configuration from a terminal:

.. code-block:: bash

  $ edgedb configure set listen_addresses 127.0.0.1 ::1


Available settings
==================

Below is an overview of available settings. a full reference of settings is
available at :ref:`Standard Library > Config <ref_std_cfg>`.


Connection settings
-------------------

:eql:synopsis:`listen_addresses -> multi str`
  The TCP/IP address(es) on which the server is to listen for
  connections from client applications.

:eql:synopsis:`listen_port -> int16`
  The TCP port the server listens on; defaults to ``5656``.

.. versionadded:: 5.0

    :eql:synopsis:`cors_allow_origins -> multi str`
      Origins that will be calling the server that need Cross-Origin Resource
      Sharing (CORS) support. Can use ``*`` to allow any origin. When HTTP
      clients make a preflight request to the server, the origins allowed here
      will be added to the ``Access-Control-Allow-Origin`` header in the
      response.

Resource usage
--------------

:eql:synopsis:`effective_io_concurrency -> int64`
  The number of concurrent disk I/O operations that can be
  executed simultaneously.

:eql:synopsis:`query_work_mem -> cfg::memory`
  The amount of memory used by internal query operations such as
  sorting.

:eql:synopsis:`shared_buffers -> cfg::memory`
  The amount of memory used for shared memory buffers.

:eql:synopsis:`http_max_connections -> int64`
  The maximum number of concurrent outbound HTTP connections to allow.

Query planning
--------------

:eql:synopsis:`default_statistics_target -> int64`
  The default data statistics target for the planner.

:eql:synopsis:`effective_cache_size -> cfg::memory`
  An estimate of the effective size of the disk
  cache available to a single query.


Query behavior
--------------

:eql:synopsis:`allow_bare_ddl -> cfg::AllowBareDDL`
  Allows for running bare DDL outside a migration. Possible values are
  ``cfg::AllowBareDDL.AlwaysAllow`` and ``cfg::AllowBareDDL.NeverAllow``.

  When you create an instance, this is set to ``cfg::AllowBareDDL.AlwaysAllow``
  until you run a migration. At that point it is set to
  ``cfg::AllowBareDDL.NeverAllow`` because it's generally a bad idea to mix
  migrations with bare DDL.

:eql:synopsis:`apply_access_policies -> bool`
  Determines whether access policies should be applied when running queries.
  Setting this to ``false`` effectively puts you into super-user mode, ignoring
  any access policies that might otherwise limit you on the instance.

  .. note::

      This setting can also be conveniently accessed via the "Config" dropdown
      menu at the top of the EdgeDB UI (accessible by running the CLI command
      ``edgedb ui`` from within a project). The setting will apply only to your
      UI session, so you won't have to remember to re-enable it when you're
      done.


Client connections
------------------

:eql:synopsis:`session_idle_timeout -> std::duration`
  How long client connections can stay inactive before being closed by the
  server. Defaults to 60 seconds; set to ``<duration>'0'`` to disable the
  mechanism.

:eql:synopsis:`session_idle_transaction_timeout -> std::duration`
  How long client connections can stay inactive
  while in a transaction. Defaults to 10 seconds; set to ``<duration>'0'`` to
  disable the mechanism.

  .. note::

      For ``session_idle_transaction_timeout`` and ``query_execution_timeout``,
      values under 1ms are rounded down to zero, which will disable the timeout.
      In order to set a timeout, please set a duration of 1ms or greater.

      ``session_idle_timeout`` can take values below 1ms.

:eql:synopsis:`query_execution_timeout -> std::duration`
  How long an individual query can run before being aborted. A value of
  ``<duration>'0'`` disables the mechanism; it is disabled by default.

  .. note::

      For ``session_idle_transaction_timeout`` and ``query_execution_timeout``,
      values under 1ms are rounded down to zero, which will disable the timeout.
      In order to set a timeout, please set a duration of 1ms or greater.

      ``session_idle_timeout`` can take values below 1ms.
