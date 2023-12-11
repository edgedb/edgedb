.. _ref_std_cfg:

======
Config
======

The ``cfg`` module contains a set of types and scalars used for configuring
EdgeDB.


.. list-table::
  :class: funcoptable

  * - **Type**
    - **Description**
  * - :eql:type:`cfg::Config`
    - The base type for all configuration objects. The properties of this type
      define the set of configuruation settings supported by EdgeDB.
  * - :eql:type:`cfg::Auth`
    - An object type representing an authentication profile.
  * - :eql:type:`cfg::ConnectionTransport`
    - An enum type representing the different protocols that EdgeDB speaks.
  * - :eql:type:`cfg::AuthMethod`
    - An abstract object type representing a method of authentication
  * - :eql:type:`cfg::Trust`
    - A subclass of ``AuthMethod`` indicating an "always trust" policy (no
      authentication).
  * - :eql:type:`cfg::SCRAM`
    - A subclass of ``AuthMethod`` indicating password-based authentication.
  * - :eql:type:`cfg::Password`
    - A subclass of ``AuthMethod`` indicating basic password-based
      authentication.
  * - :eql:type:`cfg::JWT`
    - A subclass of ``AuthMethod`` indicating token-based authentication.
  * - :eql:type:`cfg::memory`
    - A scalar type for storing a quantity of memory storage.



Configuration Parameters
========================

:edb-alt-title: Available Configuration Parameters

.. _ref_admin_config_connection:

Connection settings
-------------------

:eql:synopsis:`listen_addresses -> multi str`
  Specifies the TCP/IP address(es) on which the server is to listen for
  connections from client applications.  If the list is empty, the server
  does not listen on any IP interface at all.

:eql:synopsis:`listen_port -> int16`
  The TCP port the server listens on; ``5656`` by default.  Note that the
  same port number is used for all IP addresses the server listens on.

Resource usage
--------------

:eql:synopsis:`effective_io_concurrency -> int64`
  Sets the number of concurrent disk I/O operations that can be
  executed simultaneously. Corresponds to the PostgreSQL
  configuration parameter of the same name.

:eql:synopsis:`query_work_mem -> cfg::memory`
  The amount of memory used by internal query operations such as
  sorting. Corresponds to the PostgreSQL ``work_mem`` configuration
  parameter.

:eql:synopsis:`shared_buffers -> cfg::memory`
  The amount of memory the database uses for shared memory buffers.
  Corresponds to the PostgreSQL configuration parameter of the same
  name. Changing this value requires server restart.


Query planning
--------------

:eql:synopsis:`default_statistics_target -> int64`
  Sets the default data statistics target for the planner.
  Corresponds to the PostgreSQL configuration parameter of the same
  name.

:eql:synopsis:`effective_cache_size -> cfg::memory`
  Sets the planner's assumption about the effective size of the disk
  cache that is available to a single query. Corresponds to the
  PostgreSQL configuration parameter of the same name.


Query behavior
--------------

:eql:synopsis:`allow_bare_ddl -> cfg::AllowBareDDL`
  Allows for running bare DDL outside a migration. Possible values are
  ``cfg::AllowBareDDL.AlwaysAllow`` and ``cfg::AllowBareDDL.NeverAllow``.

  When you create an instance, this is set to ``cfg::AllowBareDDL.AlwaysAllow``
  until you run a migration. At that point it is set to
  ``cfg::AllowBareDDL.NeverAllow`` because it's generally a bad idea to mix
  migrations with bare DDL.

.. _ref_std_cfg_apply_access_policies:

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


.. _ref_std_cfg_client_connections:

Client connections
------------------

:eql:synopsis:`allow_user_specified_id -> bool`
  Makes it possible to set the ``.id`` property when inserting new objects.

  .. warning::

      Enabling this feature introduces some security vulnerabilities:

      1. An unprivileged user can discover ids that already exist in the
         database by trying to insert new values and noting when there is a
         constraint violation on ``.id`` even if the user doesn't have access
         to the relevant table.

      2. It allows re-using object ids for a different object type, which the
         application might not expect.

      Additionally, enabling can have serious performance implications as, on
      an ``insert``, every object type must be checked for collisions.

      As a result, we don't recommend enabling this. If you need to preserve
      UUIDs from an external source on your objects, it's best to create a new
      property to store these UUIDs. If you will need to filter on this
      external UUID property, you may add an :ref:`index
      <ref_datamodel_indexes>` on it.

:eql:synopsis:`session_idle_timeout -> std::duration`
  Sets the timeout for how long client connections can stay inactive
  before being forcefully closed by the server.

  Time spent on waiting for query results doesn't count as idling.
  E.g. if the session idle timeout is set to 1 minute it would be OK
  to run a query that takes 2 minutes to compute; to limit the query
  execution time use the ``query_execution_timeout`` setting.

  The default is 60 seconds. Setting it to ``<duration>'0'`` disables
  the mechanism. Setting the timeout to less than ``2`` seconds is not
  recommended.

  Note that the actual time an idle connection can live can be up to
  two times longer than the specified timeout.

  This is a system-level config setting.

:eql:synopsis:`session_idle_transaction_timeout -> std::duration`
  Sets the timeout for how long client connections can stay inactive
  while in a transaction.

  The default is 10 seconds. Setting it to ``<duration>'0'`` disables
  the mechanism.

:eql:synopsis:`query_execution_timeout -> std::duration`
  Sets a time limit on how long a query can be run.

  Setting it to ``<duration>'0'`` disables the mechanism.
  The timeout isn't enabled by default.

----------


.. eql:type:: cfg::Config

  An abstract type representing the configuration of an instance or database.

  The properties of this object type represent the set of configuration
  options supported by EdgeDB (listed above).


----------


.. eql:type:: cfg::Auth

  An object type designed to specify a client authentication profile.

  .. code-block:: edgeql-repl

    edgedb> configure instance insert
    .......   Auth {priority := 0, method := (insert Trust)};
    OK: CONFIGURE INSTANCE

  Below are the properties of the ``Auth`` class.

  :eql:synopsis:`priority -> int64`
    The priority of the authentication rule.  The lower this number,
    the higher the priority.

  :eql:synopsis:`user -> multi str`
    The name(s) of the database role(s) this rule applies to.  If set to
    ``'*'``, then it applies to all roles.

  :eql:synopsis:`method -> cfg::AuthMethod`
    The name of the authentication method type. Expects an instance of
    :eql:type:`cfg::AuthMethod`;  Valid values are:
    ``Trust`` for no authentication and ``SCRAM`` for SCRAM-SHA-256
    password authentication.

  :eql:synopsis:`comment -> optional str`
    An optional comment for the authentication rule.

---------

.. eql:type:: cfg::ConnectionTransport

  An enum listing the various protocols that EdgeDB can speak.

  Possible values are:

.. list-table::
  :class: funcoptable

  * - **Value**
    - **Description**
  * - ``cfg::ConnectionTransport.TCP``
    - EdgeDB binary protocol
  * - ``cfg::ConnectionTransport.TCP_PG``
    - Postgres protocol for the
      :ref:`SQL query mode <ref_sql_support>`
  * - ``cfg::ConnectionTransport.HTTP``
    - EdgeDB binary protocol
      :ref:`tunneled over HTTP <ref_http_tunnelling>`
  * - ``cfg::ConnectionTransport.SIMPLE_HTTP``
    - :ref:`EdgeQL over HTTP <ref_edgeql_http>`
      and :ref:`GraphQL <ref_graphql_index>` endpoints

---------

.. eql:type:: cfg::AuthMethod

  An abstract object class that represents an authentication method.

  It currently has four concrete subclasses, each of which represent an
  available authentication method: :eql:type:`cfg::SCRAM`,
  :eql:type:`cfg::JWT`, :eql:type:`cfg::Password`, and
  :eql:type:`cfg::Trust`.

  :eql:synopsis:`transports -> multi cfg::ConnectionTransport`
    Which connection transports this method applies to.
    The subclasses have their own defaults for this.

-------

.. eql:type:: cfg::Trust

  The ``cfg::Trust`` indicates an "always-trust" policy.

  When active, it disables password-based authentication.

  .. code-block:: edgeql-repl

    edgedb> configure instance insert
    .......   Auth {priority := 0, method := (insert Trust)};
    OK: CONFIGURE INSTANCE

-------

.. eql:type:: cfg::SCRAM

  ``cfg::SCRAM`` indicates password-based authentication.

  It uses a challenge-response scheme to avoid transmitting the
  password directly.  This policy is implemented via ``SCRAM-SHA-256``

  It is available for the ``TCP``, ``TCP_PG``, and ``HTTP`` transports
  and is the default for ``TCP`` and ``TCP_PG``.

  .. code-block:: edgeql-repl

    edgedb> configure instance insert
    .......   Auth {priority := 0, method := (insert SCRAM)};
    OK: CONFIGURE INSTANCE

-------

.. eql:type:: cfg::JWT

  ``cfg::JWT`` uses a JWT signed by the server to authenticate.

  It is available for the ``TCP``, ``HTTP``, and ``HTTP_SIMPLE`` transports
  and is the default for ``HTTP``.


-------

.. eql:type:: cfg::Password

  ``cfg::Password`` indicates simple password-based authentication.

  Unlike :eql:type:`cfg::SCRAM`, this policy transmits the password
  over the (encrypted) channel.  It is implemened using HTTP Basic
  Authentication over TLS.

  This policy is available only for the ``SIMPLE_HTTP`` transport, where it is
  the default.


-------

.. eql:type:: cfg::memory

  A scalar type representing a quantity of memory storage.

  As with ``uuid``, ``datetime``, and several other types, ``cfg::memory``
  values are declared by casting from an appropriately formatted string.

  .. code-block:: edgeql-repl

    db> select <cfg::memory>'1B'; # 1 byte
    {<cfg::memory>'1B'}
    db> select <cfg::memory>'5KiB'; # 5 kibibytes
    {<cfg::memory>'5KiB'}
    db> select <cfg::memory>'128MiB'; # 128 mebibytes
    {<cfg::memory>'128MiB'}

  The numerical component of the value must be a non-negative integer; the
  units must be one of ``B|KiB|MiB|GiB|TiB|PiB``. We're using the explicit
  ``KiB`` unit notation (1024 bytes) instead of ``kB`` (which is ambiguous,
  and may mean 1000 or 1024 bytes).
