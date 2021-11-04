.. _ref_admin_config:

====================
Server Configuration
====================

EdgeDB exposes a number of configuration parameters that affect its
behavior.  In this section we review the ways to change the server
configuration, as well as detail each available configuration parameter.


EdgeQL
======

:edb-alt-title: Setting Parameters with EdgeQL

The :eql:stmt:`CONFIGURE` command can be used to set the configuration
parameters using EdgeQL.  For example:

.. code-block:: edgeql

    CONFIGURE INSTANCE SET listen_addresses := {'127.0.0.1', '::1'};


edgedb configure
================

:edb-alt-title: Setting Parameters with "edgedb configure"

The :ref:`ref_cli_edgedb_configure` command allows modifying the system
configuration from a terminal:

.. code-block:: bash

    $ edgedb configure set listen_addresses 127.0.0.1 ::1


Configuration Parameters
========================

:edb-alt-title: Available Configuration Parameters

.. _ref_admin_config_connection:

Connection Settings
-------------------

:eql:synopsis:`listen_addresses (SET OF str)`
    Specifies the TCP/IP address(es) on which the server is to listen for
    connections from client applications.  If the list is empty, the server
    does not listen on any IP interface at all, in which case only Unix-domain
    sockets can be used to connect to it.

:eql:synopsis:`listen_port (int16)`
    The TCP port the server listens on; ``5656`` by default.  Note that the
    same port number is used for all IP addresses the server listens on.

:eql:synopsis:`Auth`
    A parameter class that specifies the rules of client authentication.
    Below are the properties of the ``Auth`` class.

    :eql:synopsis:`priority (int64)`
        The priority of the authentication rule.  The lower this number,
        the higher the priority.

    :eql:synopsis:`user (SET OF str)`
        The name(s) of the database role(s) this rule applies to.  If set to
        ``'*'``, then it applies to all roles.

    :eql:synopsis:`method`
        The name of the authentication method type.  Valid values are:
        ``Trust`` for no authentication and ``SCRAM`` for SCRAM-SHA-256
        password authentication.

    :eql:synopsis:`comment`
        An optional comment for the authentication rule.


Resource Usage
--------------

:eql:synopsis:`effective_io_concurrency (int64)`
    Sets the number of concurrent disk I/O operations that can be
    executed simultaneously. Corresponds to the PostgreSQL
    configuration parameter of the same name.

:eql:synopsis:`query_work_mem (str)`
    The amount of memory used by internal query operations such as
    sorting. Corresponds to the PostgreSQL ``work_mem`` configuration
    parameter.

:eql:synopsis:`shared_buffers (str)`
    The amount of memory the database uses for shared memory buffers.
    Corresponds to the PostgreSQL configuration parameter of the same
    name. Changing this value requires server restart.


Query Planning
--------------

:eql:synopsis:`default_statistics_target (str)`
    Sets the default data statistics target for the planner.
    Corresponds to the PostgreSQL configuration parameter of the same
    name.

:eql:synopsis:`effective_cache_size (str)`
    Sets the planner's assumption about the effective size of the disk
    cache that is available to a single query. Corresponds to the
    PostgreSQL configuration parameter of the same name.


Client Connections
------------------

:eql:synopsis:`client_idle_timeout (int32)`
    Sets the timeout (in milliseconds) for how long client connections
    can stay inactive before being forcefully closed by the server.

    The default is ``60_000`` milliseconds (60 seconds). Setting it to
    ``0`` disables the mechanism. Setting the timeout to less than
    ``2_000`` milliseconds is not recommended.

    Note that the actual time an idle connection can live can be up to
    two times longer than the specified timeout.

    This is a system-level config setting.
