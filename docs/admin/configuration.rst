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

    CONFIGURE SYSTEM SET listen_addresses := {'127.0.0.1', '::1'};


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

:eql:synopsis:`Port`
    A parameter class that allows configuring application ports with the
    specified protocol.  Below are the properties of the ``Port`` class.
    All are required.

    :eql:synopsis:`address (SET OF str)`
        The TCP/IP address(es) for the application port.

    :eql:synopsis:`port (int16)`
        The TCP port for the application port.

    :eql:synopsis:`protocol (str)`
        The protocol for the application port.  Valid values are:
        ``'graphql+http'`` and ``'edgeql+http'``.

    :eql:synopsis:`database (str)`
        The name of the database the application port is attached to.

    :eql:synopsis:`user (str)`
        The name of the database role the application port is attached to.

    :eql:synopsis:`concurrency (int64)`
        The maximum number of backend connections available for this
        application port.

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

:eql:synopsis:`shared_buffers (str)`
    The amount of memory the database uses for shared memory buffers.
    Corresponds to the PostgreSQL configuration parameter of the same name.
    Changing this value requires server restart.

:eql:synopsis:`query_work_mem (str)`
    The amount of memory used by internal query operations such as sorting.
    Corresponds to the PostgreSQL ``work_mem`` configuration parameter.


Query Planning
--------------

:eql:synopsis:`effective_cache_size (str)`
    Sets the planner's assumption about the effective size of the disk cache
    that is available to a single query. Corresponds to the PostgreSQL
    configuration parameter of the same name

:eql:synopsis:`default_statistics_target (str)`
    Sets the default data statistics target for the planner.
    Corresponds to the PostgreSQL configuration parameter of the same name
