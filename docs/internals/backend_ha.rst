Backend High-Availability
=========================

High availability especially in databases is usually a sophisticated and
systematic challenge. To help addressing the problem, EdgeDB server is
delivered to support selected highly-available backend (postgres) clusters,
namely in 2 categories:

* API-based HA
* Adaptive HA without API

With or without using an HA API, EdgeDB server will try its best to learn about
backend failovers in order to keep connecting to the leader node, when the
backend HA feature is enabled in EdgeDB as described below.

During backend failover, no frontend connections will be closed - a retryable
error will be raised instead if queries happen, until failover is done
successfully and backend connections are recovered. In other words, EdgeDB
clients will meet some retryable errors for the queries during a backend
failover, but the EdgeDB server will not cut off these frontend connections.
Depending on the algorithm given to ``retrying_transaction()``, the client
queries may even survive a backend failover.


API-based HA
------------

EdgeDB server accepts different types of backends by looking into the protocol
of the ``--backend-dsn`` command-line parameter. A regular ``postgres://`` DSN
means API-based HA is not enabled, while EdgeDB also supports the following
DSN protocols at the moment:

* ``stolon+consul+http://``
* ``stolon+consul+https://``

When using any of these DSNs, EdgeDB will build the actual DSN to the leader
node of the backend cluster by calling the corresponding API using credentials
in the ``--backend-dsn``, and subscribe to that API for failover events. Once
failover is detected, EdgeDB shall drop all backend connections and route all
new backend connections to the new leader node.

`Stolon <https://github.com/sorintlab/stolon/>`_ is an open-source cloud native
PostgreSQL manager for PostgreSQL high availability. Currently, EdgeDB supports
using a Stolon cluster as the backend in a Consul-based setup, where EdgeDB
acts as a Stolon proxy so that you only need to manage Stolon sentinels and
keepers, plus a Consul deployment. To use a Stolon cluster, run EdgeDB server
with a DSN like this example:

.. code-block:: bash

    $ edgedb-server \
        --backend-dsn stolon+consul+http://localhost:8500/my-cluster

EdgeDB will connect to the Consul HTTP service at ``localhost:8500``, and
subscribe to the updates of the cluster named ``my-cluster``.


Adaptive HA
-----------

EdgeDB also supports DNS-based generic HA backends like the cloud databases
with multi-AZ failover, or some custom HA Postgres cluster that keeps a DNS
name always resolved to the leader node. Adaptive HA can be enabled with a
switch in addition to a regular backend DSN:

.. code-block:: bash

    $ edgedb-server \
        --backend-dsn postgres://xxx.rds.amazonaws.com \
        --enable-backend-adaptive-ha

Once enabled, EdgeDB server will keep track of unusual backend events like
unexpected disconnects or Postgres shutdown notifications. When a threshold is
reached, EdgeDB would consider that the backend is now in "failover" state.
Likewise, EdgeDB will then drop all current backend connections, and tries to
establish new connections with the same backend DSN. Because EdgeDB doesn't
cache resolved DNS values, the new connections are likely going to the new
leader node.

Under the hood of adaptive HA, EdgeDB maintains a state machine to avoid
endless switch-overs in an unstable network, and state changes only happen when
certain conditions are met:

* ``Healthy`` - all is good
* ``Unhealthy`` - a staging state before failover
* ``Failover`` - backend failover is in process

**Rules of state switches:**

``Unhealthy`` -> ``Healthy``

* Successfully connected to a non-hot-standby backend.

``Unhealthy`` -> ``Failover``

* More than 60% (configurable with environment variable
  ``EDGEDB_SERVER_BACKEND_ADAPTIVE_HA_DISCONNECT_PERCENT``) of existing
  pgcons are "unexpectedly disconnected" (number of existing pgcons is
  captured at the moment we change to ``Unhealthy`` state, and maintained
  on "expected disconnects" too).
* (and) In ``Unhealthy`` state for more than 30 seconds (
  ``EDGEDB_SERVER_BACKEND_ADAPTIVE_HA_UNHEALTHY_MIN_TIME``).
* (and) sys_pgcon is down.
* (or) Postgres shutdown/hot-standby notification received.

``Healthy`` -> ``Unhealthy``

* Any unexpected disconnect.

``Healthy`` -> ``Failover``

* Postgres shutdown/hot-standby notification received.

``Failover`` -> ``Healthy``

* Successfully connected to a non-hot-standby backend.
* (and) sys_pgcon is healthy.

("pgcon" is a code name for backend connections, and "sys_pgcon" is a special
backend connection which EdgeDB uses to talk to the "EdgeDB system database".)
