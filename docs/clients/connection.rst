.. _ref_client_connection:

Connection Resolution
=====================

When connecting to your database with the client libraries or CLI, there are
several ways to uniquely identify your instance. These are:


####################
Aggregate parameters
####################

There are several ways to uniquely identify an EdgeDB instance. Collectively,
these ways are referred to as "aggregate parameters", since they contain all
the information required to securely connect to an EdgeDB instance.

+-----------------------+---------------------------+-------------------------+
| Parameter             | CLI flag                  | Environment variable    |
+=======================+===========================+=========================+
| Instance name         | ``--instance/-I <name>``  | ``EDGEDB_INSTANCE``     |
|                       |                           |                         |
+-----------------------+---------------------------+-------------------------+
| DSN                   | ``--dsn <dsn>``           | ``EDGEDB_INSTANCE``     |
+-----------------------+---------------------------+-------------------------+
| Credentials file      | ``--credentials-file``    | ``EDGEDB_               |
|                       |  <path to file>           | CREDENTIALS_FILE``      |
+-----------------------+---------------------------+-------------------------+
| Host                  | ``--host/-H <host>``      | ``EDGEDB_HOST``         |
+-----------------------+---------------------------+-------------------------+
| Port                  | ``--port/-P <port>``      | ``EDGEDB_PORT``         |
+-----------------------+---------------------------+-------------------------+

Let's dig into each of these a bit more.

**Instance name**
  All local instances are associated with a name. This name is sufficient to
  connect to the database; the credentials (username, password, etc) for local
  instances are stored on your file system in the EdgeDB config directory. The
  EdgeDB CLI and client libraries use the instance name to look up the
  appropriate credentials and create a connection.

  Run the ``edgedb info`` command to see where credentials are stored on your
  machine.

  You can also give names to remote instances using :ref:`edgedb instance link
  <ref_cli_edgedb_instance_link>`. The CLI will save the credentials locally,
  so you can connect to the remote instance using just its name, just like a
  local instance.

**DSN**
  A DSN (data source name) is a connection string that can contain a full set
  of connection parameters (username, password, host, port) in a single string:
  ``edgedb://user:password@db.domain.com:1234``. However, all components of the
  DSN are optional; technically ``edgedb://`` is a valid DSN. The unspecified
  values will fall back to their defaults:

  Host: ``"localhost"``
  Port: ``5656``
  User: ``"edgedb"``
  Password: ``null``

**Host and port**
  In general, we recommend using a fully-qualified DSN when connecting to the
  database. But in some circumstances, it may only be necessary to specify a
  host or a port. For convenience, you may specify one or both of these in
  place of a DSN.

  When not specified, the host defaults to ``"localhost"`` and the port
  defaults to ``5656``.

**Credentials file**
  e.g. ``/path/to/credentials.json``.

  If you wish, you can store your credentials as a JSON file. Checking this
  file into version control could present a security risk and is not
  recommended.

  .. code-block:: json

    // credentials.json
    {
      "port": 10702,
      "user": "test3n",
      "password": "lZTBy1RVCfOpBAOwSCwIyBIR",
      "database": "test3n",
      "tls_cert_data": "-----BEGIN CERTIFICATE-----\nabcdef..."
    }

  Relative paths are resolved relative to the current working directory.

###############
Priority levels
###############

There are several ways to specify these connection options. In order of
priority:

1. Explicit values. For security reasons, hard-coding connection information or
   credentials in your codebase is not recommended, though it may be required
   for debugging or testing purposes. As such, explicitly provided parameters
   are given the highest priority.

   In the case of the CLI, you can explicitly pass connection parameters using
   the appropriate command-line flags:

   .. code-block:: bash

      $ edgedb --instance my_instance
      EdgeDB 1.x
      Type \help for help, \quit to quit.
      edgedb>

   When using the client libraries, this means passing an option explicitly
   into the ``connect`` call. Here's how this looks using the JavaScript
   library:

   .. code-block:: javascript

      import * as edgedb from "edgedb";

      const pool = await edgedb.createPool({
        instance: "my_instance"
      });

   Within a given priority level, you cannot provide multiple aggregate
   parameters. For instance, providing both ``EDGEDB_INSTANCE`` and
   ``EDGEDB_DSN`` will result in an error.

   .. code-block:: javascript

      import * as edgedb from "edgedb";

      const pool = await edgedb.createPool({
        instance: "my_instance",
        dsn: "edgedb://hostname.com:1234"
      });


2. Environment variables. This is the recommended mechanism for providing
   connection information to your EdgeDB client, especially in production. All
   client libraries read the following variables:

   - ``EDGEDB_DSN``
   - ``EDGEDB_INSTANCE``
   - ``EDGEDB_CREDENTIALS_FILE``
   - ``EDGEDB_HOST`` / ``EDGEDB_PORT``



3. Project-linked instances.

   If you are using ``edgedb project`` (which we recommend!) and haven't
   otherwise specified any connection parameters, the CLI and client libraries
   will connect to the instance that's been linked to your project.

   This makes it easy to get up and running with EdgeDB. Once you've run
   ``edgedb project init``, the CLI and client libraries will be able to
   connect to your database without any further configuration, as long as
   you're inside the project directory.

.. warning::

   Within a given priority level, you cannot provide multiple aggregate
   parameters. For instance, providing both ``EDGEDB_INSTANCE`` and
   ``EDGEDB_DSN`` will result in an error.


###################
Granular parameters
###################

In many scenarios, additional information is required. These are known as
"granular parameters":

+-----------------------+---------------------------+-------------------------+
| Parameter             | CLI flag                  | Environment variable    |
+=======================+===========================+=========================+
| User                  | ``--user/-u <user>``      | ``EDGEDB_USER``         |
+-----------------------+---------------------------+-------------------------+
| Password              | ``--password <password>`` | ``EDGEDB_PASSWORD``     |
+-----------------------+---------------------------+-------------------------+
| Database              | ``--database/-d <dbname>``| ``EDGEDB_DATABASE``     |
+-----------------------+---------------------------+-------------------------+
| TLS Certificate       | ``--tls-ca-file <path>``  | ``EDGEDB_TLS_CA_FILE``  |
+-----------------------+---------------------------+-------------------------+
| TLS Verify Hostname   | ``--tls-verify-hostname`` | ``EDGEDB_TLS_VERIFY_    |
|                       |                           | HOSTNAME``              |
+-----------------------+---------------------------+-------------------------+


Let dig deeper into each of these granular parameters.

**User and password**
  These are the credentials required to connect to the EdgeDB instance.

**Database**
  Each EdgeDB *instance* can contain multiple *databases*. When in instance is
  created, a default database named ``edgedb`` is created. Unless otherwise
  specified, all incoming connections connect to the ``edgedb`` database.

**TLS certificate**
  TLS is required to connect to any EdgeDB instance. To create a secure
  connection, the instance's TLS certificate must be downloaded and made
  available to the client library. Typically this will be handled for you when
  you create a local instance or ``link`` a remote one, but if you need to
  specify a custom certificate, you can use the parameter to do so.

**TLS verify hostname**
  Sometimes TLS can be a headache in development, especially when running your
  EdgeDB instance in a local Docker container. In this scenario, you can
  disable client-side TLS verification with this parameter.


#################
Override behavior
#################

Granular parameters are so named because they can override a *particular
element* of an aggregate parameter. For instance, consider the following set of
environment variables:

.. code-block::

  EDGEDB_DSN=edgedb://olduser:password@hostname.com:5656
  EDGEDB_USER=newuser

In this scenario, ``newuser`` will override ``olduser``, and the client library
will try to connect to the instance with the following connection information:

.. code-block::

  host: "hostname.com"
  port: 5656
  user: "newuser"
  password: "password"


Overriding across priority levels
---------------------------------

A granular parameter can only override aggregate parameters in the *same or
lower priority level*. For instance, if you pass the ``--instance`` flag to the
CLI, **all** environment variables will be ignored.

.. code-block:: bash

  $ EDGEDB_PORT=1234 edgedb --dsn edgedb://hostname.com:5656
  # connects to edgedb://hostname.com:5656
  # the environment variable is ignored


To override the DSN's password, you need to pass it as an explicitly:

.. code-block:: bash

  $ edgedb --dsn edgedb://hostname.com:5656 --port 1234
  # connects to edgedb://hostname.com:1234


.. Why aren't host and port granular?
.. ----------------------------------

##################
Boolean parameters
##################

All environment variables are represented as strings. When representing a
boolean value such as ``EDGEDB_TLS_VERIFY_HOSTNAME``, any of the following
values are considered valid. All other values will throw an error.

.. code-block::

  True values: "true" | "t" | "yes" | "on" | "1"
  False values: "false" | "f" | "no" | "off" | "0"
