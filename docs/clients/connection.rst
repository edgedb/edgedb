.. _ref_client_connection:

Connection Parameter Resolution
===============================

The CLI and client libraries (collectively referred to as "clients" below) must
connect to an EdgeDB instance to run queries or commands. There are several
ways of identifying an instance, and several ways to provide this connection
information to the client. These are outlined below.

######################
Specifying an instance
######################

When connecting to an EdgeDB database with the client libraries or CLI, there
are several ways to uniquely identify your instance.

+-----------------------+---------------------------+-------------------------+
| Parameter             | CLI flag                  | Environment variable    |
+=======================+===========================+=========================+
| Instance name         | ``--instance/-I <name>``  | ``EDGEDB_INSTANCE``     |
+-----------------------+---------------------------+-------------------------+
| DSN                   | ``--dsn <dsn>``           | ``EDGEDB_INSTANCE``     |
+-----------------------+---------------------------+-------------------------+
| Host and port         | ``--host/-H <host>``      | ``EDGEDB_HOST``         |
|                       |                           |                         |
|                       | ``--port/-P <port>``      | ``EDGEDB_PORT``         |
+-----------------------+---------------------------+-------------------------+
| Credentials file      | ``--credentials-file      | ``EDGEDB_               |
|                       | <path>``                  | CREDENTIALS_FILE``      |
+-----------------------+---------------------------+-------------------------+
| *Project linking*     | *N/A*                     | *N/A*                   |
+-----------------------+---------------------------+-------------------------+

Let's dig into each of these a bit more.

**Instance name**
  All local instances (instances created on your local machine using the CLI)
  are associated with a name. The name is all you need to connect; under the
  hood, the CLI stores the instance credentials (username, password, etc) on
  your file system in the EdgeDB config directory. The CLI and client libraries
  look up these credentials to connect. (Run the ``edgedb info`` command to see
  where credentials are stored on your machine.)

  You can also assign names to remote instances using :ref:`edgedb instance
  link <ref_cli_edgedb_instance_link>`. The CLI will save the credentials
  locally, so you can connect to a remote instance using just its name, just
  like a local instance.

**DSN**
  A DSN (data source name) is a connection string that can contain a full set
  of connection parameters (username, password, host, port) in a single string:
  ``edgedb://user:password@hostname.com:1234/db_name``. However, all components
  of the DSN are optional; technically ``edgedb://`` is a valid DSN. The
  unspecified values will fall back to their defaults:

  Host: ``"localhost"``
  Port: ``5656``
  User: ``"edgedb"``
  Password: ``null``
  Database name: ``edgedb``

**Host and port**
  In general, we recommend using a fully-qualified DSN when connecting to the
  database. But in some circumstances, it may only be necessary to specify a
  host or a port. For convenience, you may specify *one or both* of these in
  place of a DSN.

  When not specified, the host defaults to ``"localhost"`` and the port
  defaults to ``5656``.

**Credentials file**
  e.g. ``/path/to/credentials.json``.

  If you wish, you can store your credentials as a JSON file. Checking this
  file into version control could present a security risk and is not
  recommended.

  .. code-block:: json

    {
      "port": 10702,
      "user": "test3n",
      "password": "lZTBy1RVCfOpBAOwSCwIyBIR",
      "database": "test3n",
      "tls_cert_data": "-----BEGIN CERTIFICATE-----\nabcdef..."
    }

  Relative paths are resolved relative to the current working directory.

**Project-linked instances**
  When you run ``edgedb project init`` in a given directory, EdgeDB creates an
  instance and "links" it to that directory. There's nothing magical about this
  link; it's just a bit of metadata that gets stored in the EdgeDB config
  directory. When you use the client libraries or run a CLI command inside a
  project-linked directory, the library/CLI can detect this, look up the linked
  instance's credentials, and connect automatically.

  For more information on how this works, check out the `release post
  </blog/introducing-edgedb-projects>`_ for ``edgedb project``.

###############
Priority levels
###############

The section above described the various ways of specifying an instance to
connect to. There are also several *mechanisms* for providing this
configuration to a client: you can pass them explicitly as parameters/flags,
use environment variables, or rely on ``edgedb project``. The CLI and all
client libraries follow a simple algorithm for resolving any potential
ambiguities.

1. Check for **explicit connection parameters**. For security reasons,
   hard-coding connection information or credentials in your codebase is not
   recommended, though it may be useful for debugging or testing purposes. As
   such, explicitly provided parameters are given the highest priority.

   In the context of the client libraries, this means passing an option
   explicitly into the ``connect`` call. Here's how this looks using the
   JavaScript library:

   .. code-block:: javascript

      import * as edgedb from "edgedb";

      const pool = await edgedb.connect({
        instance: "my_instance"
      });

   In the context of the CLI, this means using the appropriate command-line
   flags:

   .. code-block:: bash

      $ edgedb --instance my_instance
      EdgeDB 1.x
      Type \help for help, \quit to quit.
      edgedb>


2. If no explicit parameters are provided, check for **environment variables**.

   This is the recommended mechanism for providing connection information to
   your EdgeDB client, especially in production or when running EdgeDB inside a
   container. All clients read the following variables from the environment:

   - ``EDGEDB_DSN``
   - ``EDGEDB_INSTANCE``
   - ``EDGEDB_CREDENTIALS_FILE``
   - ``EDGEDB_HOST`` / ``EDGEDB_PORT``

   .. warning::

      Ambiguity is not permitted. For instance, specifying both
      ``EDGEDB_INSTANCE`` and ``EDGEDB_DSN`` will result in an error. You *can*
      use ``EDGEDB_HOST`` and ``EDGEDB_PORT`` simultaneously.

3. Check whether the command/file is being executed inside a **project
   directory**

   If you are using ``edgedb project`` (which we recommend!) and haven't
   otherwise specified any connection parameters, the CLI and client libraries
   will connect to the instance that's been linked to your project.

   This makes it easy to get up and running with EdgeDB. Once you've run
   ``edgedb project init``, the CLI and client libraries will be able to
   connect to your database without any further configuration, as long as
   you're inside the project directory.

4. **Fail to connect.**
   If no connection information can be detected using the above mechanisms, the
   connection fails.

.. warning::

   Within a given priority level, you cannot specify multiple instances
   "instance selection parameters" simultaneously. For instance, specifying
   both ``EDGEDB_INSTANCE`` and ``EDGEDB_DSN`` will result in an error.

#####################
Connection parameters
#####################

In many scenarios, additional connection information is required.

+-----------------------+---------------------------+-------------------------+
| Parameter             | CLI flag                  | Environment variable    |
+=======================+===========================+=========================+
| User                  | ``--user/-u <user>``      | ``EDGEDB_USER``         |
+-----------------------+---------------------------+-------------------------+
| Password              | ``--password <pass>``     | ``EDGEDB_PASSWORD``     |
+-----------------------+---------------------------+-------------------------+
| Database              | ``--database/-d <name>``  | ``EDGEDB_DATABASE``     |
+-----------------------+---------------------------+-------------------------+


Let dig deeper into each of these connection parameters.

**User and password**
  These are the credentials of the database user account to connect to the
  EdgeDB instance. When specified, these values will **override** the username
  or password specified in a DSN, credentials file, etc.

  For instance, consider the following environment variables:

  .. code-block::

      EDGEDB_DSN=edgedb://olduser:oldpass@hostname.com:5656
      EDGEDB_USER=newuser
      EDGEDB_PASSWORD=newpass

    In this scenario, ``newuser`` will override ``olduser`` and ``newpass``
    will override ``oldpass``. The client library will try to connect to the
    instance with the following connection information:

    .. code-block::

      host: "hostname.com"
      port: 5656
      user: "newuser"
      password: "newpass"

**Database**
  Each EdgeDB *instance* can contain multiple *databases*. When in instance is
  created, a default database named ``edgedb`` is created. Unless otherwise
  specified, all incoming connections connect to the ``edgedb`` database.

  If specified, this database name will **override** the database name
  specified in DSN, credentials file, etc.

  .. code-block::

      EDGEDB_DSN=edgedb://hostname.com:5656/old_db
      EDGEDB_DATABASE=new_db

  The ``old_db`` specified in the DSN will be discarded and replaced with
  ``new_db``. Keep in mind that most users never create multiple databases
  within their EdgeDB instance and simply use the default database (named
  ``edgedb``) which is created when the instance is first initialized.


Override behavior
-----------------

There is still potential for ambiguity here. For instance, a DSN specified with
``EDGEDB_DSN`` may contain a username, password, and database name. What
happens if you also specify ``EDGEDB_USER``, ``EDGEDB_PASSWORD``, or
``EDGEDB_DATABASE``?

In this scenario, the more granular connection parameters will override the
less granular one. For instance, consider the following set of environment
variables:

.. code-block::

  EDGEDB_DSN=edgedb://olduser:password@hostname.com:5656
  EDGEDB_USER=newuser

  # client will connect to
  # edgedb://newuser:password@hostname.com:5656


Overriding across priority levels
---------------------------------

This override behavior only happens *same or lower priority level*. Explicit

- ``EDGEDB_PASSWORD`` **will** override the password specified in
  ``EDGEDB_DSN``
- ``EDGEDB_PASSWORD`` **will not** override the password specified in a DSN
  that was passed explicitly using the ``--dsn`` flag, because explicit
  configuration takes precedence over environment variables. In fact, if you
  pass the ``--dsn`` flag to the CLI, **all** environment variables will be
  ignored.

  To override the password of an explicit DSN, you need to pass it explicitly
  as well:

  .. code-block:: bash

     $ edgedb --dsn edgedb://username:oldpass@hostname.com --password qwerty
     # connects to edgedb://username:qwerty@hostname.com

- ``EDGEDB_PASSWORD`` **will** override the stored password associated with a
  project-linked instance. (This is unlikely to be desirable.)


##############
TLS parameters
##############

EdgeDB uses TLS by default for all connections. This

+-------------------------+--------------------------+------------------------+
| Parameter               | CLI flag                 | Environment variable   |
+=========================+==========================+========================+
| TLS Root Certificate(s) | ``--tls-ca-file <path>`` | ``EDGEDB_TLS_CA_FILE`` |
+-------------------------+--------------------------+------------------------+
| TLS Verify Hostname     | ``--tls-verify-hostname``| ``EDGEDB_TLS_VERIFY_   |
|                         |                          | HOSTNAME``             |
+-------------------------+--------------------------+------------------------+
| Insecure Dev Mode       | *N/A*                    | ``EDGEDB_INSECURE_     |
|                         |                          | DEV_MODE``             |
+-------------------------+--------------------------+------------------------+

**TLS root certificate(s)**
  TLS is required to connect to any EdgeDB instance. To do so, the client needs
  a reference to the root certificate of your instance's certificate chain.
  Typically this will be handled for you when you create a local instance or
  ``link`` a remote one.

  If you're using a globally trusted CA like Let's Encrypt, the root
  certificate will almost certainly exist already in your system's global
  certificate pool. In this case, you won't need to specify this path; it will
  be discovered automatically by the client.

  If you're self-issuing certificates, you must download the root certificate
  and provide a path to its location on the filesystem. Otherwise TLS will fail
  to connect.

**TLS verify hostname**
  Defaults to ``true``. However if you provide a custom TLS root certificate,
  hostname verification is disabled by default.

  When true, the Server Name Indication (SNI) TLS extension is enabled.

  This is a boolean value. For details on how to specify boolean values in
  environment variables, see the :ref:`Boolean parameters <ref_boolean_env>`
  section.

**Insecure dev mode**
  Defaults to ``false``.

  When true, the client will connect even when TLS validation fails. This is
  useful in development if you're running an EdgeDB instance in a Docker
  container. Don't use this in production.

.. _ref_boolean_env:

##################
Boolean parameters
##################

All environment variables are represented as strings. When representing a
boolean value such as ``EDGEDB_TLS_VERIFY_HOSTNAME``, any of the following
values are considered valid. All other values will throw an error.

.. code-block::

  True        False
  ----------------------
  "true"     "false"
  "t"        "f"
  "yes"      "no"
  "on"       "off"
  "1"        "0"

