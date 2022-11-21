.. _ref_reference_environment:

Environment Variables
=====================

The behavior of EdgeDB can the configured with environment variables. The
variables documented on this page are supported when using the
``edgedb-server`` tool and the official :ref:`Docker image
<ref_guide_deployment_docker>`.


.. _ref_reference_envvar_variants:

Variants
--------
Some environment variables (noted below) support ``*_FILE`` and ``*_ENV``
variants.

- The ``*_FILE`` variant expects its value to be a file name.  The
  file's contents will be read and used as the value. This is useful for
  referencing files that are mounted in the container.
- The ``*_ENV`` variant expects its value to be the name of another
  environment variable. The value of the other environment variable is then
  used as the final value. This is convenient in deployment scenarios where
  relevant values are auto populated into fixed environment variables.

Supported variables
-------------------

EDGEDB_SERVER_BOOTSTRAP_COMMAND
...............................

Specifies one or more EdgeQL statements to run at bootstrap. If specified,
overrides ``EDGEDB_SERVER_PASSWORD``, ``EDGEDB_SERVER_PASSWORD_HASH``,
``EDGEDB_SERVER_USER`` and ``EDGEDB_SERVER_DATABASE``. Useful to fine-tune
initial user and database creation, and other initial setup. If neither the
``EDGEDB_SERVER_BOOTSTRAP_COMMAND`` variable or the
``EDGEDB_SERVER_BOOTSTRAP_SCRIPT_FILE`` are explicitly specified, the container
will look for the presence of ``/edgedb-bootstrap.edgeql`` in the container
(which can be placed in a derived image).

The ``*_FILE`` and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_BOOTSTRAP_SCRIPT_FILE
...................................

Run the script when initializing the database. The script is run by default
user within default database.


EDGEDB_SERVER_DEFAULT_AUTH_METHOD
.................................

Optionally specifies the authentication method used by the server instance.
Supported values are ``SCRAM`` (the default) and ``Trust``.  When set to
``Trust``, the database will allow complete unauthenticated access for all who
have access to the database port.  In this case the ``EDGEDB_SERVER_PASSWORD``
(or equivalent) setting is not required.

Use at your own risk and only for development and testing.


EDGEDB_SERVER_TLS_CERT_MODE
...........................

Specifies what to do when the TLS certificate and key are either not specified
or are missing.  When set to ``require_file``, the TLS certificate and key must
be specified in the ``EDGEDB_SERVER_TLS_CERT`` and ``EDGEDB_SERVER_TLS_KEY``
variables and both must exist.  When set to ``generate_self_signed`` a new
self-signed certificate and private key will be generated and placed in the
path specified by ``EDGEDB_SERVER_TLS_CERT`` and ``EDGEDB_SERVER_TLS_KEY``, if
those are set, otherwise the generated certificate and key are stored as
``edbtlscert.pem`` and ``edbprivkey.pem`` in ``EDGEDB_SERVER_DATADIR``, or, if
``EDGEDB_SERVER_DATADIR`` is not set then they will be placed in
``/etc/ssl/edgedb``.

The default is ``generate_self_signed`` when
``EDGEDB_SERVER_SECURITY=insecure_dev_mode``. Otherwise the default is
``require_file``.

The ``*_FILE`` and ``*_ENV`` variants are also supported.

EDGEDB_SERVER_GENERATE_SELF_SIGNED_CERT
.......................................

.. warning::

   Deprecated: use ``EDGEDB_SERVER_TLS_CERT_MODE=generate_self_signed``
   instead.

Set this option to ``1`` to tell the server to automatically generate a
self-signed certificate with key file in the ``EDGEDB_SERVER_DATADIR`` (if
present, see below), and echo the certificate content in the logs. If the
certificate file exists, the server will use it instead of generating a new
one.

Self-signed certificates are usually used in development and testing, you
should likely provide your own certificate and key file with the variables
below.


EDGEDB_SERVER_TLS_CERT/EDGEDB_SERVER_TLS_KEY
............................................

The TLS certificate and private key, exclusive with
``EDGEDB_SERVER_TLS_CERT_MODE=generate_self_signed``.

The ``*_FILE`` and ``*_ENV`` variants are also supported.

EDGEDB_SERVER_SECURITY
......................

When set to ``insecure_dev_mode``, sets ``EDGEDB_SERVER_DEFAULT_AUTH_METHOD``
to ``Trust`` (see above), and ``EDGEDB_SERVER_TLS_CERT_MODE`` to
``generate_self_signed`` (unless an explicit TLS certificate is specified).
Finally, if this option is set, the server will accept plaintext HTTP
connections.

Use at your own risk and only for development and testing.


EDGEDB_SERVER_PORT
..................

Specifies the network port on which EdgeDB will listen inside the container.
The default is ``5656``.  This usually doesn't need to be changed unless you
run in ``host`` networking mode.

Maps directly to the ``edgedb-server`` flag ``--port``. The ``*_FILE`` and
``*_ENV`` variants are also supported.


EDGEDB_SERVER_BIND_ADDRESS
..........................

Specifies the network interface on which EdgeDB will listen inside the
container.  The default is ``0.0.0.0``, which means all interfaces.  This
usually doesn't need to be changed unless you run in ``host`` networking mode.

Maps directly to the ``edgedb-server`` flag ``--bind-address``. The ``*_FILE``
and ``*_ENV`` variants are also supported.


.. _ref_reference_docer_edgedb_server_datadir:

EDGEDB_SERVER_DATADIR
.....................

Specifies a path within the container in which the database files are located.
Defaults to ``/var/lib/edgedb/data``.  The container needs to be able to change
the ownership of the mounted directory to ``edgedb``.  Cannot be specified at
the same time with ``EDGEDB_SERVER_BACKEND_DSN``.

Maps directly to the ``edgedb-server`` flag ``--data-dir``.


.. _ref_reference_docker_edgedb_server_backend_dsn:

EDGEDB_SERVER_BACKEND_DSN
.........................

Specifies a PostgreSQL connection string in the `URI format`_.  If set, the
PostgreSQL cluster specified by the URI is used instead of the builtin
PostgreSQL server.  Cannot be specified at the same time with
``EDGEDB_SERVER_DATADIR``.

Maps directly to the ``edgedb-server`` flag ``--backend-dsn``. The ``*_FILE``
and ``*_ENV`` variants are also supported.

.. _URI format:
   https://www.postgresql.org/docs/13/libpq-connect.html#id-1.7.3.8.3.6


EDGEDB_SERVER_RUNSTATE_DIR
..........................

Specifies a path within the container in which EdgeDB will place its Unix
socket and other transient files.

Maps directly to the ``edgedb-server`` flag ``--runstate-dir``.

EDGEDB_SERVER_ADMIN_UI
......................

Set to ``enabled`` to enable the web-based admininstrative UI for the instance.

EDGEDB_SERVER_EXTRA_ARGS
........................

Extra arguments to be passed to EdgeDB server.
