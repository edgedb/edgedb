.. _ref_reference_environment:

Environment Variables
=====================

The behavior of EdgeDB can be configured with environment variables. The
variables documented on this page are supported when using the
``edgedb-server`` tool and the official :ref:`Docker image
<ref_guide_deployment_docker>`.


.. _ref_reference_envvar_variants:

Variants
--------
Some environment variables (noted below) support ``*_FILE`` and ``*_ENV``
variants.

- The ``*_FILE`` variant expects its value to be a file name.  The file's
  contents will be read and used as the value.
- The ``*_ENV`` variant expects its value to be the name of another
  environment variable. The value of the other environment variable is then
  used as the final value. This is convenient in deployment scenarios where
  relevant values are auto populated into fixed environment variables.

Supported variables
-------------------

EDGEDB_SERVER_BOOTSTRAP_COMMAND
...............................

Useful to fine-tune initial user and database creation, and other initial
setup.

Maps directly to the ``edgedb-server`` flag ``--default-auth-method``. The
``*_FILE`` and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_DEFAULT_AUTH_METHOD
.................................

Optionally specifies the authentication method used by the server instance.
Supported values are ``SCRAM`` (the default) and ``Trust``. When set to
``Trust``, the database will allow complete unauthenticated access
for all who have access to the database port.

This is often useful when setting an admin password on an instance that lacks
one.

Use at your own risk and only for development and testing.

The ``*_FILE`` and ``*_ENV`` variants are also supported.


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

Maps directly to the ``edgedb-server`` flag ``--tls-cert-mode``. The ``*_FILE``
and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_TLS_CERT_FILE/EDGEDB_SERVER_TLS_KEY_FILE
......................................................

The TLS certificate and private key files, exclusive with
``EDGEDB_SERVER_TLS_CERT_MODE=generate_self_signed``.

Maps directly to the ``edgedb-server`` flags ``--tls-cert-file`` and
``--tls-key-file``.


EDGEDB_SERVER_SECURITY
......................

When set to ``insecure_dev_mode``, sets ``EDGEDB_SERVER_DEFAULT_AUTH_METHOD``
to ``Trust`` (see above), and ``EDGEDB_SERVER_TLS_CERT_MODE`` to
``generate_self_signed`` (unless an explicit TLS certificate is specified).
Finally, if this option is set, the server will accept plaintext HTTP
connections.

Use at your own risk and only for development and testing.

Maps directly to the ``edgedb-server`` flag ``--security``.


EDGEDB_SERVER_PORT
..................

Specifies the network port on which EdgeDB will listen.  The default is
``5656``.

Maps directly to the ``edgedb-server`` flag ``--port``. The ``*_FILE`` and
``*_ENV`` variants are also supported.


EDGEDB_SERVER_BIND_ADDRESS
..........................

Specifies the network interface on which EdgeDB will listen.

Maps directly to the ``edgedb-server`` flag ``--bind-address``. The ``*_FILE``
and ``*_ENV`` variants are also supported.


.. _ref_reference_docer_edgedb_server_datadir:


EDGEDB_SERVER_DATADIR
.....................

Specifies a path where the database files are located.  Defaults to
``/var/lib/edgedb/data``.  Cannot be specified at the same time with
``EDGEDB_SERVER_BACKEND_DSN``.

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

Specifies a path where EdgeDB will place its Unix socket and other transient
files.

Maps directly to the ``edgedb-server`` flag ``--runstate-dir``.


.. _ref_reference_envvar_admin_ui:

EDGEDB_SERVER_ADMIN_UI
......................

Set to ``enabled`` to enable the web-based admininstrative UI for the instance.

Maps directly to the ``edgedb-server`` flag ``--admin-ui``.
