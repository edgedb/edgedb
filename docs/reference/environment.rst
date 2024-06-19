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

Docker image variables
----------------------

These variables are only used by the Docker image. Setting these variables
outside that context will have no effect.


EDGEDB_DOCKER_ABORT_CODE
........................

If the process fails, the arguments are logged to stderr and the script is
terminated with this exit code. Default is ``1``.


EDGEDB_DOCKER_APPLY_MIGRATIONS
..............................

The container will attempt to apply migrations in ``dbschema/migrations``
unless this variable is set to ``never``. Default is ``always``.


EDGEDB_DOCKER_BOOTSTRAP_TIMEOUT_SEC
...................................

Sets the number of seconds to wait for instance bootstrapping to complete
before timing out. Default is ``300``.


EDGEDB_DOCKER_LOG_LEVEL
.......................

Change the logging level for the docker container. Default is ``info``. Other
levels are ``trace``, ``debug``, ``warning``, and ``error``.


EDGEDB_DOCKER_SHOW_GENERATED_CERT
.................................

Shows the generated TLS certificate in console output. Default is ``always``.
May instead be set to ``never``.


EDGEDB_DOCKER_SKIP_MIGRATIONS
.............................

.. warning:: Deprecated

    Use ``EDGEDB_DOCKER_APPLY_MIGRATIONS`` instead.

The container will skip applying migrations in ``dbschema/migrations``
if this is set.


EDGEDB_SERVER_BINARY
....................

Sets the EdgeDB server binary to run. Default is ``edgedb-server``.


EDGEDB_SERVER_BOOTSTRAP_COMMAND_FILE
....................................

Run the script when initializing the database. The script is run by the default
user within the default :versionreplace:`database;5.0:branch`. May be used with
or without ``EDGEDB_SERVER_BOOTSTRAP_ONLY``.


EDGEDB_SERVER_BOOTSTRAP_SCRIPT_FILE
...................................

.. warning:: Deprecated in image version 2.8

    Use ``EDGEDB_SERVER_BOOTSTRAP_COMMAND_FILE`` instead.

Run the script when initializing the database. The script is run by the default
user within the default :versionreplace:`database;5.0:branch`.


EDGEDB_SERVER_COMPILER_POOL_MODE
................................

Choose a mode for the compiler pool to scale. ``fixed`` means the pool will not
scale and sticks to ``EDGEDB_SERVER_COMPILER_POOL_SIZE``, while ``on_demand``
means the pool will maintain at least 1 worker and automatically scale up (to
``EDGEDB_SERVER_COMPILER_POOL_SIZE`` workers ) and down to the demand.

Default is ``fixed`` in production mode and ``on_demand`` in development mode.


EDGEDB_SERVER_COMPILER_POOL_SIZE
................................

When ``EDGEDB_SERVER_COMPILER_POOL_MODE`` is ``fixed``, this setting is the
exact size of the compiler pool. When ``EDGEDB_SERVER_COMPILER_POOL_MODE`` is
``on_demand``, this will serve as the maximum size of the compiler pool.


EDGEDB_SERVER_EMIT_SERVER_STATUS
................................

Instruct the server to emit changes in status to *DEST*, where *DEST* is a URI
specifying a file (``file://<path>``), or a file descriptor
(``fd://<fileno>``).  If the URI scheme is not specified, ``file://`` is
assumed.


EDGEDB_SERVER_EXTRA_ARGS
........................

Additional arguments to pass when starting the EdgeDB server.


EDGEDB_SERVER_GENERATE_SELF_SIGNED_CERT
.......................................

.. warning:: Deprecated

    Use ``EDGEDB_SERVER_TLS_CERT_MODE="generate_self_signed"`` instead.

Instructs the server to generate a self-signed certificate when set.


EDGEDB_SERVER_PASSWORD
......................

The password for the default superuser account (or the user specified in
``EDGEDB_SERVER_USER``) will be set to this value. If no value is provided, a
password will not be set, unless set via ``EDGEDB_SERVER_BOOTSTRAP_COMMAND``.
(If a value for ``EDGEDB_SERVER_BOOTSTRAP_COMMAND`` is provided, this variable
will be ignored.)

The ``*_FILE`` and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_PASSWORD_HASH
...........................

A variant of ``EDGEDB_SERVER_PASSWORD``, where the specified value is a hashed
password verifier instead of plain text.

If ``EDGEDB_SERVER_BOOTSTRAP_COMMAND`` is set, this variable will be ignored.

The ``*_FILE`` and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_SKIP_MIGRATIONS
.............................

.. warning:: Deprecated

    Use ``EDGEDB_DOCKER_APPLY_MIGRATIONS="never"`` instead.

When set, skips applying migrations in ``dbschema/migrations``. Not set by
default.


EDGEDB_SERVER_TENANT_ID
.......................

Specifies the tenant ID of this server when hosting multiple EdgeDB instances
on one Postgres cluster. Must be an alphanumeric ASCII string, maximum 10
characters long.


EDGEDB_SERVER_UID
.................

Specifies the ID of the user which should run the server binary. Default is
``1``.


EDGEDB_SERVER_USER
..................

If set to anything other than the default username (``edgedb``), the username
specified will be created. The user defined here will be the one assigned the
password set in ``EDGEDB_SERVER_PASSWORD`` or the hash set in
``EDGEDB_SERVER_PASSWORD_HASH``.


Server variables
----------------

These variables will work whether you are running EdgeDB inside Docker or not.


EDGEDB_DEBUG_HTTP_INJECT_CORS
.............................

Set to ``1`` to have EdgeDB send appropriate CORS headers with HTTP responses.

.. note::

    This is set to ``1`` by default for EdgeDB Cloud instances.


.. _ref_reference_envvar_admin_ui:

EDGEDB_SERVER_ADMIN_UI
......................

Set to ``enabled`` to enable the web-based admininstrative UI for the instance.

Maps directly to the ``edgedb-server`` flag ``--admin-ui``.


EDGEDB_SERVER_ALLOW_INSECURE_BINARY_CLIENTS
...........................................

.. warning:: Deprecated

    Use ``EDGEDB_SERVER_BINARY_ENDPOINT_SECURITY`` instead.

Specifies the security mode of the server's binary endpoint. When set to ``1``,
non-TLS connections are allowed. Not set by default.

.. warning::

    Disabling TLS is not recommended in production.


EDGEDB_SERVER_ALLOW_INSECURE_HTTP_CLIENTS
.........................................

.. warning:: Deprecated

    Use ``EDGEDB_SERVER_HTTP_ENDPOINT_SECURITY`` instead.

Specifies the security mode of the server's HTTP endpoint. When set to ``1``,
non-TLS connections are allowed. Not set by default.

.. warning::

    Disabling TLS is not recommended in production.


.. _ref_reference_docker_edgedb_server_backend_dsn:

EDGEDB_SERVER_BACKEND_DSN
.........................

Specifies a PostgreSQL connection string in the `URI format`_.  If set, the
PostgreSQL cluster specified by the URI is used instead of the builtin
PostgreSQL server.  Cannot be specified alongside ``EDGEDB_SERVER_DATADIR``.

Maps directly to the ``edgedb-server`` flag ``--backend-dsn``. The ``*_FILE``
and ``*_ENV`` variants are also supported.

.. _URI format:
   https://www.postgresql.org/docs/13/libpq-connect.html#id-1.7.3.8.3.6

EDGEDB_SERVER_MAX_BACKEND_CONNECTIONS
.....................................

The maximum NUM of connections this EdgeDB instance could make to the backend
PostgreSQL cluster. If not set, EdgeDB will detect and calculate the NUM:
RAM/100MiB for local Postgres, or pg_settings.max_connections for remote
Postgres minus the NUM of ``--reserved-pg-connections``.

EDGEDB_SERVER_BINARY_ENDPOINT_SECURITY
......................................

Specifies the security mode of the server's binary endpoint. When set to
``optional``, non-TLS connections are allowed. Default is ``tls``.

.. warning::

    Disabling TLS is not recommended in production.


EDGEDB_SERVER_BIND_ADDRESS
..........................

Specifies the network interface on which EdgeDB will listen.

Maps directly to the ``edgedb-server`` flag ``--bind-address``. The ``*_FILE``
and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_BOOTSTRAP_COMMAND
...............................

Useful to fine-tune initial user creation and other initial setup.

.. versionchanged:: _default

    .. note::

        A :eql:stmt:`create database` statement cannot be combined in a block with
        any other statements. Since all statements in
        ``EDGEDB_SERVER_BOOTSTRAP_COMMAND`` run in a single block, it cannot be
        used to create a database and, for example, create a user for that
        database.

        For Docker deployments, you can instead write :ref:`custom scripts to run
        before migrations <ref_guide_deployment_docker_custom_bootstrap_scripts>`.
        These are placed in ``/edgedb-bootstrap.d/``. By writing your ``create
        database`` statements in one ``.edgeql`` file each placed in
        ``/edgedb-bootstrap.d/`` and other statements in their own file, you can
        create databases and still run other EdgeQL statements to bootstrap your
        instance.

.. versionchanged:: 5.0

    .. note::

        A create branch statement (i.e., :eql:stmt:`create empty branch`,
        :eql:stmt:`create schema branch`, or :eql:stmt:`create data branch`)
        cannot be combined in a block with any other statements. Since all
        statements in ``EDGEDB_SERVER_BOOTSTRAP_COMMAND`` run in a single
        block, it cannot be used to create a branch and, for example, create a
        user on that branch.

        For Docker deployments, you can instead write :ref:`custom scripts to run
        before migrations <ref_guide_deployment_docker_custom_bootstrap_scripts>`.
        These are placed in ``/edgedb-bootstrap.d/``. By writing your ``create
        branch`` statements in one ``.edgeql`` file each placed in
        ``/edgedb-bootstrap.d/`` and other statements in their own file, you can
        create branches and still run other EdgeQL statements to bootstrap your
        instance.

Maps directly to the ``edgedb-server`` flag ``--bootstrap-command``. The
``*_FILE`` and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_BOOTSTRAP_ONLY
............................

When set, bootstrap the database cluster and exit. Not set by default.


.. _ref_reference_docer_edgedb_server_datadir:

EDGEDB_SERVER_DATADIR
.....................

Specifies a path where the database files are located.  Default is
``/var/lib/edgedb/data``.  Cannot be specified alongside
``EDGEDB_SERVER_BACKEND_DSN``.

Maps directly to the ``edgedb-server`` flag ``--data-dir``.


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


EDGEDB_SERVER_HTTP_ENDPOINT_SECURITY
....................................

Specifies the security mode of the server's HTTP endpoint. When set to
``optional``, non-TLS connections are allowed. Default is ``tls``.

.. warning::

    Disabling TLS is not recommended in production.


EDGEDB_SERVER_INSTANCE_NAME
...........................

Specify the server instance name.


EDGEDB_SERVER_JWS_KEY_FILE
..........................

Specifies a path to a file containing a public key in PEM format used to verify
JWT signatures. The file could also contain a private key to sign JWT for local
testing.


EDGEDB_SERVER_LOG_LEVEL
.......................

Set the logging level. Default is ``info``. Other possible values are
``debug``, ``warn``, ``error``, and ``silent``.


EDGEDB_SERVER_PORT
..................

Specifies the network port on which EdgeDB will listen. Default is ``5656``.

Maps directly to the ``edgedb-server`` flag ``--port``. The ``*_FILE`` and
``*_ENV`` variants are also supported.


EDGEDB_SERVER_POSTGRES_DSN
..........................

.. warning:: Deprecated

    Use ``EDGEDB_SERVER_BACKEND_DSN`` instead.

Specifies a PostgreSQL connection string in the `URI format`_.  If set, the
PostgreSQL cluster specified by the URI is used instead of the builtin
PostgreSQL server.  Cannot be specified alongside ``EDGEDB_SERVER_DATADIR``.

Maps directly to the ``edgedb-server`` flag ``--backend-dsn``. The ``*_FILE``
and ``*_ENV`` variants are also supported.

.. _URI format:
   https://www.postgresql.org/docs/13/libpq-connect.html#id-1.7.3.8.3.6


EDGEDB_SERVER_RUNSTATE_DIR
..........................

Specifies a path where EdgeDB will place its Unix socket and other transient
files.

Maps directly to the ``edgedb-server`` flag ``--runstate-dir``.


EDGEDB_SERVER_SECURITY
......................

When set to ``insecure_dev_mode``, sets ``EDGEDB_SERVER_DEFAULT_AUTH_METHOD``
to ``Trust``, and ``EDGEDB_SERVER_TLS_CERT_MODE`` to ``generate_self_signed``
(unless an explicit TLS certificate is specified). Finally, if this option is
set, the server will accept plaintext HTTP connections.

.. warning::

    Disabling TLS is not recommended in production.

Maps directly to the ``edgedb-server`` flag ``--security``.


EDGEDB_SERVER_TLS_CERT_FILE/EDGEDB_SERVER_TLS_KEY_FILE
......................................................

The TLS certificate and private key files, exclusive with
``EDGEDB_SERVER_TLS_CERT_MODE=generate_self_signed``.

Maps directly to the ``edgedb-server`` flags ``--tls-cert-file`` and
``--tls-key-file``.


EDGEDB_SERVER_TLS_CERT_MODE
...........................

Specifies what to do when the TLS certificate and key are either not specified
or are missing.

- When set to ``require_file``, the TLS certificate and key must be specified
  in the ``EDGEDB_SERVER_TLS_CERT`` and ``EDGEDB_SERVER_TLS_KEY`` variables and
  both must exist.
- When set to ``generate_self_signed`` a new self-signed certificate and
  private key will be generated and placed in the path specified by
  ``EDGEDB_SERVER_TLS_CERT`` and ``EDGEDB_SERVER_TLS_KEY``, if those are set.
  Otherwise, the generated certificate and key are stored as ``edbtlscert.pem``
  and ``edbprivkey.pem`` in ``EDGEDB_SERVER_DATADIR``, or, if
  ``EDGEDB_SERVER_DATADIR`` is not set, they will be placed in
  ``/etc/ssl/edgedb``.

Default is ``generate_self_signed`` when
``EDGEDB_SERVER_SECURITY=insecure_dev_mode``. Otherwise, the default is
``require_file``.

Maps directly to the ``edgedb-server`` flag ``--tls-cert-mode``. The ``*_FILE``
and ``*_ENV`` variants are also supported.
