.. _ref_guide_deployment_docker:

======
Docker
======

When to use the `edgedb/edgedb`_ Docker image
=============================================

.. _edgedb/edgedb: https://hub.docker.com/r/edgedb/edgedb

This image is primarily intended to be used directly when there is a
requirement to use Docker containers, such as in production, or in a
development setup that involves multiple containers orchestrated by Docker
Compose or a similar tool. Otherwise, using the :ref:`ref_cli_edgedb_server`
CLI on the host system is the recommended way to install and run EdgeDB
servers.


How to use this image
=====================

The simplest way to run the image (without data persistence) is this:

.. code-block:: bash

   $ docker run --name edgedb -d \
       -e EDGEDB_SERVER_SECURITY=insecure_dev_mode \
       edgedb/edgedb

See the :ref:`ref_guides_deployment_docker_customization` section below for the
meaning of the ``EDGEDB_SERVER_SECURITY`` variable and other options.

Then, to authenticate to the EdgeDB instance and store the credentials in a
Docker volume, run:

.. code-block:: bash

   $ docker run -it --rm --link=edgedb \
       -e EDGEDB_SERVER_PASSWORD=secret \
       -v edgedb-cli-config:/.config/edgedb edgedb/edgedb-cli \
       -H edgedb instance link my_instance

Now, to open an interactive shell to the database instance run this:

.. code-block:: bash

   $ docker run -it --rm --link=edgedb \
       -v edgedb-cli-config:/.config/edgedb edgedb/edgedb-cli \
       -I my_instance


Data Persistence
================

If you want the contents of the database to survive container restarts, you
must mount a persistent volume at the path specified by
``EDGEDB_SERVER_DATADIR`` (``/var/lib/edgedb/data``) by default.  For example:

.. code-block:: bash

   $ docker run \
       --name edgedb \
       -e EDGEDB_SERVER_PASSWORD=secret \
       -e EDGEDB_SERVER_TLS_CERT_MODE=generate_self_signed \
       -v /my/data/directory:/var/lib/edgedb/data \
       -d edgedb/edgedb

Note that on Windows you must use a Docker volume instead:

.. code-block:: bash

   $ docker volume create --name=edgedb-data
   $ docker run \
       --name edgedb \
       -e EDGEDB_SERVER_PASSWORD=secret \
       -e EDGEDB_SERVER_TLS_CERT_MODE=generate_self_signed \
       -v edgedb-data:/var/lib/edgedb/data \
       -d edgedb/edgedb

It is also possible to run an ``edgedb`` container on a remote PostgreSQL
cluster specified by ``EDGEDB_SERVER_BACKEND_DSN``. See below for details.


Schema Migrations
=================

A derived image may include application schema and migrations in ``/dbschema``,
in which case the container will attempt to apply the schema migrations found
in ``/dbschema/migrations``, unless the ``EDGEDB_DOCKER_APPLY_MIGRATIONS``
environment variable is set to ``never``.


Docker Compose
==============

A simple ``docker-compose`` configuration might look like this.
With a ``docker-compose.yaml`` containing:

.. code-block:: yaml

   version: "3"
   services:
     edgedb:
       image: edgedb/edgedb
       environment:
         EDGEDB_SERVER_SECURITY: insecure_dev_mode
       volumes:
         - "./dbschema:/dbschema"
       ports:
         - "5656:5656"

Once there is a :ref:`schema <ref_datamodel_index>` in ``dbschema/`` a
migration can be created with:

.. code-block:: bash

   $ edgedb --tls-security=insecure -P 5656 migration create

alternatively, if you don't have the EdgeDB CLI installed on your host
machine, you can use the CLI bundled with the server container:

.. code-block:: bash

   $ docker-compose exec edgedb edgedb --tls-security=insecure migration create


.. _ref_guides_deployment_docker_customization:

Customization
=============

The behavior of the EdgeDB docker image can be customized via environment
variables and initialization scripts.

Some environment variables (noted below) support ``*_FILE`` and ``*_ENV``
variants. The ``*_FILE`` variant expects its value to be a file name.  The
file's contents will be read and used as the value. This is useful for
referencing files that are mounted in the container. The ``*_ENV`` variant
expects its value to be the name of another environment variable. The value of
the other environment variable is then used as the final value. This is
convenient in deployment scenarios where relevant values are auto populated
into fixed environment variables.


.. _ref_guides_deployment_docker_initial_setup:

Initial container setup
-----------------------

When an EdgeDB container starts on the specified data directory or remote
Postgres cluster for the first time, initial instance setup is performed. This
is called the *bootstrap phase*.

The following environment variables affect the bootstrap only and have no
effect on subsequent container runs.


EDGEDB_SERVER_PASSWORD
......................

Determines the password used for the default superuser account.

The ``*_FILE`` and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_PASSWORD_HASH
...........................

A variant of ``EDGEDB_SERVER_PASSWORD``, where the specified value is a hashed
password verifier instead of plain text.

The ``*_FILE`` and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_USER
..................

Optionally specifies the name of the default superuser account. Defaults to
``edgedb`` if not specified.

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

The ``*_FILE`` and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_GENERATE_SELF_SIGNED_CERT
.......................................

Deprecated: use ``EDGEDB_SERVER_TLS_CERT_MODE=generate_self_signed`` instead.

Set this option to ``1`` to tell the server to automatically generate a
self-signed certificate with key file in the ``EDGEDB_SERVER_DATADIR`` (if
present, see below), and echo the certificate content in the logs. If the
certificate file exists, the server will use it instead of generating a new
one.

Self-signed certificates are usually used in development and testing, you
should likely provide your own certificate and key file with the variables
below.


EDGEDB_SERVER_TLS_CERT EDGEDB_SERVER_TLS_KEY
............................................

The TLS certificate and private key, exclusive with
``EDGEDB_SERVER_TLS_CERT_MODE=generate_self_signed``.

The ``*_FILE`` and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_DATABASE
......................

Optionally specifies the name of a default database that is created during
bootstrap. Defaults to ``edgedb`` if not specified.

The ``*_FILE`` and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_DEFAULT_AUTH_METHOD
.................................

Optionally specifies the authentication method used by the server instance.
Supported values are ``SCRAM`` (the default) and ``Trust``.  When set to
``Trust``, the database will allow complete unauthenticated access for all who
have access to the database port.  In this case the ``EDGEDB_SERVER_PASSWORD``
(or equivalent) setting is not required.

Use at your own risk and only for development and testing.


EDGEDB_SERVER_SECURITY
......................

When set to ``insecure_dev_mode``, sets ``EDGEDB_SERVER_DEFAULT_AUTH_METHOD``
to ``Trust`` (see above), and ``EDGEDB_SERVER_TLS_CERT_MODE`` to
``generate_self_signed`` (unless an explicit TLS certificate is specified).
Finally, if this option is set, the server will accept plaintext HTTP
connections.

Use at your own risk and only for development and testing.


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


Custom scripts in ``/edgedb-bootstrap.d/`` and ``/edgedb-bootstrap-late.d``
...........................................................................

To perform additional initialization, a derived image may include one ore more
``*.edgeql``, or ``*.sh`` scripts, which are executed in addition to and
_after_ the initialization specified by the environment variables above or the
``/edgedb-bootstrap.edgeql`` script.  Parts in ``/edgedb-bootstrap.d`` are
executed _before_ any schema migrations are applied, and parts in
``/edgedb-bootstrap-late.d`` are executed _after_ the schema migration have
been applied.


Runtime Options
---------------

Unlike options listed in the :ref:`ref_guides_deployment_docker_initial_setup`
section above, the configuration documented below applies to all container
invocations.  It can be specified either as environment variables or
command-line arguments.


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


EDGEDB_SERVER_EXTRA_ARGS
........................

Extra arguments to be passed to EdgeDB server.

Maps directly to the ``edgedb-server`` flag ``--extra-arg, ...``.


Custom scripts in ``/docker-entrypoint.d/``
...........................................

To perform additional initialization, a derived image may include one ore more
executable files in ``/docker-entrypoint.d/``, which will get executed by the
container entrypoint *before* any other processing takes place.


EDGEDB_DOCKER_LOG_LEVEL
.......................

Determines the log verbosity level in the entrypoint script. Valid levels are
``trace``, ``debug``, ``info``, ``warning``, and ``error``.  The default is
``info``.
