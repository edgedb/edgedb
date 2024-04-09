.. _ref_guide_deployment_docker:

======
Docker
======

:edb-alt-title: Deploying EdgeDB with Docker

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
``EDGEDB_SERVER_DATADIR`` (``/var/lib/edgedb/data`` by default).  For example:

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

Alternatively, if you don't have the EdgeDB CLI installed on your host
machine, you can use the CLI bundled with the server container:

.. code-block:: bash

   $ docker-compose exec edgedb edgedb --tls-security=insecure migration create


.. _ref_guides_deployment_docker_customization:

Configuration
=============

The Docker image supports the same set of enviroment variables as the EdgeDB
server process, which are documented under :ref:`Reference > Environment
Variables <ref_reference_environment>`.

EdgeDB containers can be additionally configured using initialization scripts
and some Docker-specific environment variables, documented below.

.. note::

   Some variables support ``_ENV`` and ``_FILE`` :ref:`variants
   <ref_reference_envvar_variants>` to support more advanced configurations.

.. _ref_guides_deployment_docker_initial_setup:

Initial configuration
---------------------

When an EdgeDB container starts on the specified data directory or remote
Postgres cluster for the first time, initial instance setup is performed. This
is called the *bootstrap phase*.

The following environment variables affect the bootstrap only and have no
effect on subsequent container runs.


EDGEDB_SERVER_BOOTSTRAP_COMMAND
...............................

Useful to fine-tune initial user and branch creation, and other initial
setup. If neither the ``EDGEDB_SERVER_BOOTSTRAP_COMMAND`` variable or the
``EDGEDB_SERVER_BOOTSTRAP_SCRIPT_FILE`` are explicitly specified, the container
will look for the presence of ``/edgedb-bootstrap.edgeql`` in the container
(which can be placed in a derived image).

Maps directly to the ``edgedb-server`` flag ``--bootstrap-command``. The
``*_FILE`` and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_BOOTSTRAP_SCRIPT_FILE
...................................
Deprecated in image version 2.8: use ``EDGEDB_SERVER_BOOTSTRAP_COMMAND_FILE``
instead.

Run the script when initializing the database. The script is run by default
user within default branch.


EDGEDB_SERVER_PASSWORD
......................

The password for the default superuser account will be set to this value. If
no value is provided a password will not be set, unless set via
``EDGEDB_SERVER_BOOTSTRAP_COMMAND``. (If a value for
``EDGEDB_SERVER_BOOTSTRAP_COMMAND`` is provided, this variable will be
ignored.)

The ``*_FILE`` and ``*_ENV`` variants are also supported.


EDGEDB_SERVER_PASSWORD_HASH
...........................

A variant of ``EDGEDB_SERVER_PASSWORD``, where the specified value is a hashed
password verifier instead of plain text.

If ``EDGEDB_SERVER_BOOTSTRAP_COMMAND`` is set, this variable will be ignored.

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

The TLS certificate and private key data, exclusive with
``EDGEDB_SERVER_TLS_CERT_MODE=generate_self_signed``.

The ``*_FILE`` and ``*_ENV`` variants are also supported.


Custom scripts in ``/docker-entrypoint.d/``
...........................................

To perform additional initialization, a derived image may include one or more
executable files in ``/docker-entrypoint.d/``, which will get executed by the
container entrypoint *before* any other processing takes place.


Runtime configuration
---------------------

EDGEDB_DOCKER_LOG_LEVEL
.......................

Determines the log verbosity level in the entrypoint script. Valid levels are
``trace``, ``debug``, ``info``, ``warning``, and ``error``.  The default is
``info``.

.. _ref_guide_deployment_docker_custom_bootstrap_scripts:

Custom scripts in ``/edgedb-bootstrap.d/`` and ``/edgedb-bootstrap-late.d``
...........................................................................

To perform additional initialization, a derived image may include one or more
``*.edgeql`` or ``*.sh`` scripts, which are executed in addition to and
*after* the initialization specified by the environment variables above or the
``/edgedb-bootstrap.edgeql`` script.  Parts in ``/edgedb-bootstrap.d`` are
executed *before* any schema migrations are applied, and parts in
``/edgedb-bootstrap-late.d`` are executed *after* the schema migration have
been applied.

.. note::

    Best practice for naming your script files when you will have multiple
    script files to run on bootstrap is to prepend the filenames with ``01-``,
    ``02-``, and so on to indicate your desired order of execution.

Health Checks
=============

Using an HTTP client, you can perform health checks to monitor the status of
your EdgeDB instance. Learn how to use them with our :ref:`health checks guide
<ref_guide_deployment_health_checks>`.
