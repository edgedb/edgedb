.. _ref_guide_deployment_fly_io:

======
Fly.io
======

:edb-alt-title: Deploying EdgeDB to Fly.io

In this guide we show how to deploy EdgeDB using a `Fly.io <https://fly.io>`_
PostgreSQL cluster as the backend. The deployment consists of two apps: one
running Postgres and the other running EdgeDB.


Prerequisites
=============

* Fly.io account
* ``flyctl`` CLI (`install <flyctl-install_>`_)

.. _flyctl-install: https://fly.io/docs/getting-started/installing-flyctl/


Provision a Fly.io app for EdgeDB
=================================

Every Fly.io app must have a globally unique name, including service VMs like
Postgres and EdgeDB. Pick a name and assign it to a local environment variable
called ``EDB_APP``. In the command below, replace ``myorg-edgedb`` with a name
of your choosing.

.. code-block:: bash

    $ EDB_APP=myorg-edgedb
    $ flyctl apps create --name $EDB_APP
    New app created: myorg-edgedb


Now let's use the ``read`` command to securely assign a value to the
``PASSWORD`` environment variable.

.. code-block:: bash

   $ echo -n "> " && read -s PASSWORD


Now let's assign this password to a Fly `secret
<https://fly.io/docs/reference/secrets/>`_, plus a few other secrets that
we'll need. There are a couple more environment variables we need to set:

.. code-block:: bash

    $ flyctl secrets set \
        EDGEDB_SERVER_PASSWORD="$PASSWORD" \
        EDGEDB_SERVER_BACKEND_DSN_ENV=DATABASE_URL \
        EDGEDB_SERVER_TLS_CERT_MODE=generate_self_signed \
        EDGEDB_SERVER_PORT=8080 \
        --app $EDB_APP
    Secrets are staged for the first deployment

Let's discuss what's going on with all these secrets.

- The ``EDGEDB_SERVER_BACKEND_DSN_ENV`` tells the EdgeDB container where to
  look for the PostgreSQL connection string (more on that below)
- The ``EDGEDB_SERVER_TLS_CERT_MODE`` tells EdgeDB to auto-generate a
  self-signed TLS certificate.

  You may instead choose to provision a custom TLS certificate. In this
  case, you should instead create two other secrets: assign your certificate
  to ``EDGEDB_SERVER_TLS_CERT`` and your private key to
  ``EDGEDB_SERVER_TLS_KEY``.
- Lastly, ``EDGEDB_SERVER_PORT`` tells EdgeDB to listen on port 8080 instead
  of the default 5656, because Fly.io prefers ``8080`` for its default health
  checks.

Finally, let's configure the VM size as EdgeDB requires a little bit more than
the default Fly.io VM side provides. Put this in a file called ``fly.toml`` in
your current directory.:

.. code-block:: yaml

    [build]
      image = "edgedb/edgedb"

    [[vm]]
      memory = "512mb"
      cpus = 1
      cpu-kind = "shared"


Create a PostgreSQL cluster
===========================

Now we need to provision a PostgreSQL cluster and attach it to the EdgeDB app.

.. note::

  If you have an existing PostgreSQL cluster in your Fly.io organization,
  you can skip to the attachment step.

Then create a new PostgreSQL cluster. This may take a few minutes to complete.

.. code-block:: bash

    $ PG_APP=myorg-postgres
    $ flyctl pg create --name $PG_APP --vm-size shared-cpu-1x
    ? Select region: sea (Seattle, Washington (US))
    ? Specify the initial cluster size: 1
    ? Volume size (GB): 10
    Creating postgres cluster myorg-postgres in organization personal
    Postgres cluster myorg-postgres created
        Username:    postgres
        Password:    <random password>
        Hostname:    myorg-postgres.internal
        Proxy Port:  5432
        PG Port: 5433
    Save your credentials in a secure place, you won't be able to see them
    again!
    Monitoring Deployment
    ...
    --> v0 deployed successfully

In the output, you'll notice a line that says ``Machine <machine-id> is
created``. The ID in that line is the ID of the virtual machine created for
your Postgres cluster. We now need to use that ID to scale the cluster since
the ``shared-cpu-1x`` VM doesn't have enough memory by default. Scale it with
this command:

.. code-block:: bash

    $ flyctl machine update <machine-id> --memory 1024 --app $PG_APP -y
    Searching for image 'flyio/postgres:14.6' remotely...
    image found: img_0lq747j0ym646x35
    Image: registry-1.docker.io/flyio/postgres:14.6
    Image size: 361 MB

    Updating machine <machine-id>
      Waiting for <machine-id> to become healthy (started, 3/3)
    Machine <machine-id> updated successfully!
    ==> Monitoring health checks
      Waiting for <machine-id> to become healthy (started, 3/3)
    ...

With the VM scaled sufficiently, we can now attach the PostgreSQL cluster to
the EdgeDB app:

.. code-block:: bash

    $ PG_ROLE=myorg_edgedb
    $ flyctl pg attach "$PG_APP" \
        --database-user "$PG_ROLE" \
        --app $EDB_APP
    Postgres cluster myorg-postgres is now attached to myorg-edgedb
    The following secret was added to myorg-edgedb:
      DATABASE_URL=postgres://...

Lastly, EdgeDB needs the ability to create Postgres databases and roles,
so let's adjust the permissions on the role that EdgeDB will use to connect
to Postgres:

.. code-block:: bash

    $ echo "alter role \"$PG_ROLE\" createrole createdb; \quit" \
        | flyctl pg connect --app $PG_APP
    ...
    ALTER ROLE

.. _ref_guide_deployment_fly_io_start_edgedb:

Start EdgeDB
============

Everything is set! Time to start EdgeDB.

.. code-block:: bash

    $ flyctl deploy --remote-only --app $EDB_APP
    ...
    Finished launching new machines
    -------
     ✔ Machine e286630dce9638 [app] was created
    -------

That's it!  You can now start using the EdgeDB instance located at
``edgedb://myorg-edgedb.internal`` in your Fly.io apps.


If deploy did not succeed:

1. make sure you've created the ``fly.toml`` file.
2. re-run the ``deploy`` command
3. check the logs for more information: ``flyctl logs --app $EDB_APP``

Persist the generated TLS certificate
=====================================

Now we need to persist the auto-generated TLS certificate to make sure it
survives EdgeDB app restarts. (If you've provided your own certificate,
skip this step).

.. code-block:: bash

    $ EDB_SECRETS="EDGEDB_SERVER_TLS_KEY EDGEDB_SERVER_TLS_CERT"
    $ flyctl ssh console --app $EDB_APP -C \
        "edgedb-show-secrets.sh --format=toml $EDB_SECRETS" \
      | tr -d '\r' | flyctl secrets import --app $EDB_APP


Connecting to the instance
==========================

Let's construct the DSN (AKA "connection string") for our instance. DSNs have
the following format: ``edgedb://<username>:<password>@<hostname>:<port>``. We
can construct the DSN with the following components:

- ``<username>``: the default value — ``edgedb``
- ``<password>``: the value we assigned to ``$PASSWORD``
- ``<hostname>``: the name of your EdgeDB app (stored in the
  ``$EDB_APP`` environment variable) suffixed with ``.internal``. Fly uses this
  synthetic TLD to simplify inter-app communication. Ex:
  ``myorg-edgedb.internal``.
- ``<port>``: ``8080``, which we configured earlier

We can construct this value and assign it to a new environment variable called
``DSN``.

.. code-block:: bash

    $ DSN=edgedb://edgedb:$PASSWORD@$EDB_APP.internal:8080

Consider writing it to a file to ensure the DSN looks correct. Remember to
delete the file after you're done. (Printing this value to the terminal with
``echo`` is insecure and can leak your password into shell logs.)

.. code-block:: bash

    $ echo $DSN > dsn.txt
    $ open dsn.txt
    $ rm dsn.txt

From a Fly.io app
-----------------

To connect to this instance from another Fly app (say, an app that runs your
API server) set the value of the ``EDGEDB_DSN`` secret inside that app.

.. code-block:: bash

    $ flyctl secrets set \
        EDGEDB_DSN=$DSN \
        --app my-other-fly-app

We'll also set another variable that will disable EdgeDB's TLS checks.
Inter-application communication is secured by Fly so TLS isn't vital in
this case; configuring TLS certificates is also beyond the scope of this guide.

.. code-block:: bash

    $ flyctl secrets set EDGEDB_CLIENT_TLS_SECURITY=insecure \
        --app my-other-fly-app


You can also set these values as environment variables inside your
``fly.toml`` file, but using Fly's built-in `secrets
<https://fly.io/docs/reference/secrets/>`_ functionality is recommended.

From external application
-------------------------

If you need to access EdgeDB from outside the Fly.io network, you'll need to
configure the Fly.io proxy to let external connections in.

Let's make sure the ``[[services]]`` section in our ``fly.toml`` looks
something like this:

.. code-block:: toml

    [[services]]
        http_checks = []
        internal_port = 8080
        processes = ["app"]
        protocol = "tcp"
        script_checks = []
        [services.concurrency]
            hard_limit = 25
            soft_limit = 20
            type = "connections"

        [[services.ports]]
            port = 5656

        [[services.tcp_checks]]
            grace_period = "1s"
            interval = "15s"
            restart_limit = 0
            timeout = "2s"

In the same directory, :ref:`redeploy the EdgeDB app
<ref_guide_deployment_fly_io_start_edgedb>`. This makes the EdgeDB port
available to the outside world. You can now access the instance from any host
via the following public DSN: ``edgedb://edgedb:$PASSWORD@$EDB_APP.fly.dev``.

To secure communication between the server and the client, you will also
need to set the ``EDGEDB_TLS_CA`` environment secret in your application.
You can securely obtain the certificate content by running:

.. code-block:: bash

    $ flyctl ssh console -a $EDB_APP \
        -C "edgedb-show-secrets.sh --format=raw EDGEDB_SERVER_TLS_CERT"

From your local machine
-----------------------

To access the EdgeDB instance from local development machine/laptop, install
the Wireguard `VPN <vpn_>`_ and create a tunnel, as described on Fly's
`Private Networking
<https://fly.io/docs/reference/private-networking/#private-network-vpn>`_
docs.

Once it's up and running, use ``edgedb instance link`` to create a local
alias to the remote instance.

.. code-block:: bash

    $ edgedb instance link \
        --trust-tls-cert \
        --dsn $DSN \
        --non-interactive \
        fly
    Authenticating to edgedb://edgedb@myorg-edgedb.internal:5656/edgedb
    Successfully linked to remote instance. To connect run:
      edgedb -I fly

You can now run CLI commands against this instance by specifying it by name
with ``-I fly``; for example, to apply migrations:

.. note::

   The command groups ``edgedb instance`` and ``edgedb project`` are not
   intended to manage production instances.

.. code-block:: bash

   $ edgedb -I fly migrate

.. _vpn: https://fly.io/docs/reference/private-networking/#private-network-vpn

Health Checks
=============

Using an HTTP client, you can perform health checks to monitor the status of
your EdgeDB instance. Learn how to use them with our :ref:`health checks guide
<ref_guide_deployment_health_checks>`.
