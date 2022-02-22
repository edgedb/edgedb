.. _ref_guide_deployment_fly_io:

=========
On Fly.io
=========

In this guide we show how to deploy EdgeDB to Fly.io using a Fly.io
PostgreSQL cluster as the backend.


Prerequisites
=============

* Fly.io account
* ``flyctl`` CLI (`install <flyctl-install_>`_)

.. _flyctl-install: https://fly.io/docs/getting-started/installing-flyctl/


Provision a Fly.io app for EdgeDB
=================================

Every Fly.io app must have a globally unique name, including service VMs like
Postgres and EdgeDB.  Here we assume the name for the EdgeDB app is
"myorg-edgedb", which you would need to replace with a name of your choosing.

.. code-block:: bash

    $ EDB_APP=myorg-edgedb
    $ flyctl apps create --name $EDB_APP
    New app created: myorg-edgedb

Now, let's secure the pending EdgeDB instance with a strong password:

.. code-block:: bash

    $ read -s EDGEDB_PASSWORD
    <enter-password>
    $ flyctl secrets set EDGEDB_PASSWORD="$EDGEDB_PASSWORD" -a $EDB_APP
    Secrets are staged for the first deployment

There are a couple more environment variables we need to set:

.. code-block:: bash

    $ flyctl secrets set \
        EDGEDB_SERVER_BACKEND_DSN_ENV=DATABASE_URL \
        EDGEDB_SERVER_TLS_CERT_MODE=generate_self_signed \
        EDGEDB_SERVER_PORT=8080 \
        -a $EDB_APP
    Secrets are staged for the first deployment

The ``EDGEDB_SERVER_BACKEND_DSN_ENV`` tells the EdgeDB container where to
look for the PostgreSQL connection string (more on that below), and the
``EDGEDB_SERVER_TLS_CERT_MODE`` tells EdgeDB to auto-generate a self-signed
TLS certificate.  You may choose to provision a custom TLS certificate instead
and pass it in the ``EDGEDB_SERVER_TLS_CERT`` secret, with the private key in
the ``EDGEDB_SERVER_TLS_KEY`` secret.  Lastly, ``EDGEDB_SERVER_PORT`` makes
EdgeDB listen on port 8080 instead of the default 5656, because Fly.io prefers
``8080`` for its default health checks.

Finally, let's scale the VM as EdgeDB requires a little bit more than the
default Fly.io VM side provides:

.. code-block:: bash

    $ flyctl scale vm shared-cpu-1x --memory=1024 -a $EDB_APP
    Scaled VM Type to
     shared-cpu-1x
          CPU Cores: 1
             Memory: 1 GB


Create a PostgreSQL cluster
===========================

Now we need to provision a PostgreSQL cluster and attach it to the EdgeDB app.

.. note::

  If you have an existing PostgreSQL cluster in your Fly.io organization,
  you can skip to the attachment step.

Create a new PostgreSQL cluster:

.. code-block:: bash

    $ PG_APP=myorg-postgres
    $ flyctl pg create --name $PG_APP --vm-size dedicated-cpu-1x
    ? Select VM size: dedicated-cpu-1x - 256
    ? Volume size (GB): 10
    Creating postgres cluster myorg-postgres in organization personal
    Postgres cluster myorg-postgres created
    ...
    --> v0 deployed successfully

Attach the PostgreSQL cluster to the EdgeDB app:

.. code-block:: bash

    $ PG_ROLE=myorg_edgedb
    $ flyctl pg attach \
        --postgres-app "$PG_APP" \
        --database-user "$PG_ROLE" \
        -a $EDB_APP
    Postgres cluster myorg-postgres is now attached to myorg-edgedb
    The following secret was added to myorg-edgedb:
      DATABASE_URL=postgres://...

When you deploy EdgeDB it will now automatically recognize which PostgreSQL
cluster to run on (via the ``EDGEDB_SERVER_BACKEND_DSN_ENV = "DATABASE_URL"``
bit we added in an earlier step).

Lastly, EdgeDB needs the ability to create Postgres databases and roles,
so let's adjust the permissions on the role that EdgeDB will use to connect
to Postgres:

.. code-block:: bash

    $ echo "alter role \"$PG_ROLE\" createrole createdb; \quit" \
        | flyctl pg connect $PG_APP
    ...
    ALTER ROLE


Start EdgeDB
============

Everything is set, time to start EdgeDB:

.. code-block:: bash

    $ flyctl deploy --image=edgedb/edgedb \
        --remote-only -a $EDB_APP
    ...
    1 desired, 1 placed, 1 healthy, 0 unhealthy
    --> v0 deployed successfully

That's it!  You can now start using the EdgeDB instance located at
edgedb://myorg-edgedb.internal/ in your Fly.io apps.

.. note::

   If deploy did not succeed, make sure you've scaled the EdgeDB VM
   appropriately and check the logs (``flyctl logs myorg-edgedb``).


Persist the generated TLS certificate
=====================================

Now we need to persist the auto-generated TLS certificate to make sure it
survives EdgeDB app restarts.  (If you've provided your own certificate,
skip this step).

.. code-block:: bash

    $ EDB_SECRETS="EDGEDB_SERVER_TLS_KEY EDGEDB_SERVER_TLS_CERT"
    $ flyctl ssh console -a $EDB_APP -C \
        "edgedb-show-secrets.sh --format=toml $EDB_SECRETS" \
      | tr -d '\r' | flyctl secrets import -a $EDB_APP


Create a local link to the new EdgeDB instance
==============================================

To access the EdgeDB instance you've just provisioned on Fly.io from your
local machine first make sure you have the `Private Network VPN <vpn_>`_ up and
running and then run ``edgedb instance link``:

.. code-block:: bash

   $ echo $EDGEDB_PASSWORD | edgedb instance link \
        --trust-tls-cert \
        --host $EDB_APP.internal \
        --port 8080 \
        --password-from-stdin \
        --non-interactive \
        fly
   Authenticating to edgedb://edgedb@myorg-edgedb.internal:5656/edgedb
   Successfully linked to remote instance. To connect run:
     edgedb -I fly

Don't forget to replace ``myorg-edgedb`` above with the name of your EdgeDB
app.  You can now use the EdgeDB instance deployed on Fly.io as ``fly``,
for example:

.. code-block:: bash

   $ edgedb -I fly
   edgedb>

.. _vpn: https://fly.io/docs/reference/private-networking/#private-network-vpn
