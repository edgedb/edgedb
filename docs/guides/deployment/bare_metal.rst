.. _ref_guide_deployment_bare_metal:

==========
Bare Metal
==========

:edb-alt-title: Deploying EdgeDB to a Bare Metal Server

In this guide we show how to deploy EdgeDB to bare metal using your system's
package manager and systemd.


Install the EdgeDB Package
==========================

The steps for installing the EdgeDB package will be slightly different
depending on your Linux distribution. Once you have the package installed you
can jump to :ref:`ref_guide_deployment_bare_metal_enable_unit`.


Debian/Ubuntu LTS
-----------------
Import the EdgeDB packaging key.

.. code-block:: bash

   $ sudo mkdir -p /usr/local/share/keyrings && \
       sudo curl --proto '=https' --tlsv1.2 -sSf \
       -o /usr/local/share/keyrings/edgedb-keyring.gpg \
       https://packages.edgedb.com/keys/edgedb-keyring.gpg

Add the EdgeDB package repository.

.. code-block:: bash

   $ echo deb [signed-by=/usr/local/share/keyrings/edgedb-keyring.gpg] \
       https://packages.edgedb.com/apt \
       $(grep "VERSION_CODENAME=" /etc/os-release | cut -d= -f2) main \
       | sudo tee /etc/apt/sources.list.d/edgedb.list

Install the EdgeDB package.

.. code-block:: bash

   $ sudo apt-get update && sudo apt-get install edgedb-5


CentOS/RHEL 7/8
---------------
Add the EdgeDB package repository.

.. code-block:: bash

   $ sudo curl --proto '=https' --tlsv1.2 -sSfL \
      https://packages.edgedb.com/rpm/edgedb-rhel.repo \
      > /etc/yum.repos.d/edgedb.repo

Install the EdgeDB package.

.. code-block:: bash

   $ sudo yum install edgedb-5


.. _ref_guide_deployment_bare_metal_enable_unit:

Enable a systemd unit
=====================

The EdgeDB package comes bundled with a systemd unit that is disabled by
default. You can start the server by enabling the unit.

.. code-block:: bash

   $ sudo systemctl enable --now edgedb-server-5

This will start the server on port 5656, and the data directory will be
``/var/lib/edgedb/1/data``.

.. warning::

    ``edgedb-server`` cannot be run as root.

Set environment variables
=========================

To set environment variables when running EdgeDB with ``systemctl``,

.. code-block:: bash

   $ systemctl edit --full edgedb-server-5

This opens a ``systemd`` unit file. Set the desired environment variables
under the ``[Service]`` section. View the supported environment variables at
:ref:`Reference > Environment Variables <ref_reference_environment>`.

.. code-block:: toml

   [Service]
   Environment="EDGEDB_SERVER_TLS_CERT_MODE=generate_self_signed"
   Environment="EDGEDB_SERVER_ADMIN_UI=enabled"

Save the file and exit, then restart the service.

.. code-block:: bash

   $ systemctl restart edgedb-server-5


Set a password
==============
There is no default password. To set one, you will first need to get the Unix
socket directory. You can find this by looking at your system.d unit file.

.. code-block:: bash

    $ sudo systemctl cat edgedb-server-5

Set a password by connecting from localhost.

.. code-block:: bash

   $ echo -n "> " && read -s PASSWORD
   $ RUNSTATE_DIR=$(systemctl show edgedb-server-5 -P ExecStart | \
      grep -o -m 1 -- "--runstate-dir=[^ ]\+" | \
      awk -F "=" '{print $2}')
   $ sudo edgedb --port 5656 --tls-security insecure --admin \
      --unix-path $RUNSTATE_DIR \
      query "ALTER ROLE edgedb SET password := '$PASSWORD'"

The server listens on localhost by default. Changing this looks like this.

.. code-block:: bash

   $ edgedb --port 5656 --tls-security insecure --password query \
      "CONFIGURE INSTANCE SET listen_addresses := {'0.0.0.0'};"

The listen port can be changed from the default ``5656`` if your deployment
scenario requires a different value.

.. code-block:: bash

   $ edgedb --port 5656 --tls-security insecure --password query \
      "CONFIGURE INSTANCE SET listen_port := 1234;"

You may need to restart the server after changing the listen port or addresses.

.. code-block:: bash

   $ sudo systemctl restart edgedb-server-5


Link the instance with the CLI
==============================

The following is an example of linking a bare metal instance that is running on
``localhost``. This command assigns a name to the instance, to make it more
convenient to refer to when running CLI commands.

.. code-block:: bash

   $ edgedb instance link \
      --host localhost \
      --port 5656 \
      --user edgedb \
      --branch main \
      --trust-tls-cert \
      bare_metal_instance

This allows connecting to the instance with its name.

.. code-block:: bash

   $ edgedb -I bare_metal_instance


Upgrading EdgeDB
================

.. note::

   The command groups ``edgedb instance`` and ``edgedb project`` are not
   intended to manage production instances.

When you want to upgrade to the newest point release upgrade the package and
restart the ``edgedb-server-5`` unit.


Debian/Ubuntu LTS
-----------------

.. code-block:: bash

   $ sudo apt-get update && sudo apt-get install --only-upgrade edgedb-5
   $ sudo systemctl restart edgedb-server-5


CentOS/RHEL 7/8
---------------

.. code-block:: bash

   $ sudo yum update edgedb-5
   $ sudo systemctl restart edgedb-server-5

Health Checks
=============

Using an HTTP client, you can perform health checks to monitor the status of
your EdgeDB instance. Learn how to use them with our :ref:`health checks guide
<ref_guide_deployment_health_checks>`.
