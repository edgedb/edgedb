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

   $ sudo apt-get update && sudo apt-get install edgedb-1


CentOS/RHEL 7/8
---------------
Add the EdgeDB package repository.

.. code-block:: bash

   $ sudo curl --proto '=https' --tlsv1.2 -sSfL \
      https://packages.edgedb.com/rpm/edgedb-rhel.repo \
      > /etc/yum.repos.d/edgedb.repo

Install the EdgeDB package.

.. code-block:: bash

   $ sudo yum install edgedb-1


.. _ref_guide_deployment_bare_metal_enable_unit:

Enable a systemd unit
=====================

The EdgeDB package comes bundled with a systemd unit that is disabled by
default. You can start the server by enabling the unit.

.. code-block:: bash

   $ sudo systemctl enable --now edgedb-server-1

This will start the server on port 5656, and the data directory will be
``/var/lib/edgedb/1/data``. You can edit the unit to specify server arguments
via the environment. The variables are largely the same as :ref:`those
documented for Docker <ref_guides_deployment_docker_customization>`.


Set a Password
==============
There is no default password. Set a password by connecting from localhost.

.. code-block:: bash

   $ echo -n "> " && read -s PASSWORD
   $ sudo edgedb --port 5656 --tls-security insecure --admin query \
      "ALTER ROLE edgedb SET password := '$PASSWORD'"

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

   $ sudo systemctl restart edgedb-server-1


Upgrading EdgeDB
================

When you want to upgrade to the newest point release upgrade the package and
restart the ``edgedb-server-1`` unit.


Debian/Ubuntu LTS
-----------------

.. code-block:: bash

   $ sudo apt-get update && sudo apt-get install --only-upgrade edgedb-1
   $ sudo systemctl restart edgedb-server-1


CentOS/RHEL 7/8
---------------

.. code-block:: bash

   $ sudo yum update edgedb-1
   $ sudo systemctl restart edgedb-server-1
