.. _ref_admin_install:

============
Installation
============

This section describes the installation of EdgeDB using a packaged
distribution.


Binary Packages
===============

EdgeDB packages are available for common Linux distributions and macOS in
the official `package repository`_.

.. _`package repository`:
        https://www.edgedb.com/download/


---------------------------------
Red Hat Enterprise Linux / CentOS
---------------------------------

Packages are available for RHEL/CentOS 7 and later.

Step 1. Add the EdgeDB package repository:

.. code-block:: bash

    sudo tee <<'EOF' /etc/yum.repos.d/edgedb.repo
    [edgedb]
    name=edgedb
    baseurl=https://packages.edgedb.com/rpm/el$releasever/
    enabled=1
    gpgcheck=1
    gpgkey=https://packages.edgedb.com/keys/edgedb.asc
    EOF

Step 2. Install the EdgeDB package:


.. code-block:: bash

    sudo yum install edgedb-1-alpha1


---------------
Debian / Ubuntu
---------------

Step 1. Import the EdgeDB packaging key:

.. code-block:: bash

    curl https://packages.edgedb.com/keys/edgedb.asc \
        | sudo apt-key add -

Step 2. Add the EdgeDB package repository:

.. code-block:: bash

    dist=$(awk -F"=" '/VERSION_CODENAME=/ {print $2}' /etc/os-release)
    [ -n "${dist}" ] || \
        dist=$(awk -F"[)(]+" '/VERSION=/ {print $2}' /etc/os-release)
    echo deb https://packages.edgedb.com/apt ${dist} main \
        | sudo tee /etc/apt/sources.list.d/edgedb.list

Step 3. Install the EdgeDB package:

.. code-block:: bash

    apt-get update && apt-get install edgedb-1-alpha1


-----
macOS
-----

You can download and install the `macOS EdgeDB package`_ using the normal
package installation GUI.

It is also possible to install the package from the command line:

.. code-block:: bash

    sudo installer -pkg /path/to/edgedb-1-alpha1.pkg -target /


.. _`macOS EdgeDB package`:
        https://packages.edgedb.com/macos/edgedb-1-alpha1.pkg
