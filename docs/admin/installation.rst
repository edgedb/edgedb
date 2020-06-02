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

    sudo yum install edgedb-1-alpha3


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

    apt-get update && apt-get install edgedb-1-alpha3


-----
macOS
-----

You can download and install the `macOS EdgeDB package`_ using the normal
package installation GUI.

It is also possible to install the package from the command line:

.. code-block:: bash

    sudo installer -pkg /path/to/edgedb-1-alpha3_latest.pkg -target /

Next, download the EdgeDB CLI executable:

.. code-block:: bash

    mkdir -p ~/.edgedb/bin/ \
    && curl https://packages.edgedb.com/dist/macos-x86_64/edgedb-cli_latest \
        > ~/.edgedb/bin/edgedb \
    && chmod +x ~/.edgedb/bin/edgedb


.. _`macOS EdgeDB package`:
      https://packages.edgedb.com/dist/macos-x86_64/
      edgedb-server-1-alpha3_latest.pkg


.. _ref_admin_install_docker:

Docker
======

Step 1. Pull the EdgeDB server Docker image:

.. code-block:: bash

    docker pull edgedb/edgedb

Step 2.  Run the container (replace ``<datadir>`` with the directory you
want to persist the data in):

.. code-block:: bash

    docker run -it --rm -p 5656:5656 -p 8888:8888 \
                -p 8889:8889 --name=edgedb-server \
                -v <datadir>:/var/lib/edgedb/data \
                edgedb/edgedb

When configuring extra :ref:`ports <ref_admin_config_connection>`, make
sure to expose them on the host by adding a corresponding ``-p`` argument to
the ``docker run`` command. The command above exposes the default ports used by
:ref:`EdgeQL over binary protocol <ref_protocol_overview>` (5656),
:ref:`EdgeQL over HTTP <ref_edgeql_index>` (8889), and
:ref:`GraphQL over HTTP <ref_graphql_index>` (8888).

Step3. Download the EdgeDB CLI executable as described below.


CLI Installation
================

To download the EdgeDB CLI tool, follow the instructions below.

-----
Linux
-----

Download the executable:

.. code-block:: bash

    mkdir -p ~/.local/bin/ \
    && curl https://packages.edgedb.com/dist/linux-x86_64/edgedb-cli_latest \
        > ~/.local/bin/edgedb \
    && chmod +x ~/.local/bin/edgedb

Add ``~/.local/bin`` to ``$PATH`` in ``~/.bash_profile`` or, if you don't
use Bash, ``~/.profile``:

.. code-block::

   export PATH="$HOME/.local/bin:$PATH"

Apply the ``$PATH`` change to the current shell by running
``source ~/.bash_profile`` or ``source ~/.profile``.


-----
macOS
-----

Download the executable:

.. code-block:: bash

    mkdir -p ~/.edgedb/bin/ \
    && curl https://packages.edgedb.com/dist/macos-x86_64/edgedb-cli_latest \
        > ~/.edgedb/bin/edgedb \
    && chmod +x ~/.edgedb/bin/edgedb

Add ``~/.edgedb/bin`` to ``$PATH`` in ``~/.bash_profile`` or, if you don't
use Bash, ``~/.profile``:

.. code-block::

   export PATH="$HOME/.edgedb/bin:$PATH"

Apply the ``$PATH`` change to the current shell by running
``source ~/.bash_profile`` or ``source ~/.profile``.
