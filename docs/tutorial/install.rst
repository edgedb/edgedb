.. _ref_tutorial_install:

1. Installation
===============

.. NOTE this is a good place to mention sublime, atom, vs code and vim
..      extensions for EdgeDB

The easiest way to install EdgeDB is to pick one of the pre-built packages
from the `download page`_.  Please follow the installation instructions
appropriate for your OS.

In most cases, after a successful installation, EdgeDB would automatically
start a local system-wide server instance.  If it didn't, and you see
connection errors, run:

On Linux:

.. code-block:: bash

    $ sudo systemctl start edgedb-1-alpha1

On macOS:

.. code-block:: bash

    $ sudo launchctl enable system/com.edgedb.edgedb-1-alpha1

Once the installation is complete, we need to set the password for the
database superuser:

.. code-block:: bash

    $ sudo -u edgedb edgedb --admin alter-role edgedb --password

With that done we should be able to connect to the EdgeDB server instance
using the newly set password:

.. code-block:: bash

    $ edgedb -u edgedb

.. _`download page`:
        https://www.edgedb.com/download/

With EdgeDB up and running we're ready to
:ref:`create a schema <ref_tutorial_createdb>`.
