.. _ref_cli_edgedb_install:

============
Installation
============


On Linux or MacOS, run the following in your terminal and follow the on-screen
instructions:

.. code-block:: bash

    $ curl --proto '=https' --tlsv1.2 -sSf https://sh.edgedb.com | sh

With command-line tools installed see
:ref:`server installation <ref_tutorial_install>` or
:ref:`connection options <ref_cli_edgedb_connopts>` to connect to a remote
server.


.. _ref_cli_edgedb_uninstall:

Uninstallation
==============

Command-line tools contain just one binary, so to remove it on Linux or
macOS run:


.. code-block:: bash

   $ rm "$(which edgedb)"

To also remove configuration files:

.. code-block:: bash

   $ rm -rf ~/.edgedb

If the command-line tool was installed by the user (recommended) then it
will also remove the binary.

If you've used ``edgedb server`` commands you can also delete instances
and server packages, prior to removing the tool:

.. code-block:: bash

   $ edgedb server destroy <instance_name>
   $ edgedb server uninstall --version=<ver>

To list instances and server versions use the following commands
respectively:

.. code-block:: bash

   $ edgedb server status
   $ edgedb server list-versions --installed
