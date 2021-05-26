.. eql:section-intro-page:: cli

.. _ref_cli_overview:

========
Overview
========

:edgedb-alt-title: EdgeDB Commands

EdgeDB includes the all-encompassing ``edgedb`` command-line tool. It
provides an idiomatic way to do just about everything: install EdgeDB,
spin up a local instance, open a REPL, execute queries, manage auth
roles, introspect a database schema, create migrations, and more.

You can install it with one shell command.

.. _ref_cli_edgedb_install:

.. rubric:: Installation

On Linux or MacOS, run the following in your terminal and follow the
on-screen instructions:

.. code-block:: bash

    $ curl --proto '=https' -sSf1 https://sh.edgedb.com | sh

For Windows, the installation script is:

.. code-block:: bash

    PS> iwr https://ps1.edgedb.com -useb | iex

* The `script <https://sh.edgedb.com>`_, inspired by ``rustup``, will
  detect the OS and download the appropriate build of the EdgeDB CLI
  tool, ``edgedb``.
* The ``edgedb`` command is a single executable (it's `open source!
  <https://github.com/edgedb/edgedb-cli/>`_)
* Once installed, the ``edgedb`` command can be used to install,
  uninstall, upgrade, and interact with EdgeDB server instances.
* You can uninstall EdgeDB server or remove the ``edgedb`` command at
  any time.

With command-line tools installed see
:ref:`server installation <ref_tutorial_install>` or
:ref:`connection options <ref_cli_edgedb_connopts>` to connect to a remote
server.

To install the nightly version of the command-line tools (that's not
the same as the nightly version of the EdgeDB server) use this
command:

.. code-block:: bash

    $ curl --proto '=https' --tlsv1.2 -sSf https://sh.edgedb.com | \
      sh -s -- --nightly


.. _ref_cli_edgedb_uninstall:

.. rubric:: Uninstallation

Command-line tools contain just one binary, so to remove it on Linux or
macOS run:


.. code-block:: bash

   $ rm "$(which edgedb)"

To also remove configuration files:

.. code-block:: bash

   $ rm -rf ~/.edgedb

If the command-line tool was installed by the user (recommended) then it
will also remove the binary.

If you've used ``edgedb server`` commands you can also delete
:ref:`instances <ref_cli_edgedb_server_destroy>` and :ref:`server
<ref_cli_edgedb_server_uninstall>` packages, prior to removing the
tool:

.. code-block:: bash

   $ edgedb server destroy <instance_name>
   $ edgedb server uninstall --version=<ver>

To list instances and server versions use the following commands
respectively:

.. code-block:: bash

   $ edgedb server status
   $ edgedb server list-versions --installed


:ref:`Notes on network usage <ref_cli_edgedb_network>`
