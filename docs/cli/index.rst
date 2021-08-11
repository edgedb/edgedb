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

.. code-block:: powershell

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


.. rubric:: Connection options

All commands respect a common set of
:ref:`connection options <ref_cli_edgedb_connopts>`, which let you specify
a target instance. This instance can be local to your machine or hosted
remotely.


.. _ref_cli_edgedb_nightly:

.. rubric:: Nightly version

To install the nightly version of the CLI (not to be confused with the nightly
version of EdgeDB itself!) use this command:

.. code-block:: bash

    $ curl --proto '=https' --tlsv1.2 -sSf https://sh.edgedb.com | \
    sh -s -- --nightly


.. _ref_cli_edgedb_uninstall:

.. rubric:: Uninstallation

Command-line tools contain just one binary, so to remove it on Linux or
macOS run:

.. code-block:: bash

   $ rm "$(which edgedb)"

To remove all configuration files, run ``edgedb info`` to list the directories
where EdgeDB stores data, then use ``rf -rf <dir>`` to delete those
directories.

If the command-line tool was installed by the user (recommended) then it
will also remove the binary.

If you've used ``edgedb`` commands you can also delete
:ref:`instances <ref_cli_edgedb_instance_destroy>` and :ref:`server
<ref_cli_edgedb_server_uninstall>` packages, prior to removing the
tool:

.. code-block:: bash

   $ edgedb instance destroy <instance_name>
   $ edgedb server uninstall --version=<ver>

To list instances and server versions use the following commands
respectively:

.. code-block:: bash

   $ edgedb instance status
   $ edgedb server list-versions --installed


:ref:`Notes on network usage <ref_cli_edgedb_network>`
