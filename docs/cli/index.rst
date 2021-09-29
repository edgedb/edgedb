.. eql:section-intro-page:: cli

.. _ref_cli_overview:

============
Command Line
============

:edb-alt-title: 'edgedb' command

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


.. _ref_cli_edgedb_config:

.. rubric:: Configure CLI and REPL

You can customize the behavior of the ``edgedb`` CLI and REPL with a
global configuration file. The file is called ``cli.toml`` and its
location differs between operating systems. Use
:ref:`ref_cli_edgedb_info` to find the "Config" directory on your
system.

The ``cli.toml`` has the following structure. All fields are optional:

.. code-block::

    [shell]
    expand-strings = true         # Stop escaping newlines in quoted strings
    history-size = 10000          # Set number of entries retained in history
    implicit-properties = false   # Print implicit properties of objects
    implicit-limit = 100          # Set implicit LIMIT
                                  # Defaults to 100, specify 0 to disable
    input-mode = "emacs"          # Set input mode. One of: vi, emacs
    output-format = "default"     # Set output format.
                                  # One of: default, json, json-pretty,
                                  # json-lines
    print-stats = false           # Print statistics on each query
    verbose-errors = false        # Print all errors with maximum verbosity


:ref:`Notes on network usage <ref_cli_edgedb_network>`


.. toctree::
    :maxdepth: 3
    :hidden:

    edgedb
    edgedb_dump
    edgedb_restore
    edgedb_configure
    edgedb_migration/index
    edgedb_migrate
    edgedb_database_create
    edgedb_describe/index
    edgedb_list
    edgedb_query
    edgedb_info
    edgedb_project/index
    edgedb_instance/index
    edgedb_server/index
    edgedb_cli_upgrade
    edgedb_connopts
    network
