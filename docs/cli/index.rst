.. eql:section-intro-page:: cli

.. _ref_cli_overview:

===
CLI
===

:edb-alt-title: The Gel CLI

The |gelcmd| command-line interface (CLI) provides an idiomatic way to
install |Gel|, spin up local instances, open a REPL, execute queries,
manage auth roles, introspect schema, create migrations, and more.

You can install it with one shell command.

.. _ref_cli_gel_install:

.. rubric:: Installation

On Linux or MacOS, run the following in your terminal and follow the
on-screen instructions:

.. code-block:: bash

    $ curl --proto '=https' --tlsv1.2 -sSf https://geldata.com/sh | sh

For Windows, the installation script is:

.. code-block:: powershell

    PS> iwr https://geldata.com/ps1 -useb | iex

* The `script <https://geldata.com/sh>`_, inspired by ``rustup``, will
  detect the OS and download the appropriate build of the Gel CLI
  tool, ``gel``.
* The |gelcmd| command is a single executable (it's `open source!
  <https://github.com/geldata/gel-cli/>`_)
* Once installed, the ``gel`` command can be used to install,
  uninstall, upgrade, and interact with |Gel| server instances.
* You can uninstall Gel server or remove the ``gel`` command at
  any time.


.. rubric:: Connection options

All commands respect a common set of
:ref:`connection options <ref_cli_gel_connopts>`, which let you specify
a target instance. This instance can be local to your machine or hosted
remotely.


.. _ref_cli_gel_nightly:

.. rubric:: Nightly version

To install the nightly version of the CLI (not to be confused with the nightly
version of |Gel| itself!) use this command:

.. code-block:: bash

    $ curl --proto '=https' --tlsv1.2 -sSf https://geldata.com/sh | \
      sh -s -- --nightly


.. _ref_cli_gel_uninstall:

.. rubric:: Uninstallation

Command-line tools contain just one binary, so to remove it on Linux or
macOS run:

.. code-block:: bash

   $ rm "$(which gel)"

To remove all configuration files, run :gelcmd:`info` to list the directories
where |Gel| stores data, then use ``rm -rf <dir>`` to delete those
directories.

If the command-line tool was installed by the user (recommended) then it
will also remove the binary.

If you've used ``gel`` commands you can also delete
:ref:`instances <ref_cli_gel_instance_destroy>` and :ref:`server
<ref_cli_gel_server_uninstall>` packages, prior to removing the
tool:

.. code-block:: bash

   $ gel instance destroy <instance_name>

To list instances and server versions use the following commands
respectively:

.. code-block:: bash

   $ gel instance status
   $ gel server list-versions --installed-only


.. _ref_cli_gel_config:

.. rubric:: Configure CLI and REPL

You can customize the behavior of the |gelcmd| CLI and REPL with a
global configuration file. The file is called ``cli.toml`` and its
location differs between operating systems. Use
:ref:`ref_cli_gel_info` to find the "Config" directory on your
system.

The ``cli.toml`` has the following structure. All fields are optional:

.. code-block::

    [shell]
    expand-strings = true         # Stop escaping newlines in quoted strings
    history-size = 10000          # Set number of entries retained in history
    implicit-properties = false   # Print implicit properties of objects
    limit = 100                   # Set implicit LIMIT
                                  # Defaults to 100, specify 0 to disable
    input-mode = "emacs"          # Set input mode. One of: vi, emacs
    output-format = "default"     # Set output format.
                                  # One of: default, json, json-pretty,
                                  # json-lines
    print-stats = "off"           # Print statistics on each query.
                                  # One of: off, query, detailed
    verbose-errors = false        # Print all errors with maximum verbosity


:ref:`Notes on network usage <ref_cli_gel_network>`


.. toctree::
    :maxdepth: 3
    :hidden:

    gel_connopts
    network
    gel
    gel_project/index
    gel_ui
    gel_watch
    gel_migrate
    gel_migration/index
    gel_cloud/index
    gel_branch/index
    gel_dump
    gel_restore
    gel_configure
    gel_query
    gel_analyze
    gel_list
    gel_info
    gel_cli_upgrade
    gel_server/index
    gel_describe/index
    gel_instance/index
    gel_database/index
