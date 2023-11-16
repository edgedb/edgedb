.. _ref_intro_cli:

.. _ref_admin_install:

=======
The CLI
=======

The ``edgedb`` command line tool is an integral part of the developer workflow
of building with EdgeDB. Below are instructions for installing it.

Installation
------------

To get started with EdgeDB, the first step is install the ``edgedb`` CLI.

**Linux or macOS**

.. code-block:: bash

    $ curl --proto '=https' --tlsv1.2 -sSf https://sh.edgedb.com | sh

**Windows Powershell**

.. note::

    EdgeDB on Windows requires WSL 2 because the EdgeDB server runs on Linux.

.. code-block:: powershell

    PS> iwr https://ps1.edgedb.com -useb | iex

Follow the prompts on screen to complete the installation. The script will
download the ``edgedb`` command built for your OS and add a path to it to your
shell environment. Then test the installation:

.. code-block:: bash

    $ edgedb --version
    EdgeDB CLI 4.x+abcdefg

.. note::

  If you encounter a ``command not found`` error, you may need to open a fresh
  shell window.

.. note::

    To install the CLI with a package manager, refer to the "Additional
    methods" section of the `Install <https://www.edgedb.com/install>`_ page
    for instructions.


See ``help`` commands
---------------------

The entire CLI is self-documenting. Once it's installed, run ``edgedb --help``
to see a breakdown of all the commands and options.

.. code-block:: bash

  $ edgedb --help
  Usage: edgedb [OPTIONS] [COMMAND]

  Commands:
    <list of commands>

  Options:
    <list of options>

  Connection Options (edgedb --help-connect to see full list):
    <list of connection options>

  Cloud Connection Options:
    <list of cloud connection options>

The majority of CLI commands perform some action against a *particular* EdgeDB
instance. As such, there are a standard set of flags that are used to specify
*which instance* should be the target of the command, plus additional
information like TLS certificates. The following command documents these flags.

.. code-block:: bash

  $ edgedb --help-connect
  Connection Options (full list):

    -I, --instance <INSTANCE>
            Instance name (use `edgedb instance list` to list local, remote and
            Cloud instances available to you)

        --dsn <DSN>
            DSN for EdgeDB to connect to (overrides all other options except
            password)

        --credentials-file <CREDENTIALS_FILE>
            Path to JSON file to read credentials from

    -H, --host <HOST>
            EdgeDB instance host

    -P, --port <PORT>
            Port to connect to EdgeDB

        --unix-path <UNIX_PATH>
            A path to a Unix socket for EdgeDB connection

            When the supplied path is a directory, the actual path will be
            computed using the `--port` and `--admin` parameters.
    ...

If you ever want to see documentation for a particular command (``edgedb
migration create``) or group of commands (``edgedb instance``), just append
the ``--help`` flag.

.. code-block:: bash

  $ edgedb instance --help
  Manage local EdgeDB instances

  Usage: edgedb instance <COMMAND>

  Commands:
    create          Initialize a new EdgeDB instance
    list            Show all instances
    status          Show status of an instance
    start           Start an instance
    stop            Stop an instance
    ...

Upgrade the CLI
---------------

To upgrade to the latest version:

.. code-block:: bash

  $ edgedb cli upgrade
