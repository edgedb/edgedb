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
    EdgeDB CLI 2.x+abcdefg

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
  EdgeDB CLI
  Use the edgedb command-line tool to spin up local instances, manage EdgeDB
  projects, create and apply migrations, and more.

  Running edgedb without a subcommand opens an interactive shell.

  USAGE:
    edgedb [OPTIONS] [SUBCOMMAND]

  OPTIONS:
    <list of options>

  CONNECTION OPTIONS (edgedb --help-connect to see the full list):
    <list of connection options>

  SUBCOMMANDS:
    <list of all major commands>

The majority of CLI commands perform some action against a *particular* EdgeDB
instance. As such, there are a standard set of flags that are used to specify
*which instance* should be the target of the command, plus additional
information like TLS certificates. The following command documents these flags.

.. code-block:: bash

  $ edgedb --help-connect
  -I, --instance <instance>
        Local instance name created with edgedb instance create to connect to
        (overrides host and port)
  --dsn <dsn>
        DSN for EdgeDB to connect to (overrides all other options except
        password)
  --credentials-file <credentials_file>
        Path to JSON file to read credentials from
  -H, --host <host>
        Host of the EdgeDB instance
  -P, --port <port>
        Port to connect to EdgeDB
  --unix-path <unix_path>
        Unix socket dir for the
  -u, --user <user>
        User name of the EdgeDB user
  -d, --database <database>
        Database name to connect to
  --password
        Ask for password on the terminal (TTY)
  --no-password
        Don't ask for password

If you ever want to see documentation for a particular command (``edgedb
migration create``) or group of commands (``edgedb instance``), just append
the ``--help`` flag.

.. code-block:: bash

  $ edgedb instance --help
  Manage local EdgeDB instances

  USAGE:
      edgedb instance <SUBCOMMAND>

  OPTIONS:
      -h, --help    Print help information

  SUBCOMMANDS:
      create            Initialize a new EdgeDB instance
      credentials       Echo credentials to connect to the instance
      destroy           Destroy an instance and remove the data
      link              Link a remote instance
      list              Show all instances
      ...

Upgrade the CLI
---------------

To upgrade to the latest version:

.. code-block:: bash

  $ edgedb cli upgrade
