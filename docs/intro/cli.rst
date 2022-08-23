.. _ref_intro_cli:

.. _ref_admin_install:

=============
Using the CLI
=============

Below are instructions for installing the CLI, installed EdgeDB itself, and
managing installing.

Install the CLI
---------------

To get started with EdgeDB, the first step is install the ``edgedb`` CLI.

**Linux or macOS**

.. code-block:: bash

    $ curl --proto '=https' --tlsv1.2 -sSf https://sh.edgedb.com | sh

**Windows Powershell**

.. code-block:: powershell

    PS> iwr https://ps1.edgedb.com -useb | iex

Follow the prompts on screen to complete the installation. The script will
download the ``edgedb`` command built for your OS and add a path to it to your
shell environment. To test the installation, run ``edgedb --version`` from the
command line.


.. code-block:: bash

    $ edgedb --version
    EdgeDB CLI 1.x+abcdefg


If you encounter a ``command not found`` error, you may need to open a new
terminal window before the ``edgedb`` command is available.


.. note::

    To install the CLI with a package manager, refer to the "Additional
    methods" section of the `Install <https://www.edgedb.com/install>`_ page
    for instructions.


See ``help`` commands
---------------------

The entire CLI is self-documenting. Once its installed, run ``edgedb --help``
to see a breakdown of all the commands. You can append the ``--help`` flag to any command or group of commands to see documentation.

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

