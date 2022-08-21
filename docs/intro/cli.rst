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

The entire CLI is self-documenting. Once its installed, run ``edgedb -h`` to
see a breakdown of all the commands.

Creating an instance
--------------------

Projects are the most convenient way to develop applications with EdgeDB
*locally*. To get started, create a new directory and run ``edgedb project
init``. You'll see something like this:

.. code-block:: bash

  $ edgedb project init
  No `edgedb.toml` found in this repo or above.
  Do you want to initialize a new project? [Y/n]
  > Y
  Specify the version of EdgeDB to use with this project [2.1]:
  > # left blank for default
  Specify the name of EdgeDB instance to use with this project:
  > my_instance
  Initializing EdgeDB instance...
  Bootstrap complete. Server is up and running now.
  Project initialialized.

This command does a couple important things.

1. It spins up a new EdgeDB instance. *Every project has an associated
   instance.*
2. It scaffolds your project directory: it creates an
   ``edgedb.toml`` file (to indicate that this directory is the root of an
   EdgeDB project) and a new directory called ``dbschema`` (to contain your
   schema and migration files).

As long as you are inside the project directory, all CLI commands will be
executed against the project-linked. For instance, you can simply run
``edgedb`` to open a REPL to the newly created instance—no need to specify a
connection string, port, or instance name. The CLI sees that you're in a
project directory and automatically connects to the appropriate instance.

.. code-block:: bash

  $ edgedb
  EdgeDB 2.0+88c1706 (repl 2.0.4+a7fc49b)
  Type \help for help, \quit to quit.
  edgedb> select "Hello world!";

Similarly, all client libraries will auto-connect to the project's
linked instance without additional configuration.

Creating standalone instances
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It's possible to create instances that aren't linked to a project.

.. code-block:: bash

    $ edgedb instance create my_instance
    Initializing EdgeDB instance...
    Instance my_instance is up and running.
    To connect to the instance run:
      edgedb -I my_instance


Unlink a project
^^^^^^^^^^^^^^^^

An instance can be unlinked from a project.


.. code-block:: bash

    $ edgedb project unlink

This leaves the project's associated instance running. If you wish to delete
the instance as well, use the ``-D`` flag.

.. code-block:: bash

    $ edgedb project unlink -D

See project info
^^^^^^^^^^^^^^^^

You can see the location of a project and the name of its linked instance.

.. code-block:: bash

  $ edgedb project info
  ┌───────────────┬──────────────────────────────────────────┐
  │ Instance name │ my_app                                   │
  │ Project root  │ /path/to/my_app                          │
  └───────────────┴──────────────────────────────────────────┘


Open a REPL
-----------

Once you've created an instance, run ``edgedb`` to open a REPL.

.. code-block:: bash

    $ edgedb
    EdgeDB 2.0+88c1706 (repl 2.0.4+a7fc49b)
    Type \help for help, \quit to quit.
    edgedb> select "Hello world!";


Migrations
----------

To modify your schema, edit your ``.esdl`` schema file, then create a
migration using the CLI.

.. code-block:: bash

    $ edgedb migration create

Then apply the newly created migration.

.. code-block:: bash

    $ edgedb migrate

