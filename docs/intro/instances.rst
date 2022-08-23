.. _ref_intro_instances:

=========
Instances
=========


Let's get to the good stuff. Spin up an EdgeDB instance with a single command:

.. code-block:: bash

  $ edgedb instance create my_instance

This creates a new instance running the latest stable version of EdgeDB.
Alternatively you can specify a specific version with ``--version``.

.. code-block:: bash

  $ edgedb instance create my_instance --version 2.0
  $ edgedb instance create my_instance --version nightly

We can execute a query against our new instance with ``edgedb query``. Note
that we need to specify which instance to connect to by passing an instance
name into the ``-I`` flag.

.. code-block:: bash

  $ edgedb query -I qb "select 3.14;"
  3.14


Initialize a project
--------------------

It can be inconvenient to pass the ``-I`` flag every time you wish to run a
CLI command. That's one of the reasons we introduced the concept of an *EdgeDB
project*. A project is a directory on your file system that is associated with
an EdgeDB intance.

To initialize one, create a new directory and run ``edgedb
project init`` inside it. You'll see something like this:

.. code-block:: bash

  $ edgedb project init
  No `edgedb.toml` found in this repo or above.
  Do you want to initialize a new project? [Y/n]
  > Y
  Specify the version of EdgeDB to use with this project [2.x]:
  > # (left blank for default)
  Specify the name of EdgeDB instance to use with this project:
  > my_instance
  Initializing EdgeDB instance...
  Bootstrap complete. Server is up and running now.
  Project initialialized.

This command does a couple important things.

1. It spins up a new EdgeDB instance called ``my_instance``.
2. If no ``edgedb.toml`` file exists, it will create one. This is a
   configuration file that indicates that a given directory is an EdgeDB
   project. Currently it only supports a single setting: ``server-version``.

   .. code-block:: toml

     [edgedb]
     server-version = 2.1
3. If no ``dbschema`` directory exists, it will be created, along with an
   empty ``default.esdl`` file which will contain your schema. If a
   ``dbschema`` directory exists and contains a subdirectory called
   ``migrations``, those migrations will be applied against the new instance.

Every project maps one-to-one to a particular EdgeDB instance. From
inside a project directory, you can run ``edgedb project info`` to see
information about the current project.

.. code-block:: bash

  $ edgedb project info
  ┌───────────────┬──────────────────────────────────────────┐
  │ Instance name │ my_instance                              │
  │ Project root  │ /path/to/project                         │
  └───────────────┴──────────────────────────────────────────┘

As long as you are inside the project directory, all CLI commands will be
executed against the project-linked instance. For instance, you can simply run
``edgedb`` to open a REPL.

.. code-block:: bash

  $ edgedb
  EdgeDB 2.0+88c1706 (repl 2.0.4+a7fc49b)
  Type \help for help, \quit to quit.
  edgedb> select "Hello world!";

By contrast, if you leave the project directory, the CLI will no longer know
which instance to connect to. You can solve this by specifing an instance name
with the ``-I`` flag.

.. code-block::

  $ cd ~
  $ edgedb
  ClientNoCredentialsError: no `edgedb.toml` found and no connection options are specified
  Hint: Run `edgedb project init` or use any of `-H`, `-P`, `-I` arguments to specify connection parameters. See `--help` for details
  $ edgedb -I my_instance
  EdgeDB 2.0+88c1706 (repl 2.0.4+a7fc49b)
  Type \help for help, \quit to quit.
  edgedb>

Similarly, client libraries will auto-connect to the project's
linked instance without additional configuration.

.. note::

  We `introduced projects <https://www.edgedb.com/blog/introducing-edgedb-projects>`_ as a simpler mechanism for developing

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

