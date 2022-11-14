.. _ref_intro_projects:

========
Projects
========

It can be inconvenient to pass the ``-I`` flag every time you wish to run a
CLI command.

.. code-block:: bash

  $ edgedb migration create -I my_instance

That's one of the reasons we introduced the concept of an *EdgeDB
project*. A project is a directory on your file system that is associated
("linked") with an EdgeDB instance.

.. note::

  Projects are intended to make *local development* easier! They only exist on
  your local machine and are managed with the CLI. When deploying EdgeDB for
  production, you will typically pass connection information to the client
  library using environment variables.

When you're inside a project, all CLI commands will be applied against the
*linked instance* by default (no CLI flags required).

.. code-block:: bash

  $ edgedb migration create

The same is true for all EdgeDB client libraries (discussed in more depth in
the :ref:`Clients <ref_intro_clients>` section). If the following file lives
inside an EdgeDB project directory, ``createClient`` will discover the project
and connect to its linked instance with no additional configuration.

.. code-block:: typescript

    // clientTest.js
    import {createClient} from 'edgedb';

    const client = createClient();
    await client.query("select 5");

Initializing
^^^^^^^^^^^^

To initialize a project, create a new directory and run ``edgedb
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
     server-version = "2.1"

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


Connection
^^^^^^^^^^

As long as you are inside the project directory, all CLI commands will be
executed against the project-linked instance. For instance, you can simply run
``edgedb`` to open a REPL.

.. code-block:: bash

  $ edgedb
  EdgeDB 2.x+88c1706 (repl 2.x+a7fc49b)
  Type \help for help, \quit to quit.
  edgedb> select "Hello world!";

By contrast, if you leave the project directory, the CLI will no longer know
which instance to connect to. You can solve this by specifing an instance name
with the ``-I`` flag.

.. code-block:: bash

  $ cd ~
  $ edgedb
  ClientNoCredentialsError: no `edgedb.toml` found and no
  connection options are specified
  Hint: Run `edgedb project init` or use any of `-H`, `-P`, `-I` arguments
  to specify connection parameters. See `--help` for details
  $ edgedb -I my_instance
  EdgeDB 2.x+88c1706 (repl 2.x+a7fc49b)
  Type \help for help, \quit to quit.
  edgedb>

Similarly, client libraries will auto-connect to the project's
linked instance without additional configuration.

Using remote instances
^^^^^^^^^^^^^^^^^^^^^^

You may want to initialize a project that points to a remote EdgeDB instance.
This is totally a valid case and EdgeDB fully supports it! Before running
``edgedb project init``, you just need to create an alias for the remote
instance using ``edgedb instance link``, like so:

.. lint-off

.. code-block:: bash

  $ edgedb instance link
  Specify the host of the server [default: localhost]:
  > 192.168.4.2
  Specify the port of the server [default: 5656]:
  > 10818
  Specify the database user [default: edgedb]:
  > edgedb
  Specify the database name [default: edgedb]:
  > edgedb
  Unknown server certificate: SHA1:c38a7a90429b033dfaf7a81e08112a9d58d97286.
  Trust? [y/N]
  > y
  Password for 'edgedb':
  Specify a new instance name for the remote server [default: abcd]:
  > staging_db
  Successfully linked to remote instance. To connect run:
    edgedb -I staging_db

.. lint-on

After receving the necessary connection information, this command links the
remote instance to a local alias ``"staging_db"``. You can use this as
instance name in CLI commands.

.. code-block::

  $ edgedb -I staging_db
  edgedb>

To initialize a project that uses the remote instance, provide this alias when
prompted for an instance name during the ``edgedb project init`` workflow.


Unlinking
^^^^^^^^^

An instance can be unlinked from a project. This leaves the instance running
but effectively "uninitializes" the project. The ``edgedb.toml`` and
``dbschema`` are left untouched.

.. code-block:: bash

    $ edgedb project unlink

If you wish to delete the instance as well, use the ``-D`` flag.

.. code-block:: bash

    $ edgedb project unlink -D

Upgrading
^^^^^^^^^

A standalone instance (not linked to a project) can be upgraded with the
``edgedb instance upgrade`` command.

.. code-block:: bash

  $ edgedb project upgrade --to-latest
  $ edgedb project upgrade --to-nightly
  $ edgedb project upgrade --to-version 2.x


See info
^^^^^^^^

You can see the location of a project and the name of its linked instance.

.. code-block:: bash

  $ edgedb project info
  ┌───────────────┬──────────────────────────────────────────┐
  │ Instance name │ my_app                                   │
  │ Project root  │ /path/to/my_app                          │
  └───────────────┴──────────────────────────────────────────┘
