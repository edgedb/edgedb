.. _ref_intro_projects:

========
Projects
========

It can be inconvenient to pass the ``-I`` flag every time you wish to run a
CLI command.

.. code-block:: bash

  $ gel migration create -I my_instance

That's one of the reasons we introduced the concept of an *Gel
project*. A project is a directory on your file system that is associated
("linked") with an Gel instance.

.. note::

  Projects are intended to make *local development* easier! They only exist on
  your local machine and are managed with the CLI. When deploying Gel for
  production, you will typically pass connection information to the client
  library using environment variables.

When you're inside a project, all CLI commands will be applied against the
*linked instance* by default (no CLI flags required).

.. code-block:: bash

  $ gel migration create

The same is true for all Gel client libraries (discussed in more depth in
the :ref:`Clients <ref_intro_clients>` section). If the following file lives
inside an Gel project directory, ``createClient`` will discover the project
and connect to its linked instance with no additional configuration.

.. code-block:: typescript

    // clientTest.js
    import {createClient} from 'edgedb';

    const client = createClient();
    await client.query("select 5");

Initializing
^^^^^^^^^^^^

To initialize a project, create a new directory and run :gelcmd:`project init`
inside it. You'll see something like this:

.. code-block:: bash

  $ gel project init
  No `gel.toml` found in this repo or above.
  Do you want to initialize a new project? [Y/n]
  > Y
  Specify the name of Gel instance to use with this project
  [default: my_instance]:
  > my_instance
  Checking Gel versions...
  Specify the version of Gel to use with this project [default: x.x]:
  > # (left blank for default)
  ...
  Successfully installed x.x+cc4f3b5
  Initializing Gel instance...
  Applying migrations...
  Everything is up to date. Revision initial
  Project initialized.
  To connect to my_instance, run `edgedb`

This command does a couple important things.

1. It spins up a new Gel instance called ``my_instance``.
2. If no |gel.toml| file exists, it will create one. This is a
   configuration file that marks a given directory as an Gel project. Learn
   more about it in the :ref:`gel.toml reference <ref_reference_gel_toml>`.

   .. code-block:: toml

     [edgedb]
     server-version = "4.1"

3. If no ``dbschema`` directory exists, it will be created, along with an
   empty ``default.esdl`` file which will contain your schema. If a
   ``dbschema`` directory exists and contains a subdirectory called
   ``migrations``, those migrations will be applied against the new instance.

Every project maps one-to-one to a particular Gel instance. From
inside a project directory, you can run :gelcmd:`project info` to see
information about the current project.

.. code-block:: bash

  $ gel project info
  ┌───────────────┬──────────────────────────────────────────┐
  │ Instance name │ my_instance                              │
  │ Project root  │ /path/to/project                         │
  └───────────────┴──────────────────────────────────────────┘


Connection
^^^^^^^^^^

As long as you are inside the project directory, all CLI commands will be
executed against the project-linked instance. For instance, you can simply run
|gelcmd| to open a REPL.

.. code-block:: bash

  $ gel
  Gel x.x+cc4f3b5 (repl x.x+da2788e)
  Type \help for help, \quit to quit.
  my_instance:edgedb> select "Hello world!";

By contrast, if you leave the project directory, the CLI will no longer know
which instance to connect to. You can solve this by specifing an instance name
with the ``-I`` flag.

.. code-block:: bash

  $ cd ~
  $ gel
  gel error: no `gel.toml` found and no connection options are specified
    Hint: Run `edgedb project init` or use any of `-H`, `-P`, `-I` arguments to
    specify connection parameters. See `--help` for details
  $ gel -I my_instance
  Gel x.x+cc4f3b5 (repl x.x+da2788e)
  Type \help for help, \quit to quit.
  my_instance:edgedb>

Similarly, client libraries will auto-connect to the project's
linked instance without additional configuration.

Using remote instances
^^^^^^^^^^^^^^^^^^^^^^

You may want to initialize a project that points to a remote Gel instance.
This is totally a valid case and Gel fully supports it! Before running
:gelcmd:`project init`, you just need to create an alias for the remote
instance using :gelcmd:`instance link`, like so:

.. lint-off

.. code-block:: bash

  $ gel instance link
  Specify server host [default: localhost]:
  > 192.168.4.2
  Specify server port [default: 5656]:
  > 10818
  Specify database user [default: edgedb]:
  > edgedb
  Specify branch [default: main]:
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

After receiving the necessary connection information, this command links the
remote instance to a local alias ``"staging_db"``. You can use this as
instance name in CLI commands.

.. code-block::

  $ gel -I staging_db
  gel>

To initialize a project that uses the remote instance, provide this alias when
prompted for an instance name during the :gelcmd:`project init` workflow.


Unlinking
^^^^^^^^^

An instance can be unlinked from a project. This leaves the instance running
but effectively "uninitializes" the project. The |gel.toml| and
``dbschema`` are left untouched.

.. code-block:: bash

    $ gel project unlink

If you wish to delete the instance as well, use the ``-D`` flag.

.. code-block:: bash

    $ gel project unlink -D

Upgrading
^^^^^^^^^

A standalone instance (not linked to a project) can be upgraded with the
:gelcmd:`instance upgrade` command.

.. code-block:: bash

  $ gel project upgrade --to-latest
  $ gel project upgrade --to-nightly
  $ gel project upgrade --to-version x.x


See info
^^^^^^^^

You can see the location of a project and the name of its linked instance.

.. code-block:: bash

  $ gel project info
  ┌───────────────┬──────────────────────────────────────────┐
  │ Instance name │ my_app                                   │
  │ Project root  │ /path/to/my_app                          │
  └───────────────┴──────────────────────────────────────────┘
