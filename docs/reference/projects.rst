.. _ref_guide_using_projects:

================
Create a project
================

Projects are the most convenient way to develop applications with EdgeDB. This
is the recommended approach.

To get started, navigate to the root directory of your codebase in a shell and
run ``edgedb project init``. You'll see something like this:

.. code-block:: bash

  $ edgedb project init
  No `edgedb.toml` found in this repo or above.
  Do you want to initialize a new project? [Y/n]
  > Y
  Checking EdgeDB versions...
  Specify the version of EdgeDB to use with this project [1-rc3]:
  > # left blank for default
  Specify the name of EdgeDB instance to use with this project:
  > my_instance
  Initializing EdgeDB instance...
  Bootstrap complete. Server is up and running now.
  Project initialialized.

Let's unpack that.

1. First, it asks you to specify an EdgeDB version, defaulting to the most
   recent version you have installed. You can also specify a version you
   *don't* have installed, in which case it will be installed.
2. Then it asks you how you'd like to run EdgeDB: locally, in a Docker image,
   or in the cloud (coming soon!).
3. Then it asks for an instance name. If no instance currently exists with this
   name, it will be created (using the method you specified in #2).
4. Then it **links** the current directory to that instance. A "link" is
   represented as some metadata stored in EdgeDB's :ref:`config directory
   <ref_cli_edgedb_paths>`—feel free to peek inside to see how it's stored.
5. Then it creates an :ref:`ref_reference_edgedb_toml` file, which marks this
   directory as an EdgeDB project.
6. Finally, it creates a ``dbschema`` directory and a ``dbschema/default.esdl``
   schema file (if they don't already exist).


FAQ
---

How does this help me?
^^^^^^^^^^^^^^^^^^^^^^

Once you've initialized a project, your project directory is *linked* to a
particular instance. That means, you can run CLI commands without connection
flags. For instance, ``edgedb -I my_instance migrate`` becomes simply ``edgedb
migrate``. The CLI detects the existence of the ``edgedb.toml`` file, reads the
current directory, and checks if it's associated with an existing project. If
it is, it looks up the credentials of the linked instance (they're stored in a
:ref:`standardized location <ref_cli_edgedb_paths>`), uses that information to
connect to the instance, and applies the command.

Similarly, all :ref:`client libraries <ref_clients_index>` will use the same
mechanism to auto-connect inside project directories, no hard-coded credentials
required.

.. code-block:: typescript-diff

      import edgedb from "edgedb";

    - const pool = edgedb.createPool("my_instance");
    + const pool = edgedb.createPool();

What do you mean *link*?
^^^^^^^^^^^^^^^^^^^^^^^^

The "link" is just metaphor that makes projects easier to think about; in
practice, it's just a bit of metadata we store in the EdgeDB :ref:`config
directory <ref_cli_edgedb_paths>`. When the CLI or client libraries try to
connect to an instance, they read the currect directory and cross-reference it
against the list of initialized projects. If there's a match, it reads the
credentials of the project's associated instance and auto-connects.

How does this work in production?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It doesn't. Projects are intended as a convenient development tool that make it
easier to develop EdgeDB-backed applications locally. In production, you should
provide instance credentials to your client library of choice using environment
variables. See :ref:`Connection parameters <ref_reference_connection>` page for
more information.


What's the ``edgedb.toml`` file?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The most important role of ``edgedb.toml`` is to mark a directory as an
instance-linked project, but it can also specify the server version and the
schema directory for a project. The server version value in the generated
``edgedb.toml`` is determined by the EdgeDB version you selected when you ran
:ref:`ref_cli_edgedb_project_init`.

Read :ref:`our reference documentation on edgedb.toml
<ref_reference_edgedb_toml>` to learn more.

.. note::

    If you're not familiar with the TOML file format, it's a very cool, minimal
    language for config files designed to be simpler than JSON or YAML. Check
    out `the TOML documentation <https://toml.io/en/v1.0.0>`_.


How do I use ``edgedb project`` for existing codebases?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you already have an project on your computer that uses EdgeDB, follow these
steps to convert it into an EdgeDB project:

1. Navigate into the project directory (the one containing you ``dbschema``
   directory).
2. Run ``edgedb project init``.
3. When asked for an instance name, enter the name of the existing local
   instance you use for development.

This will create ``edgedb.toml`` and link your project directory to the
instance. And you're done! Try running some commands without connection flags.
Feels good, right?

How does this make projects more portable?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's say you just cloned a full-stack application that uses EdgeDB. The
project directory already contains an ``edgedb.toml`` file. What do you do?

Just run ``edgedb project init`` inside the directory! This is the beauty of
``edgedb project``. You don't need to worry about creating an instance with a
particular name, running on a particular port, creating users and passwords,
specifying environment variables, or any of the other things that make setting
up local databases hard. Running ``edgedb project init`` will install the
necessary version of EdgeDB (if you don't already have it installed), create an
instance, apply all unapplied migrations. Then you can start up the application
and it should work out of the box.


How do I unlink a project?
^^^^^^^^^^^^^^^^^^^^^^^^^^

If you want to remove the link between your project and its linked instance,
run ``edgedb project unlink`` anywhere inside the project. This doesn't affect
the instance, it continues running as before. After unlinking, can run ``edgedb
project init`` inside project again to create or select a new instance.


.. code-block:: bash

  $ edgedb project init
  No `edgedb.toml` found in `~/path/to/my_project` or above.
  Do you want to initialize a new project? [Y/n]
  > Y
  Specify the name of EdgeDB instance to use with this project
  [default: my_project]:
  > my_project
  Checking EdgeDB versions...
  Specify the version of EdgeDB to use with this project [default: 2.x]:
  > 2.x


How do I use ``edgedb project`` with a non-local instance?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sometimes you may want to work on an EdgeDB instance that is just not in your
local development environment, like you may have a second workstation, or you
want to test against a staging database shared by the team.

This is totally a valid case and EdgeDB fully supports it!

Before running ``edgedb project init``, you just need to create a local link to
the remote EdgeDB instance first:

.. TODO: Will need to change this once https://github.com/edgedb/edgedb-cli/issues/1269 is resolved

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
  Unknown server certificate: SHA1:c38a7a90429b033dfaf7a81e08112a9d58d97286. Trust? [y/N]
  > y
  Password for 'edgedb':
  Specify a new instance name for the remote server [default: 192_168_4_2_10818]:
  > staging_db
  Successfully linked to remote instance. To connect run:
    edgedb -I staging_db

.. lint-on

Then you could run the normal ``edgedb project init`` and use ``staging_db`` as
the instance name.

.. note::

  When using an existing instance, make sure that the project source tree is in
  sync with the current migration revision of the instance. If the current
  revision in the database doesn't exist under ``dbschema/migrations/``, it'll
  raise an error trying to migrate or create new migrations. In this case, you
  should update your local source tree to the revision that matches the current
  revision of the database.
