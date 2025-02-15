.. _ref_quickstart:

==========
Quickstart
==========

Welcome to |Gel|!

This quickstart will walk you through the entire process of creating a simple
Gel-powered application: installation, defining your schema, adding some
data, and writing your first query. Let's jump in!

.. _ref_quickstart_install:

1. Installation
===============

First let's install the Gel CLI. Open a terminal and run the appropriate
command below.

JavaScript and Python users
---------------------------

If you use ``npx`` or ``uvx`` you can skip the installation steps below
and use Gel CLI like this:

.. code-block:: bash

  # JavaScript:
  $ npx gel project init

  # Python
  $ uvx gel project init


Linux
-----

.. tabs::

    .. code-tab:: bash
        :caption: Script

        $ curl https://geldata.com/sh --proto '=https' -sSf1 | sh

    .. code-tab:: bash
        :caption: APT

        $ # Import the Gel packaging key
        $ sudo mkdir -p /usr/local/share/keyrings && \
          sudo curl --proto '=https' --tlsv1.2 -sSf \
            -o /usr/local/share/keyrings/gel-keyring.gpg \
            https://packages.geldata.com/keys/gel-keyring.gpg && \
        $ # Add the Gel package repository
        $ echo deb [signed-by=/usr/local/share/keyrings/gel-keyring.gpg]\
            https://packages.geldata.com/apt \
            $(grep "VERSION_CODENAME=" /etc/os-release | cut -d= -f2) main \
            | sudo tee /etc/apt/sources.list.d/gel.list
        $ # Install the Gel package
        $ sudo apt-get update && sudo apt-get install gel-6

    .. code-tab:: bash
        :caption: YUM

        $ # Add the Gel package repository
        $ sudo curl --proto '=https' --tlsv1.2 -sSfL \
            https://packages.geldata.com/rpm/gel-rhel.repo \
            > /etc/yum.repos.d/gel.repo
        $ # Install the Gel package
        $ sudo yum install gel-6

macOS
-----

.. tabs::

    .. code-tab:: bash
        :caption: Script

        $ curl https://geldata.com/sh --proto '=https' -sSf1 | sh

    .. code-tab:: bash
        :caption: Homebrew

        $ # Add the Gel tap to your Homebrew
        $ brew tap geldata/tap
        $ # Install Gel CLI
        $ brew install gel-cli

Windows (Powershell)
--------------------

.. note::

    Gel on Windows requires WSL 2 to create local instances because the
    Gel server runs on Linux. It is *not* required if you will use the CLI
    only to manage Gel Cloud and/or other remote instances. This quickstart
    *does* create local instances, so WSL 2 is required to complete the
    quickstart.

.. code-block:: powershell

    PS> iwr https://geldata.com/ps1 -useb | iex

.. note:: Command prompt installation

    To install Gel in the Windows Command prompt, follow these steps:

    1. `Download the CLI <https://packages.geldata.com/dist/x86_64-pc-windows-msvc/gel-cli.exe>`__

    2. Navigate to the download location in the command prompt

    3. Run the installation command:

    .. code-block::

        gel-cli.exe _self_install

The script installation methods download and execute a bash script that
installs the |gelcmd| CLI on your machine. You may be asked for your
password. Once the installation completes, you may need to **restart your
terminal** before you can use the |gelcmd| command.

Now let's set up your Gel project.

.. _ref_quickstart_createdb:

2. Initialize a project
=======================

In a terminal, create a new directory and ``cd`` into it.

.. code-block:: bash

  $ mkdir quickstart
  $ cd quickstart

Then initialize your Gel project:

.. code-block:: bash

  $ gel project init

This starts an interactive tool that walks you through the process of setting
up your first Gel instance. You should see something like this:

.. code-block:: bash

  $ gel project init
  No `tel.toml` found in `/path/to/quickstart` or above
  Do you want to initialize a new project? [Y/n]
  > Y
  Specify the name of Gel instance to use with this project
  [default: quickstart]:
  > quickstart
  Checking Gel versions...
  Specify the version of Gel to use with this project [default: x.x]:
  > x.x
  Specify branch name: [default: main]:
  > main
  ┌─────────────────────┬───────────────────────────────────────────────┐
  │ Project directory   │ ~/path/to/quickstart                          │
  │ Project config      │ ~/path/to/quickstart/gel.toml                 │
  │ Schema dir (empty)  │ ~/path/to/quickstart/dbschema                 │
  │ Installation method │ portable package                              │
  │ Version             │ x.x+cc4f3b5                                   │
  │ Instance name       │ quickstart                                    │
  └─────────────────────┴───────────────────────────────────────────────┘
  Downloading package...
  00:00:01 [====================] 41.40 MiB/41.40 MiB 32.89MiB/s | ETA: 0s
  Successfully installed x.x+cc4f3b5
  Initializing Gel instance...
  Applying migrations...
  Everything is up to date. Revision initial
  Project initialized.
  To connect to quickstart, run `gel`


This did a couple things.

1. First, it scaffolded your project by creating an
   :ref:`ref_reference_gel_toml` config file and a schema file
   :dotgel:`dbschema/default`. In the next section, you'll define a schema in
   :dotgel:`default`.

2. Second, it spun up an Gel instance called ``quickstart`` and "linked" it
   to the current directory. As long as you're inside the project
   directory, all CLI commands will be executed against this
   instance. For more details on how Gel projects work, check out the
   :ref:`Managing instances <ref_intro_instances>` guide.

.. note::

  Quick note! You can have several **instances** of Gel running on your
  computer simultaneously. Each instance may be **branched** many times. Each
  branch may have an independent schema consisting of a number of **modules**
  (though commonly your schema will be entirely defined inside the ``default``
  module).

Let's connect to our new instance! Run |gelcmd| in your terminal to open an
interactive REPL to your instance. You're now connected to a live Gel
instance running on your computer! Try executing a simple query (``select 1 + 1;``) after the
REPL prompt (``quickstart:main>``):

.. code-block:: edgeql-repl

  quickstart:main> select 1 + 1;
  {2}

Run ``\q`` to exit the REPL. More interesting queries are coming soon,
promise! But first we need to set up a schema.

.. _ref_quickstart_createdb_sdl:

3. Set up your schema
=====================

Open the ``quickstart`` directory in your IDE or editor of choice. You should
see the following file structure.

.. code-block::

  /path/to/quickstart
  ├── gel.toml
  ├── dbschema
  │   ├── default.gel
  │   ├── migrations

|Gel| schemas are defined with a dedicated schema definition language called
(predictably) Gel SDL (or just **SDL** for short). It's an elegant,
declarative way to define your data model.

SDL lives inside |.gel| files. Commonly, your entire schema will be
declared in a file called :dotgel:`default` but you can split your schema
across several |.gel| files if you prefer.

.. note::

  Syntax-highlighter packages/extensions for |.gel| files are available
  for
  `Visual Studio Code <https://marketplace.visualstudio.com/
  itemdetails?itemName=magicstack.edgedb>`_,
  `Sublime Text <https://packagecontrol.io/packages/EdgeDB>`_,
  `Atom <https://atom.io/packages/edgedb>`_,
  and `Vim <https://github.com/geldata/edgedb-vim>`_.

Let's build a simple movie database. We'll need to define two **object types**
(equivalent to a *table* in SQL): Movie and Person. Open
:dotgel:`dbschema/default` in your editor of choice and paste the following:

.. code-block:: sdl

    module default {
      type Person {
        required name: str;
      }

      type Movie {
        title: str;
        multi actors: Person;
      }
    };


A few things to note here.

- Our types don't contain an ``id`` property; Gel automatically
  creates this property and assigns a unique UUID to every object inserted
  into the database.
- The ``Movie`` type includes a **link** named ``actors``. In Gel, links are
  used to represent relationships between object types. They eliminate the need
  for foreign keys; later, you'll see just how easy it is to write "deep"
  queries without JOINs.
- The object types are inside a ``module`` called ``default``. You can split
  up your schema into logical subunits called modules, though it's common to
  define the entire schema in a single module called ``default``.

Now we're ready to run a migration to apply this schema to the database.

4. Run a migration
==================

Generate a migration file with :gelcmd:`migration create`. This command
gathers up our :dotgel:`*` files and sends them to the database. The *database
itself* parses these files, compares them against its current schema, and
generates a migration plan! Then the database sends this plan back to the CLI,
which creates a migration file.

.. code-block:: bash

  $ gel migration create
  Created ./dbschema/migrations/00001.edgeql (id: <hash>)

.. note::

  If you're interested, open this migration file to see what's inside! It's
  a simple EdgeQL script consisting of :ref:`DDL <ref_eql_sdl>` commands like
  ``create type``, ``alter type``, and ``create property``.

The migration file has been *created* but we haven't *applied it* against the
database. Let's do that.

.. code-block:: bash

  $ gel migrate
  Applied m1k54jubcs62wlzfebn3pxwwngajvlbf6c6qfslsuagkylg2fzv2lq (00001.edgeql)

Looking good! Let's make sure that worked by running :gelcmd:`list types` on
the command line. This will print a table containing all currently-defined
object types.

.. code-block:: bash

  $ gel list types
  ┌─────────────────┬──────────────────────────────┐
  │      Name       │          Extending           │
  ├─────────────────┼──────────────────────────────┤
  │ default::Movie  │ std::BaseObject, std::Object │
  │ default::Person │ std::BaseObject, std::Object │
  └─────────────────┴──────────────────────────────┘


.. _ref_quickstart_migrations:

.. _Migrate your schema:

Before we proceed, let's try making a small change to our schema: making the
``title`` property of ``Movie`` required. First, update the schema file:

.. code-block:: sdl-diff

        type Movie {
    -     title: str;
    +     required title: str;
          multi actors: Person;
        }

Then create another migration. Because this isn't the initial migration, we
see something a little different than before.

.. code-block:: bash

  $ gel migration create
  did you make property 'title' of object type 'default::Movie'
  required? [y,n,l,c,b,s,q,?]
  >

As before, Gel parses the schema files and compared them against its
current internal schema. It correctly detects the change we made, and prompts
us to confirm it. This interactive process lets you sanity check every change
and provide guidance when a migration is ambiguous (e.g. when a property is
renamed).

Enter ``y`` to confirm the change.

.. code-block:: bash

  $ gel migration create
  did you make property 'title' of object type 'default::Movie'
  required? [y,n,l,c,b,s,q,?]
  > y
  Please specify an expression to populate existing objects in
  order to make property 'title' of object type 'default::Movie' required:
  fill_expr> <std::str>{}

Hm, now we're seeing another prompt. Because ``title`` is changing from
*optional* to *required*, Gel is asking us what to do for all the ``Movie``
objects that don't currently have a value for ``title`` defined. We'll just
specify a placeholder value of "Untitled". Replace the ``<std::str>{}`` value
with ``"Untitled"`` and press Enter.

.. code-block::

  fill_expr> "Untitled"
  Created dbschema/migrations/00002.edgeql (id: <hash>)


If we look at the generated migration file, we see it contains the following
lines:

.. code-block:: edgeql

  ALTER TYPE default::Movie {
    ALTER PROPERTY title {
      SET REQUIRED USING ('Untitled');
    };
  };

Let's wrap up by applying the new migration.

.. code-block:: bash

  $ gel migrate
  Applied m1rd2ikgwdtlj5ws7ll6rwzvyiui2xbrkzig4adsvwy2sje7kxeh3a (00002.edgeql)

.. _ref_quickstart_insert_data:

.. _Insert data:

.. _Run some queries:

5. Write some queries
=====================

Let's write some simple queries via *Gel UI*, the admin dashboard baked
into every Gel instance. To open the dashboard:

.. code-block:: bash

  $ gel ui
  Opening URL in browser:
  http://localhost:107xx/ui?authToken=<jwt token>

You should see a simple landing page, as below. You'll see a card for each
branch of your instance. Remember: each instance can be branched multiple
times!

.. image:: images/ui_landing.jpg
  :width: 100%

Currently, there's only one branch, which is simply called |main| by
default. Click the |main| card.

.. image:: images/ui_db.jpg
  :width: 100%

Then click ``Open Editor`` so we can start writing some queries. We'll start
simple: ``select "Hello world!";``. Click ``RUN`` to execute the query.

.. image:: images/ui_hello.jpg
    :width: 100%

The result of the query will appear on the right.

The query will also be added to your history of previous queries, which can be
accessed via the "HISTORY" tab located on the lower left side of the editor.

Now let's actually ``insert`` an object into our database. Copy the following
query into the query textarea and hit ``Run``.

.. code-block:: edgeql

  insert Movie {
    title := "Dune"
  };

Nice! You've officially inserted the first object into your database! Let's
add a couple cast members with an ``update`` query.

.. code-block:: edgeql

  update Movie
  filter .title = "Dune"
  set {
    actors := {
      (insert Person { name := "Timothee Chalamet" }),
      (insert Person { name := "Zendaya" })
    }
  };

Finally, we can run a ``select`` query to fetch all the data we just inserted.

.. code-block:: edgeql

  select Movie {
    title,
    actors: {
      name
    }
  };

Click the outermost ``COPY`` button in the top right of the query result area
to copy the result of this query to your clipboard as JSON. The copied text
will look something like this:

.. code-block:: json

  [
    {
      "title": "Dune",
      "actors": [
        {
          "name": "Timothee Chalamet"
        },
        {
          "name": "Zendaya"
        }
      ]
    }
  ]

|Gel| UI is a useful development tool, but in practice your application will
likely be using one of Gel's *client libraries* to execute queries. Gel
provides official libraries for many langauges:

- :ref:`JavaScript/TypeScript <gel-js-intro>`
- :ref:`Go <gel-go-intro>`
- :ref:`Python <gel-python-intro>`

.. XXX: link to third-party doc websites
.. - :ref:`Rust <ref_rust_index>`
.. - :ref:`C# and F# <edgedb-dotnet-intro>`
.. - :ref:`Java <edgedb-java-intro>`
.. - :ref:`Dart <edgedb-dart-intro>`
.. - :ref:`Elixir <edgedb-elixir-intro>`

Check out the :ref:`Clients <ref_intro_clients>` guide to get
started with the language of your choice.

.. _ref_quickstart_onwards:

.. _Computeds:

Onwards and upwards
===================

You now know the basics of Gel! You've installed the CLI and database, set
up a local project, run a couple migrations, inserted and queried some data,
and used a client library.

- For a more in-depth exploration of each topic covered here, continue reading
  the other pages in the Getting Started section, which will cover important
  topics like migrations, the schema language, and EdgeQL in greater detail.

.. XXX:
.. - For guided tours of major concepts, check out the showcase pages for `Data
..   Modeling <https://www.geldata.com/showcase/data-modeling>`_, `EdgeQL
..   <https://www.geldata.com/showcase/edgeql>`_, and `Migrations
..   <https://www.geldata.com/showcase/migrations>`_.

- To start building an application using the language of your choice, check out
  our client libraries:

  - :ref:`JavaScript/TypeScript <gel-js-intro>`
  - :ref:`Go <gel-go-intro>`
  - :ref:`Python <gel-python-intro>`
