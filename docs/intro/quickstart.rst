.. _ref_quickstart:

==========
Quickstart
==========

Welcome to EdgeDB!

This quickstart will walk you through the entire process of creating a simple
EdgeDB-powered application: installation, defining your schema, adding some
data, and writing your first query. Let's jump in!

.. _ref_quickstart_install:

1. Installation
===============

First let's install the EdgeDB CLI. Open a terminal and run the appropriate
command below.

**macOS/Linux**

.. code-block:: bash

  $ curl https://sh.edgedb.com --proto '=https' -sSf1 | sh

**Windows**

.. code-block::

  # in Powershell
  PS> iwr https://ps1.edgedb.com -useb | iex

This command downloads and executes a bash script that installs the ``edgedb``
CLI on your machine. You may be asked for your password. Once the installation
completes, **restart your terminal** so the ``edgedb`` command becomes
available.

Now let's set up your EdgeDB project.

.. _ref_quickstart_createdb:

2. Initialize a project
=======================

In a terminal, create a new directory and ``cd`` into it.

.. code-block:: bash

  $ mkdir quickstart
  $ cd quickstart

Then initialize your EdgeDB project:

.. code-block:: bash

  $ edgedb project init

This starts an interactive tool that walks you through the process of setting
up your first EdgeDB instance. You should see something like this:

.. code-block:: bash

  $ edgedb project init
  No `edgedb.toml` found in `/path/to/quickstart` or above
  Do you want to initialize a new project? [Y/n]
  > Y
  Specify the name of EdgeDB instance to use with this project [quickstart]:
  > quickstart
  Checking EdgeDB versions...
  Specify the version of EdgeDB to use with this project [default: 2.0-rc.3]:
  > 2.0
  ┌─────────────────────┬───────────────────────────────────────────────┐
  │ Project directory   │ ~/path/to/quickstart                          │
  │ Project config      │ ~/path/to/quickstart/edgedb.toml              │
  │ Schema dir (empty)  │ ~/path/to/quickstart/dbschema                 │
  │ Installation method │ portable package                              │
  │ Version             │ 2.0+c21decd                                   │
  │ Instance name       │ quickstart                                    │
  └─────────────────────┴───────────────────────────────────────────────┘
  Downloading package...
  00:00:01 [====================] 32.98MiB/32.98MiB 32.89MiB/s | ETA: 0s
  Successfully installed 2.0+c21decd
  Initializing EdgeDB instance...
  Applying migrations...
  Everything is up to date. Revision initial
  Project initialized.
  To connect to quickstart, run `edgedb`


This did a couple things.

1. First, it scaffolded your project by creating an ``edgedb.toml`` config
   file and a schema file ``dbschema/default.esdl``. In the next section,
   you'll define your schema in ``default.esdl``.

2. Second, it spun up an EdgeDB instance called ``quickstart`` (unless you
   overrode this with a different name). As long as you're inside the project
   directory all ``edgedb`` CLI commands will be executed against this
   instance. For more details on how EdgeDB projects work, check out the
   :ref:`Using projects <ref_guide_using_projects>` guide.

.. note::

  Quick note! You can have several **instances** of EdgeDB running on your
  computer simultaneously. Each instance contains several **databases**. Each
  database may contain several **modules** (though commonly your schema
  will be entirely defined inside the ``default`` module).

Let's give it a try! Run ``edgedb`` in your terminal. This will connect to
your database and open a REPL. You're now connected to a live EdgeDB instance
running on your computer! Try executing a simple query:

.. code-block:: edgeql-repl

  db> select 1 + 1;
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
  ├── edgedb.toml
  ├── dbschema
  │   ├── default.esdl
  │   ├── migrations

EdgeDB schemas are defined with a dedicated schema description language called
(predictably) EdgeDB SDL (or just **SDL** for short). It's an elegant,
declarative way to define your data model. SDL lives inside ``.esdl`` files.
Commonly, your entire schema will be declared in a file called
``default.esdl`` but you can split your schema across several ``.esdl`` files;
the filenames don't matter.

.. note::

  Syntax-highlighter packages/extensions for ``.esdl`` files are available
  for
  `Visual Studio Code <https://marketplace.visualstudio.com/
  itemdetails?itemName=magicstack.edgedb>`_,
  `Sublime Text <https://packagecontrol.io/packages/EdgeDB>`_,
  `Atom <https://atom.io/packages/edgedb>`_,
  and `Vim <https://github.com/edgedb/edgedb-vim>`_.

Let's build a simple movie database. We'll need to define two **object types**
(equivalent to a *table* in SQL): Movie and Person. Open
``dbschema/default.esdl`` in your editor of choice and paste the following:

.. code-block:: sdl

  module default {
    type Person {
      required property first_name -> str;
      required property last_name -> str;
    }

    type Movie {
      required property title -> str;
      property release_year -> int64;
      multi link actors -> Person;
      link director -> Person;
    }
  };

A few things to note here.

- Our types don't contain an ``id`` property; EdgeDB automatically
  creates this property and assigned a unique UUID to every object inserted
  into the database.
- The ``Movie`` type also includes two **links**. In EdgeDB, links are used to
  represent relationships between object types. They entirely abstract away the
  concept of foreign keys; later, you'll see just how easy it is to write
  "deep" queries without JOINs.

Now we're ready to run a migration to apply this schema to the database.

Generate the migration
----------------------

First, we generate a migration file with ``edgedb migration create``. This
starts an interactive tool that asks a series of questions. Pay attention to
these questions to make sure you aren't making any unintended changes.

.. code-block:: bash

  $ edgedb migration create
  Created ./dbschema/migrations/00001.edgeql, id: m1la5u4qi...

This creates an ``.edgeql`` migration file in the ``dbschema/migrations``
directory.

.. note::

  If you're interested, open this migration file to see what's inside! It's
  a simple EdgeQL script consisting of :ref:`DDL <ref_eql_sdl>` commands like
  ``create type``, ``alter type``, and ``create property``. When you generate
  migrations, EdgeDB reads your declared ``.esdl`` schema and generates a
  migration path.


Execute the migration
---------------------

Let's apply the migration:

.. code-block:: bash

  $ edgedb migrate
  Applied m1la5u4qi... (00001.edgeql)
  Note: adding first migration disables DDL.

Let's make sure that worked. Run ``edgedb list types`` to view all
currently-defined object types.

.. code-block:: bash

  $ edgedb list types
  ┌─────────────────┬──────────────────────────────┐
  │      Name       │          Extending           │
  ├─────────────────┼──────────────────────────────┤
  │ default::Movie  │ std::BaseObject, std::Object │
  │ default::Person │ std::BaseObject, std::Object │
  └─────────────────┴──────────────────────────────┘

Looking good! Now let's add some data to the database.

.. _ref_quickstart_insert_data:

4. Insert data
==============

For this tutorial we'll just use the REPL tool to execute queries. In
practice, you'll probably be using one of EdgeDB's client libraries for
`JavaScript/TypeScript <https://github.com/edgedb/edgedb-js>`__,
`Go <https://github.com/edgedb/edgedb-go>`__,
or `Python <https://github.com/edgedb/edgedb-python>`__.

Open the REPL:

.. code-block:: bash

  $ edgedb

Inserting objects
-----------------

Now, let's add Denis Villeneuve to the database with a simple EdgeQL query:

.. code-block:: edgeql-repl

  db> insert Person {
  ...     first_name := 'Denis',
  ...     last_name := 'Villeneuve',
  ... };
  {default::Person {id: 86d0eb18-b7ff-11eb-ba80-7b8e9facf817}}

As you can see, EdgeQL differs from SQL in some important ways. It
uses curly braces and the assignment operator (``:=``) to make queries
**explicit** and **intuitive** for the people who write them: programmers.
It's also completely **composable**, so subqueries are easy; let's try a
nested insert.

The query below contains a :ref:`query parameter <ref_eql_params>`
``$director_id``. After executing the query in the REPL, we'll be prompted to
provide a value for it. Copy and paste the UUID for Denis Villeneuve from the
previous query.

.. code-block:: edgeql-repl

  db> with director_id := <uuid>$director_id
  ... insert Movie {
  ...   title := 'Blade Runnr 2049', # typo is intentional 🙃
  ...   release_year := 2017,
  ...   director := (
  ...     select Person
  ...     filter .id = director_id
  ...   ),
  ...   actors := {
  ...     (insert Person {
  ...       first_name := 'Harrison',
  ...       last_name := 'Ford',
  ...     }),
  ...     (insert Person {
  ...       first_name := 'Ana',
  ...       last_name := 'de Armas',
  ...     }),
  ...   }
  ... };
  Parameter <uuid>$director_id: 86d0eb18-b7ff-11eb-ba80-7b8e9facf817
  {default::Movie {id: 4d0c8ddc-54d4-11e9-8c54-7776f6130e05}}

Updating objects
----------------

Oops, we misspelled "Runner". Let's fix that with an :ref:`update
<ref_eql_update>` query.

.. code-block:: edgeql-repl

  db> update Movie
  ... filter .title = 'Blade Runnr 2049'
  ... set {
  ...   title := "Blade Runner 2049",
  ... };
  {default::Movie {id: 4d0c8ddc-54d4-11e9-8c54-7776f6130e05}}

Now for something a little more interesting; let's add Ryan Gosling to the
cast.

.. code-block:: edgeql-repl

  db> update Movie
  ... filter .title = 'Blade Runner 2049'
  ... set {
  ...   actors += (
  ...     insert Person {
  ...       first_name := "Ryan",
  ...       last_name := "Gosling"
  ...     }
  ...   )
  ... };
  {default::Movie {id: 4d0c8ddc-54d4-11e9-8c54-7776f6130e05}}

This query uses the ``+=`` operator to assign an additional item to the
``actors`` link without overwriting the existing contents. By contrast, ``-=``
unlinks elements and ``:=`` overwrites the link entirely.

Our database is still a little sparse. Let's quickly add a couple more movies.

.. code-block:: edgeql-repl

  db> insert Movie { title := "Dune" };
  {default::Movie {id: 64d024dc-54d5-11e9-8c54-a3f59e1d995e}}
  db> insert Movie {
  ...   title := "Arrival",
  ...   release_year := 2016
  ... };
  {default::Movie {id: ca69776e-40df-11ec-b1b8-b7c909ac034a}}

.. _ref_quickstart_queries:

5. Run some queries
===================

Let's write some basic queries:

.. code-block:: edgeql-repl

  db> select Movie;
  {
    default::Movie {id: 4d0c8ddc-54d4-11e9-8c54-7776f6130e05},
    default::Movie {id: 64d024dc-54d5-11e9-8c54-a3f59e1d995e},
    default::Movie {id: ca69776e-40df-11ec-b1b8-b7c909ac034a}
  }

This query simply returns all the ``Movie`` objects in the database. By
default, only the ``id`` property is returned for each result. To specify
which properties to select, add a :ref:`shape <ref_reference_shapes>`:

.. code-block:: edgeql-repl

  db> select Movie {
  ...   title,
  ...   release_year
  ... };
  {
    default::Movie {title: 'Blade Runner 2049', release_year: 2017},
    default::Movie {title: 'Dune', release_year: {}},
    default::Movie {title: 'Arrival', release_year: 2016}
  }

This time, the results contain ``title`` and ``release_year`` as requested in
the query **shape**. Note that the ``release_year`` for Dune is given as
``{}`` (the empty set). This is the equivalent of a ``null`` value in SQL.

Let's retrieve some information about Blade Runner 2049.

.. code-block:: edgeql-repl

  db> select Movie {
  ...   title,
  ...   release_year,
  ...   actors: {
  ...     first_name,
  ...     last_name
  ...   }
  ... }
  ... filter .title = "Blade Runner 2049";
  {
    default::Movie {
      title: 'Blade Runner 2049',
      release_year: 2017,
      director: default::Person {first_name: 'Denis', last_name: 'Villeneuve'},
      actors: {
        default::Person {first_name: 'Harrison', last_name: 'Ford'},
        default::Person {first_name: 'Ana', last_name: 'de Armas'},
        default::Person {first_name: 'Ryan', last_name: 'Gosling'},
      },
    },
  }

Nice and easy! We're able to fetch the movie and its related objects by
nesting shapes (similar to GraphQL).

.. _ref_quickstart_migrations:

6. Migrate your schema
======================

Let's add some more information about "Dune"; for starters, we'll insert
``Person`` objects for its cast members Jason Momoa, Oscar Isaac, and Zendaya.

.. code-block:: edgeql-repl

  db> insert Person {
  ...   first_name := 'Jason',
  ...   last_name := 'Momoa'
  ... };
  default::Person {id: 618d4cd6-54db-11e9-8c54-67c38dbbba18}
  db> insert Person {
  ...   first_name := 'Oscar',
  ...   last_name := 'Isaac'
  ... };
  default::Person {id: 618d5a64-54db-11e9-8c54-9393cfcd9598}
  db> insert Person { first_name := 'Zendaya'};
  ERROR: MissingRequiredError: missing value for required property
  'last_name' of object type 'default::Person'

Oh no! We can't add Zendaya with the current schema since both ``first_name``
and ``last_name`` are required. So let's migrate our schema to make
``last_name`` optional.

If necessary, close the REPL with ``\q``, then open ``dbschema/default.esdl``.

.. code-block:: sdl-diff

    module default {
      type Person {
        required property first_name -> str;
  -     required property last_name -> str;
  +     property last_name -> str;
      }
      type Movie {
        required property title -> str;
        property release_year -> int64;
        link director -> Person;
        multi link actors -> Person;
      }
    };

Then create a new migration.

.. code-block:: bash

  $ edgedb migration create
  did you make property 'last_name' of object type
  'default::Person' optional? [y,n,l,c,b,s,q,?]
  > y
  Created ./dbschema/migrations/00002.edgeql, id: m1k62y4x...

EdgeDB detects the modifications to your schema, and prompts you to confirm
each change. In this case there's only one modification. Answer ``y`` to
proceed, then apply the migration.

.. code-block:: bash

  $ edgedb migrate
  Applied m1k62y4x... (00002.edgeql)

Now run ``edgedb`` to re-open the REPL and insert Zendaya. This time, the
query works.

.. code-block:: edgeql-repl

  db> insert Person {
  ...   first_name := 'Zendaya'
  ... };
  {default::Person {id: 65fce84c-54dd-11e9-8c54-5f000ca496c9}}

.. _ref_quickstart_computeds:

7. Computeds
============

Now that last names are optional, we may want an easy way to retrieve the full
name for a given Person. We'll do this with a :ref:`computed property
<ref_datamodel_computed>`:

.. code-block:: edgeql-repl

  db> select Person {
  ...   full_name :=
  ...    .first_name ++ ' ' ++ .last_name
  ...      if exists .last_name
  ...      else .first_name
  ... };
  {
    default::Person {full_name: 'Zendaya'},
    default::Person {full_name: 'Harrison Ford'},
    default::Person {full_name: 'Ryan Gosling'},
    ...
  }

Let's say we're planning to use ``full_name`` a lot. Instead of re-defining it
in each query, we can add it directly to the schema alongside the other
properties of ``Person``. Let's update ``dbschema/default.esdl``:

.. code-block:: sdl-diff

    module default {
      type Person {
        required property first_name -> str;
        property last_name -> str;

  +     property full_name :=
  +       .first_name ++ ' ' ++ .last_name
  +       if exists .last_name
  +       else .first_name;

      }
      type Movie {
        required property title -> str;
        property release_year -> int64;
        link director -> Person;
        multi link actors -> Person;
      }
    };

Then create and run another migration:

.. code-block:: bash

  $ edgedb migration create
  did you create property 'full_name' of object type
  'default::Person'? [y,n,l,c,b,s,q,?]
  > y
  Created ./dbschema/migrations/00003.edgeql, id:
  m1gd3vxwz3oopur6ljgg7kzrin3jh65xhhjbj6de2xaou6i7owyhaq

  $ edgedb migrate
  Applied m1gd3vxwz3oopur6ljgg7kzrin3jh65xhhjbj6de2xaou6i7owyhaq
  (00003.edgeql)

Now we can easily fetch ``full_name`` just like any other property!

.. code-block:: edgeql-repl

  db> select Person {
  ...   full_name
  ... };
  {
    default::Person {full_name: 'Denis Villeneuve'},
    default::Person {full_name: 'Harrison Ford'},
    default::Person {full_name: 'Ana de Armas'},
    default::Person {full_name: 'Ryan Gosling'},
    default::Person {full_name: 'Jason Momoa'},
    default::Person {full_name: 'Oscar Isaac'},
    default::Person {full_name: 'Zendaya'},
  }


.. _ref_quickstart_onwards:

8. Onwards and upwards
======================

You now know the basics of EdgeDB! You've installed the CLI and database, set
up a local project, created an initial schema, added and queried data, and run
a schema migration.

- For guided tours of major concepts, check out the
  showcase pages for `Data Modeling </showcase/data-modeling>`_,
  `EdgeQL </showcase/edgeql>`_, and `Migrations </showcase/migrations>`_.

- For a deep dive into the EdgeQL query language, check out the
  `Interactive Tutorial </tutorial>`_.

- For an immersive, comprehensive walkthrough of EdgeDB concepts, check out
  our illustrated e-book `Easy EdgeDB </easy-edgedb>`_; it's designed to walk a
  total beginner through EdgeDB, from the basics all the way through advanced
  concepts.

- To start building an application using the language of your choice, check
  out our client libraries for
  `JavaScript/TypeScript </docs/clients/01_js/index>`__,
  `Python </docs/clients/00_python/index>`__, and
  `Go </docs/clients/02_go/index>`__.

- Or just jump into the :ref:`docs <index_toplevel>`!
