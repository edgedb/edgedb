.. _ref_quickstart:

==========
Quickstart
==========

Welcome to EdgeDB!

This quickstart will walk you through the entire process of creating a simple
EdgeDB-powered application: installation, defining your schema, adding some
data, and writing your first query. Let's jump in!

..    :ref:`Installation <ref_quickstart_install>`
..    :ref:`Initialize a project <ref_quickstart_createdb>`
..    :ref:`Set up your schema <ref_quickstart_createdb_sdl>`
..    :ref:`Insert data <ref_quickstart_insert_data>`
..    :ref:`Run some queries <ref_quickstart_queries>`
..    :ref:`Migrate your schema <ref_quickstart_migrations>`
..    :ref:`Computables <ref_quickstart_computables>`
..    :ref:`Onwards and upwards <ref_quickstart_onwards>`

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

    $ mkdir edgedb-quickstart
    $ cd edgedb-quickstart

Then initialize your EdgeDB project:

.. code-block:: bash

    $ edgedb project init

This starts an interactive tool that walks you through the process of setting
up your first EdgeDB instance. You should see something like this:

.. code-block:: bash

    $ edgedb project init

    No `edgedb.toml` found at `~/path/to/edgedb-quickstart`
    or above. Do you want to initialize a new project? [Y/n]
    > Y

    Specify the name of EdgeDB instance to use with this
    project [default: edgedb_quickstart]:
    > edgedb_quickstart

    How would you like to run EdgeDB for this project?
    1. Local (native package)
    2. Docker
    Type a number to choose an option:
    > 1

    Checking EdgeDB versions...
    Specify the version of EdgeDB to use with this project
    [default: 1-beta3]:
    > 1-beta3
    ┌─────────────────────┬───────────────────────────────────────────────┐
    │ Project directory   │ ~/path/to/edgedb-quickstart                   │
    │ Project config      │ ~/path/to/edgedb-quickstart/edgedb.toml       │
    │ Schema dir (empty)  │ ~/path/to/edgedb-quickstart/dbschema          │
    │ Installation method │ Native System Package                         │
    │ Version             │ 1.0b2+ga7130d5c7.cv202104290000-202105060205  │
    │ Instance name       │ edgedb_quickstart                             │
    └─────────────────────┴───────────────────────────────────────────────┘
    Initializing EdgeDB instance...
    Bootstrap complete. Server is up and running now.
    Project initialialized.
    To connect to edgedb_quickstart, just run `edgedb`.


This did a couple things.

First, it scaffolded your project by creating an ``edgedb.toml`` config file
and a schema file ``dbschema/default.esdl``. In the next section, you'll
define your schema in ``default.esdl``.

Second, it spun up an EdgeDB instance called ``edgedb-quickstart`` (unless you
overrode this with a different name). As long as you're inside the project
directory all ``edgedb`` CLI
commands will be executed against this instance. For more details on how
EdgeDB projects work, check out
`this blog post </blog/introducing-edgedb-projects>`_.

.. note::

    Quick note! You can have several "instances" of EdgeDB running on your
    computer simultaneously. Each instance contains several "databases". Each
    database may contain several "schema modules" (though commonly your schema
    will be entirely defined inside the ``default`` module).

Let's give it a try! Run ``edgedb`` in your terminal. This will connect to
your database and open a REPL. You're now connected to a live EdgeDB instance
running on your computer! Try executing a simple query:

.. code-block:: edgeql-repl

    edgedb> SELECT 1 + 1;
    {2}

Run ``\q`` to exit the REPL. More interesting queries are coming soon,
promise! But first we need to set up a schema.

.. _ref_quickstart_createdb_sdl:

3. Set up your schema
=====================

With EdgeDB, you define your schema with EdgeDB's dedicated schema description
language (SDL). It's an elegant, declarative way to define your data model. By
convention, you write your schema inside a file called ``default.esdl`` inside
a ``dbschema`` folder in your project directory. Alternatively you can split
your schema across several ``.esdl`` files; the filenames don't matter.


.. note::

    Syntax-highlighter packages/extensions for ``.esdl`` files are available
    for
    `Visual Studio Code <https://marketplace.visualstudio.com/
    itemdetails?itemName=magicstack.edgedb>`_,
    `Sublime Text <https://packagecontrol.io/packages/EdgeDB>`_,
    `Atom <https://atom.io/packages/edgedb>`_,
    and `Vim <https://github.com/edgedb/edgedb-vim>`_.

Let's build a simple movie database. We'll need to define two **object types**
(equivalent to table in SQL): Movie and Person. Open ``dbschema/default.esdl``
in your editor of choice and paste the following:

.. code-block:: sdl

    module default {
        type Person {
            required property first_name -> str;
            required property last_name -> str;
        }

        type Movie {
            required property title -> str;
            property year -> int64;
            required link director -> Person;
            multi link actors -> Person;
        }
    };

Our ``Person`` schema just contains two string properties, ``first_name`` and
``last_name`` (both required). Our ``Movie`` schema contains a string property
``title`` and an optional integer property ``year``. EdgeDB automatically
assigns a unique ``id`` property to every object inserted into the database
— no need to define it manually.

The ``Movie`` type also includes two ``links``. In EdgeDB, links are used to
represent relationships between object types. They entirely abstract away the
concept of foreign keys. Later, you'll see just how easy it is to write "deep"
queries that include relations — no JOINs required!

Now we're ready to run a migration to apply this schema to the database.

Generate the migration
----------------------

First, we generate a migration file with ``edgedb migration create``. This
starts an interactive tool that asks a series of questions. Pay attention to
these questions to make sure you aren't making any unintended changes.

.. code-block:: bash

    $ edgedb migration create
    did you create object type 'default::Person'? [y,n,l,c,b,s,q,?]
    > y
    did you create object type 'default::Movie'? [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00001.edgeql, id: m1la5u4qi...

For now, just type ``y`` to confirm each change. But you have several other
options too:

.. code-block::

    y - confirm the prompt, use the DDL statements
    n - reject the prompt
    l - list the DDL statements associated with prompt
    c - list already confirmed EdgeQL statements
    b - revert back to previous save point, perhaps previous question
    s - stop and save changes (splits migration into multiple)
    q - quit without saving changes
    h or ? - print help

Using these options, you can introspect the DDL (data definition language)
commands associated with the change, split up the updates into several
individual migrations, revisit earlier questions, or exit the tool.

When you complete the questions, a ``.edgeql`` migration file we be generated
in the ``dbschema/migrations`` directory!

.. note::

    If you're interested, open this migration file to see what's inside! It's
    a simple EdgeQL script consisting of DDL commands like ``CREATE TYPE``,
    ``ALTER TYPE``, and ``CREATE PROPERTY``. When you generate migrations,
    EdgeDB reads your declared ``.esdl`` schema and generates a sequence of
    DDL commands that bring the instance into agreement with it.


Execute the migration
---------------------

Let's apply the migration:

.. code-block:: bash

    $ edgedb migrate
    Applied m1la5u4qi... (00001.edgeql)

.. note::

    Each EdgeDB instance can contain multiple databases! When an instance is
    created, an initial database called ``edgedb`` is automatically created.
    This is the instance against which all CLI commands are executed by
    default.

    To use a non-default database, first create it with ``edgedb
    create-database my-database``. Then use the ``-d`` flag to tell the CLI
    which instance to run against:

    .. code-block:: bash

        $ edgedb -d my-database migrate

Let's make sure that worked. Run ``edgedb list-object-types`` to re-open the
REPL. Then run the special ``\lt`` command to list all object types.

.. code-block::

    $ edgedb
    edgedb> \lt
    ┌─────────────────┬──────────────────────────────┐
    │      Name       │          Extending           │
    ├─────────────────┼──────────────────────────────┤
    │ default::Movie  │ std::BaseObject, std::Object │
    │ default::Person │ std::BaseObject, std::Object │
    └─────────────────┴──────────────────────────────┘

Looking good! Now let's add some data to the database!

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

Now, let's add Ryan Gosling to the database with a simple EdgeQL query:

.. code-block:: edgeql-repl

    edgedb> INSERT Person {
    .......     first_name := 'Ryan',
    .......     last_name := 'Gosling',
    ....... };
    {default::Person {id: 86d0eb18-b7ff-11eb-ba80-7b8e9facf817}}

That was easy!

.. note::

    By convention, we're using all-caps to indicate EdgeQL keywords, but
    EdgeQL isn't case sensitive; if you want, you can use ``insert`` (or
    ``InSeRt``) instead of ``INSERT``.

As you can see, EdgeQL differs from SQL in some important ways. It uses curly
braces and the assignment operator (``:=``) to make queries **explicit** and
**intuitive** for the people who write them: programmers. It's also completely
**composable**, so it's possible to add a movie, its director, and its actors
simultaneously:

.. code-block:: edgeql-repl

    edgedb> INSERT Movie {
    .......     title := 'Blade Runner 2049',
    .......     year := 2017,
    .......     director := (
    .......         INSERT Person {
    .......             first_name := 'Denis',
    .......             last_name := 'Villeneuve',
    .......         }
    .......     ),
    .......     actors := {
    .......         (INSERT Person {
    .......             first_name := 'Harrison',
    .......             last_name := 'Ford',
    .......         }),
    .......         (INSERT Person {
    .......             first_name := 'Ana',
    .......             last_name := 'de Armas',
    .......         }),
    .......     }
    ....... };
    {default::Movie {id: 4d0c8ddc-54d4-11e9-8c54-7776f6130e05}}

.. note::

    The specific ``id`` values will differ from the ones
    above. They are shown explicitly here for demonstration purposes.

As you can see, it's easy to nest :ref:`INSERT <ref_eql_statements_insert>`
subqueries inside each other. Now lets add Ryan Gosling to the cast with an
:ref:`UPDATE <ref_eql_statements_update>`:

.. code-block:: edgeql-repl

    edgedb> UPDATE Movie
    ....... FILTER .title = 'Blade Runner 2049'
    ....... SET {
    .......     actors += (
    .......         SELECT Person
    .......         FILTER .id = <uuid>'86d0eb18-b7ff-11eb-ba80-7b8e9facf817'
    .......     )
    ....... };
    {default::Movie {id: 64d024dc-54d5-11e9-8c54-a3f59e1d995e}}


This query also uses a subquery to fetch Ryan Gosling and add him to the cast
of Blade Runner 2049 using the ``+=`` operator. You could also remove a cast
member with ``-=``.

Our database is still a little sparse. Let's add another movie directed by
Denis Villeneuve: "Dune".

.. code-block:: edgeql-repl

    edgedb> INSERT Movie {
    .......     title := 'Dune',
    .......     director := (
    .......         SELECT Person
    .......         FILTER
    .......             .first_name = 'Denis' AND
    .......             .last_name = 'Villeneuve'
    .......         # the LIMIT is needed to satisfy the single
    .......         # link requirement validation
    .......         LIMIT 1
    .......     )
    ....... };
    {default::Movie {id: 64d024dc-54d5-11e9-8c54-a3f59e1d995e}}

We have to use ``LIMIT 1`` for this query to be valid. In EdgeDB, the result
of a query is a **set** (in the "set theory" sense). Since we're assigning to
``Movie.director`` (a singular/"to-one" relation) , we need to provide a
guarantee that our query set will only contain a single element. To do that we
need to either 1) use ``LIMIT 1`` or 2) ``FILTER`` by ``.id`` (or another
property with a uniqueness constraint).

.. _ref_quickstart_queries:

5. Run some queries
===================

Let's write some basic queries:

.. code-block:: edgeql-repl

    edgedb> SELECT Movie;
    {
      default::Movie {id: 4d0c8ddc-54d4-11e9-8c54-7776f6130e05},
      default::Movie {id: 64d024dc-54d5-11e9-8c54-a3f59e1d995e}
    }

The above query simply returned all the ``Movie`` objects in the database. By
default, only the ``id`` property is returned for each result. To select more
properties, we add a :ref:`shape <ref_eql_expr_shapes>`:

.. code-block:: edgeql-repl

    edgedb> SELECT Movie {
    .......     title,
    .......     year
    ....... };
    {
      default::Movie {title: 'Blade Runner 2049', year: 2017},
      default::Movie {title: 'Dune', year: {}},
    }

This time, the results contain ``title`` and ``year`` as requested in
the query shape. Note that the ``year`` for Dune is given as ``{}`` (the
empty set). This is the equivalent of a ``NULL`` value in SQL.

Let's narrow down the ``Movie`` search to "blade runner" using
:eql:op:`ILIKE` (case-insensitive pattern matching). With the %
at the end, anything after ``blade runner`` will match: "Blade Runner",
"Blade Runner 2049", "BlAdE RUnnEr 2: Electric Boogaloo", etc....

.. code-block:: edgeql-repl

    edgedb> SELECT Movie {
    .......     title,
    .......     year
    ....... }
    ....... FILTER .title ILIKE 'blade runner%';
    {default::Movie {title: 'Blade Runner 2049', year: 2017}}

Let's get more details about the ``Movie``:

.. code-block:: edgeql-repl

    edgedb> SELECT Movie {
    .......     title,
    .......     year,
    .......     director: {
    .......         first_name,
    .......         last_name
    .......     },
    .......     actors: {
    .......         first_name,
    .......         last_name
    .......     }
    ....... }
    ....... FILTER .title ILIKE 'blade runner%';
    {
      default::Movie {
        title: 'Blade Runner 2049',
        year: 2017,
        director: default::Person {
          first_name: 'Denis',
          last_name: 'Villeneuve'
        },
        actors: {
          default::Person {
            first_name: 'Harrison',
            last_name: 'Ford'
          },
          default::Person {
            first_name: 'Ryan',
            last_name: 'Gosling'
          },
          default::Person {
            first_name: 'Ana',
            last_name: 'de Armas',
          },
        },
      },
    }


.. _ref_quickstart_migrations:

6. Migrate your schema
======================

Let's add some more information about "Dune". For example, we can add
some of the actors, like Jason Momoa, Zendaya, and Oscar Isaac:

.. code-block:: edgeql-repl

    edgedb> INSERT Person {
    .......     first_name := 'Jason',
    .......     last_name := 'Momoa'
    ....... };
    default::Person {id: 618d4cd6-54db-11e9-8c54-67c38dbbba18}
    edgedb> INSERT Person {
    .......     first_name := 'Oscar',
    .......     last_name := 'Isaac'
    ....... };
    default::Person {id: 618d5a64-54db-11e9-8c54-9393cfcd9598}
    edgedb> INSERT Person { first_name := 'Zendaya'}
    ERROR: MissingRequiredError: missing value for required property
    'last_name' of object type 'default::Person'

Unfortunately, adding Zendaya isn't possible with the current schema
since both ``first_name`` and ``last_name`` are required. So let's
migrate our schema to make ``last_name`` optional.

First, we'll update the ``dbschema/schema.esdl``:

.. code-block:: sdl

    module default {
        type Person {
            required property first_name -> str;

            # delete "required"
            property last_name -> str;
        }
        type Movie {
            required property title -> str;
            property year -> int64; # the year of release
            required link director -> Person;
            multi link actors -> Person;
        }
    };

Then we'll create a new migration and apply it:

.. code-block:: bash

    $ edgedb migration create
    did you make property 'last_name' of object type
    'default::Person' optional? [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00002.edgeql, id: m1k62y4x...

    $ edgedb migrate
    Applied m1k62y4x... (00002.edgeql)

Now back in our REPL we can add Zendaya:

.. code-block:: edgeql-repl

    edgeql> INSERT Person {
    .......     first_name := 'Zendaya'
    ....... };
    {default::Person {id: 65fce84c-54dd-11e9-8c54-5f000ca496c9}}

.. _ref_quickstart_computables:

7. Computables
==============

Now that last names are optional, we may want an easy way to retrieve the full
name for a given Person. We'll do this with a :ref:`computable property
<ref_datamodel_computables>`:

.. code-block:: edgeql-repl

    edgedb> SELECT Person {
    .......     full_name := .first_name ++ ' ' ++ .last_name
    .......       IF EXISTS .last_name
    .......       ELSE .first_name
    ....... };
    {
      default::Person {full_name: 'Zendaya'},
      default::Person {full_name: 'Harrison Ford'},
      default::Person {full_name: 'Ryan Gosling'},
      ...
    }

Let's say we're planning to use ``full_name`` a lot. Instead of re-defining it
in each query, we can add it directly to the schema alongside the other
properties of ``Person``. Let's update ``dbschema/default.esdl``:

.. code-block:: sdl

    module default {
        type Person {
            required property first_name -> str;
            property last_name -> str;

            # add computable property "name"
            property full_name :=
                .first_name ++ ' ' ++ .last_name
                IF EXISTS .last_name
                ELSE .first_name;
        }
        type Movie {
            required property title -> str;
            property year -> int64; # the year of release
            required link director -> Person;
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

    edgeql> SELECT Movie {
    .......     title,
    .......     year,
    .......     director: { full_name },
    .......     actors: { full_name }
    ....... }
    ....... FILTER .title = 'Dune';
    {
        default::Movie {
            title: 'Dune',
            year: {},
            director: default::Person {name: 'Denis Villeneuve'},
            actors: {
                default::Person {name: 'Jason Momoa'},
                default::Person {name: 'Zendaya'},
                default::Person {name: 'Oscar Isaac'},
            }
        }
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

- Or just jump into the `docs </docs>`_!
