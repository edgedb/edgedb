.. _edgedb-js-intro:

===========================
EdgeDB TypeScript/JS Client
===========================

.. toctree::
   :maxdepth: 3
   :hidden:

   driver
   generation
   queries
   interfaces
   querybuilder
   literals
   types
   funcops
   parameters
   objects
   select
   insert
   update
   delete
   with
   for
   group
   reference

This is the official EdgeDB client library for JavaScript and TypeScript. It’s
the easiest way to connect to your database and execute queries from a Node.js
or Deno backend.

.. _edgedb-js-installation:


Installation
============

You can install the published database driver and optional (but recommended!)
generators from npm using your package manager of choice.

.. tabs::

    .. code-tab:: bash
      :caption: npm

      $ npm install --save-prod edgedb          # database driver
      $ npm install --save-dev @edgedb/generate # generators

    .. code-tab:: bash
      :caption: yarn

      $ yarn add edgedb                 # database driver
      $ yarn add --dev @edgedb/generate # generators

    .. code-tab:: bash
      :caption: pnpm

      $ pnpm add --save-prod edgedb          # database driver
      $ pnpm add --save-dev @edgedb/generate # generators

    .. code-tab:: typescript
      :caption: deno

      import * as edgedb from "http://deno.land/x/edgedb/mod.ts";

    .. code-tab:: bash
      :caption: bun

      $ bun add edgedb                 # database driver
      $ bun add --dev @edgedb/generate # generators

.. note:: Deno users

    Create these two files in your project root:

    .. code-block:: json
        :caption: importMap.json

        {
          "imports": {
            "edgedb": "https://deno.land/x/edgedb/mod.ts",
            "edgedb/": "https://deno.land/x/edgedb/"
          }
        }

    .. code-block:: json
        :caption: deno.js

        {
          "importMap": "./importMap.json"
        }


.. _edgedb-js-quickstart:

Quickstart
==========

Setup
^^^^^

This section assumes you have gone through the :ref:`Quickstart Guide
<ref_quickstart>` and understand how to update schemas, run migrations, and have
a working EdgeDB project. Let's update the schema to make the ``title`` property
of the ``Movie`` type exclusive. This will help with filtering by
``Movie.title`` in our queries.

.. code-block:: sdl-diff
  :caption: dbschema/default.esdl

    module default {
      type Person {
        required name: str;
      }

      type Movie {
  -     required title: str;
  +     required title: str {
  +       constraint exclusive;
  +     };
        multi actors: Person;
      }
    }

Generate the new migration and apply them:

.. code-block:: bash

  $ edgedb migration create
  $ edgedb migrate

We'll be using TypeScript and Node for this example, so let's setup a simple
app:

.. code-block:: bash

  $ npm init -y # initialize a new npm project
  $ npm i edgedb
  $ npm i -D typescript @types/node @edgedb/generate tsx
  $ npx tsc --init # initialize a basic TypeScript project

Client
^^^^^^

The ``Client`` class implements the core functionality required to establish a
connection to your database and execute queries. If you prefer writing queries
as strings, the Client API is all you need.

Let's create a simple Node.js script that seeds the database by running an
insert query directly with the driver:

.. code-block:: typescript
  :caption: seed.ts

  import * as edgedb from "edgedb";

  const client = edgedb.createClient();

  async function main() {
    await client.execute(`
      insert Person { name := "Robert Downey Jr." };
      insert Person { name := "Scarlett Johansson" };
      insert Movie {
        title := <str>$title,
        actors := (
          select Person filter .name in {
            "Robert Downey Jr.",
            "Scarlett Johansson"
          }
        )
      }
    `, { title: "Iron Man 2" });
  }

  main();

We can now seed the database by running this script with ``tsx``

.. code-block:: bash

  $ npx tsx seed.ts

Feel free to explore the database in the :ref:`EdgeDB UI <ref_cli_edgedb_ui>`,
where you will find the new data you inserted through this script, as well as
any data you inserted when running the Quickstart.

.. note:: A word on module systems

  Different build tools and runtimes have different specifications for how
  modules are imported, and we support a wide-range of those styles. For
  clarity, we will be sticking to standard TypeScript-style ESM module importing
  without a file extension throughout this documentation. Please see your build
  or environment tooling's guidance on how to adapt this style.

Querying with plain strings
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now, let's write a Node.js script that queries the database for details about
Iron Man 2:

.. code-block:: typescript
  :caption: query.ts

  import * as edgedb from "edgedb";

  const client = edgedb.createClient();

  async function main() {
    const result = await client.querySingle(`
      select Movie {
        id,
        title,
        actors: {
          id,
          name,
        }
      } filter .title = "Iron Man 2"
    `);

    console.log(JSON.stringify(result, null, 2));
  }

  main();

Interfaces
^^^^^^^^^^

Since we're using TypeScript, it would be nice to be able to type the return
value of this query, so let's use our first generator, the :ref:`interfaces
generator <edgedb-js-interfaces>` to tell TypeScript what the type of our result
is.

First we run the generator:

.. code-block:: bash

  $ npx @edgedb/generate interfaces

This generator introspects your database schema and generates a set of
equivalent TypeScript interfaces.

Now we can annotate our query since we are selecting the whole ``Movie`` type:

.. code-block:: typescript-diff
  :caption: query.ts

    import * as edgedb from "edgedb";
    import { Movie } from "./dbschema/interfaces"

    const client = edgedb.createClient();

    async function main() {
      // result will be inferred as Movie | null
  -   const result = await client.querySingle(`
  +   const result = await client.querySingle<Movie>(`
        select Movie {
          id,
          title,
          actors: {
            id,
            name,
          }
        } filter .title = "Iron Man 2"
      `);

      console.log(JSON.stringify(result, null, 2));
    }

    main();

You can now run the script with ``tsx``:

.. code-block:: bash

  $ npx tsx query.ts

Queries generator
^^^^^^^^^^^^^^^^^

Wouldn't it be great if we could write any arbitrary query and get a type-safe
function that we could call? Good news, that's exactly what the next generator
does! The :ref:`queries generator <edgedb-js-queries>` scans your project for
``*.edgeql`` files and generates a file containing a strongly-typed function.

First, move the query into a separate file called ``getMovie.edgeql``.

.. code-block:: edgeql
  :caption: getMovie.edgeql

  select Movie {
    id,
    title,
    actors: {
      id,
      name,
    }
  };


Next, we'll run the ``queries`` generator, specifying the ``--file`` option
which will compile all the queries it finds into a single TypeScript module:

.. code-block:: bash

  $ npx @edgedb/generate queries --file

Now, let's update our query script to call the generated function, which will
provide us with type-safe querying.

.. code-block:: typescript-diff
  :caption: query.ts

    import * as edgedb from "edgedb";
  - import { Movie } from "./dbschema/interfaces"
  + import { getMovie } from "./dbschema/queries"

    const client = edgedb.createClient();

    async function main() {
      // result will be inferred as Movie | null
  -   const result = await client.querySingle<Movie>(`
  -     select Movie {
  -       id,
  -       title,
  -       actors: {
  -         id,
  -         name,
  -       }
  -     } filter .title = "Iron Man 2"
  -   `);
  +   const result = await getMovie(client);

      console.log(JSON.stringify(result, null, 2));
    }

    main();

Now, if you change the query to return different data, or take parameters, and
run the queries generator again, the type of the newly generated function will
change. It'll be completely type safe!

Query builder
^^^^^^^^^^^^^

At last we've arrived at the most powerful API for querying your EdgeDB
instance: the query builder. The EdgeDB query builder provides a **code-first**
way to write **fully-typed** EdgeQL queries with TypeScript. We recommend it for
TypeScript users, or anyone who prefers writing queries with code.

First, we'll run the query builder generator:

.. code-block:: bash

  $ npx @edgedb/generate edgeql-js

.. note:: Version control

  The first time you run the generator, you'll be prompted to add the generated
  files to your ``.gitignore``. Confirm this prompt to automatically add a line
  to your ``.gitignore`` that excludes the generated files.

  For consistency, we recommend omitting the generated files from version
  control and re-generating them as part of your deployment process. However,
  there may be circumstances where checking the generated files into version
  control is desirable, e.g. if you are building Docker images that must contain
  the full source code of your application.

Now, we can import the generated query builder and express our query completely
in TypeScript, getting editor completion, type checking, and type inferrence:

.. code-block:: typescript-diff
  :caption: query.ts

    import * as edgedb from "edgedb";
  - import { getMovie } from "./dbschema/queries";
  + import e from "./dbschema/edgeql-js";

    const client = edgedb.createClient();

    async function main() {
  -   // result will be inferred as Movie | null
  +   // result will be inferred based on the query
  -   const result = await getMovie(client);
  +   const result = await e
  +     .select(e.Movie, () => ({
  +       id: true,
  +       title: true,
  +       actors: () => ({ id: true, name: true }),
  +       filter_single: { title: "Iron Man 2" },
  +     }))
  +     .run(client);

      console.log(JSON.stringify(result, null, 2));
    }

    main();

What's next
===========

We recommend reading the :ref:`client docs <edgedb-js-driver>` first and getting
familiar with configuring the client. You'll find important APIs like
``withGlobals`` and connection details there. After that, depending on your
preferences, look through the :ref:`query builder <edgedb-js-qb>` documentation
and use the other pages as a reference for writing code-first EdgeDB queries.
