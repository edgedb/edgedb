.. _edgedb-js-qb:

=======================
Query Builder Generator
=======================
:index: querybuilder generator typescript

The EdgeDB query builder provides a **code-first** way to write
**fully-typed** EdgeQL queries with TypeScript. We recommend it for TypeScript
users, or anyone who prefers writing queries with code.

.. code-block:: typescript

  import * as edgedb from "edgedb";
  import e from "./dbschema/edgeql-js";

  const client = edgedb.createClient();

  async function run() {
    const query = e.select(e.Movie, ()=>({
      id: true,
      title: true,
      actors: { name: true }
    }));

    const result = await query.run(client)
    /*
      {
        id: string;
        title: string;
        actors: { name: string; }[];
      }[]
    */
  }

  run();

.. note:: Is it an ORM?

  No—it's better! Like any modern TypeScript ORM, the query builder gives you
  full typesafety and autocompletion, but without the power and `performance
  <https://github.com/edgedb/imdbench>`_
  tradeoffs. You have access to the **full power** of EdgeQL and can write
  EdgeQL queries of arbitrary complexity. And since EdgeDB compiles each
  EdgeQL query into a single, highly-optimized SQL query, your queries stay
  fast, even when they're complex.

Why use the query builder?
--------------------------

*Type inference!* If you're using TypeScript, the result type of *all
queries* is automatically inferred for you. For the first time, you don't
need an ORM to write strongly typed queries.

*Auto-completion!* You can write queries full autocompletion on EdgeQL
keywords, standard library functions, and link/property names.

*Type checking!* In the vast majority of cases, the query builder won't let
you construct invalid queries. This eliminates an entire class of bugs and
helps you write valid queries the first time.

*Close to EdgeQL!* The goal of the query builder is to provide an API that is as
close as possible to EdgeQL itself while feeling like idiomatic TypeScript.

Installation
------------

To get started, install the following packages.

.. note::

  If you're using Deno, you can skip this step.

Install the ``edgedb`` package.

.. code-block:: bash

  $ npm install edgedb       # npm users
  $ yarn add edgedb          # yarn users
  $ bun add edgedb           # bun users

Then install ``@edgedb/generate`` as a dev dependency.

.. code-block:: bash

  $ npm install @edgedb/generate --save-dev      # npm users
  $ yarn add @edgedb/generate --dev              # yarn users
  $ bun add --dev @edgedb/generate               # bun users


Generation
----------

The following command will run the ``edgeql-js`` query builder generator.

.. tabs::

  .. code-tab:: bash
    :caption: Node.js

    $ npx @edgedb/generate edgeql-js

  .. code-tab:: bash
    :caption: Deno

    $ deno run --allow-all --unstable https://deno.land/x/edgedb/generate.ts edgeql-js

  .. code-tab:: bash
    :caption: Bun

    $ bunx @edgedb/generate edgeql-js

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

The generation command is configurable in a number of ways.

``--output-dir <path>``
  Sets the output directory for the generated files.

``--target <ts|cjs|esm|mts>``
  What type of files to generate.

``--force-overwrite``
  To avoid accidental changes, you'll be prompted to confirm whenever the
  ``--target`` has changed from the previous run. To avoid this prompt, pass
  ``--force-overwrite``.

The generator also supports all the :ref:`connection flags
<ref_cli_edgedb_connopts>` supported by the EdgeDB CLI. These aren't
necessary when using a project or environment variables to configure a
connection.

.. note::

   Generators work by connecting to the database to get information about the current state of the schema. Make sure you run the generators again any time the schema changes so that the generated code is in-sync with the current state of the schema.

.. _edgedb-js-execution:

Expressions
-----------

Throughout the documentation, we use the term "expression" a lot. This is a
catch-all term that refers to *any query or query fragment* you define with
the query builder. They all conform to an interface called ``Expression`` with
some common functionality.

Most importantly, any expression can be executed with the ``.run()`` method,
which accepts a ``Client`` instead as the first argument. The result is
``Promise<T>``, where ``T`` is the inferred type of the query.

.. code-block:: typescript

  await e.str("hello world").run(client);
  // => "hello world"

  await e.set(e.int64(1), e.int64(2), e.int64(3)).run(client);
  // => [1, 2, 3]

  await e
    .select(e.Movie, () => ({
      title: true,
      actors: { name: true },
    }))
    .run(client);
  // => [{ title: "The Avengers", actors: [...]}]

Note that the ``.run`` method accepts an instance of :js:class:`Client` (or
``Transaction``) as it's first argument. See :ref:`Creating a Client
<edgedb-js-create-client>` for details on creating clients. The second
argument is for passing :ref:`$parameters <edgedb-js-parameters>`, more on
that later.

.. code-block:: typescript

  .run(client: Client | Transaction, params: Params): Promise<T>


Converting to EdgeQL
--------------------
:index: querybuilder toedgeql

You can extract an EdgeQL representation of any expression calling the
``.toEdgeQL()`` method. Below is a number of expressions and the EdgeQL they
produce. (The actual EdgeQL the create may look slightly different, but it's
equivalent.)

.. code-block:: typescript

  e.str("hello world").toEdgeQL();
  // => select "hello world"

  e.set(e.int64(1), e.int64(2), e.int64(3)).toEdgeQL();
  // => select {1, 2, 3}

  e.select(e.Movie, () => ({
    title: true,
    actors: { name: true }
  })).toEdgeQL();
  // => select Movie { title, actors: { name }}

Extracting the inferred type
----------------------------

The query builder *automatically infers* the TypeScript type that best
represents the result of a given expression. This inferred type can be
extracted with the ``$infer`` type helper.

.. code-block:: typescript

  import e, { type $infer } from "./dbschema/edgeql-js";

  const query = e.select(e.Movie, () => ({ id: true, title: true }));
  type result = $infer<typeof query>;
  // { id: string; title: string }[]

Cheatsheet
----------

Below is a set of examples to get you started with the query builder. It is
not intended to be comprehensive, but it should provide a good starting point.

.. note::

  Modify the examples below to fit your schema, paste them into ``script.ts``,
  and execute them with the ``npx`` command from the previous section! Note
  how the signature of ``result`` changes as you modify the query.

Insert an object
^^^^^^^^^^^^^^^^

.. code-block:: typescript

  const query = e.insert(e.Movie, {
    title: 'Doctor Strange 2',
    release_year: 2022
  });

  const result = await query.run(client);
  // { id: string }
  // by default INSERT only returns the id of the new object

.. _edgedb-js-qb-transaction:

Transaction
^^^^^^^^^^^

We can also run the same query as above, build with the query builder, in a
transaction.

.. code-block:: typescript

  const query = e.insert(e.Movie, {
    title: 'Doctor Strange 2',
    release_year: 2022
  });

  await client.transaction(async (tx) => {
    const result = await query.run(tx);
    // { id: string }
  });


Select objects
^^^^^^^^^^^^^^

.. code-block:: typescript

  const query = e.select(e.Movie, () => ({
    id: true,
    title: true,
  }));

  const result = await query.run(client);
  // { id: string; title: string; }[]

To select all properties of an object, use the spread operator with the
special ``*`` property:

.. code-block:: typescript

  const query = e.select(e.Movie, () => ({
    ...e.Movie['*']
  }));

  const result = await query.run(client);
  /*
    {
      id: string;
      title: string;
      release_year: number | null;  # optional property
    }[]
  */

Nested shapes
^^^^^^^^^^^^^

.. code-block:: typescript

  const query = e.select(e.Movie, () => ({
    id: true,
    title: true,
    actors: {
      name: true,
    }
  }));

  const result = await query.run(client);
  /*
    {
      id: string;
      title: string;
      actors: { name: string; }[];
    }[]
  */

Filtering
^^^^^^^^^

Pass a boolean expression as the special key ``filter`` to filter the results.

.. code-block:: typescript

  const query = e.select(e.Movie, (movie) => ({
    id: true,
    title: true,
    // special "filter" key
    filter: e.op(movie.release_year, ">", 1999)
  }));

  const result = await query.run(client);
  // { id: string; title: number }[]

Since ``filter`` is a reserved keyword in EdgeQL, the special ``filter`` key can
live alongside your property keys without a risk of collision.

.. note::

  The ``e.op`` function is used to express EdgeQL operators. It is documented in
  more detail below and on the :ref:`Functions and operators
  <edgedb-js-funcops>` page.

Select a single object
^^^^^^^^^^^^^^^^^^^^^^

To select a particular object, use the ``filter_single`` key. This tells the
query builder to expect a singleton result.

.. code-block:: typescript

  const query = e.select(e.Movie, (movie) => ({
    id: true,
    title: true,
    release_year: true,

    filter_single: e.op(
      movie.id,
      "=",
      e.uuid("2053a8b4-49b1-437a-84c8-e1b0291ccd9f")
    },
  }));

  const result = await query.run(client);
  // { id: string; title: string; release_year: number | null }

For convenience ``filter_single`` also supports a simplified syntax that
eliminates the need for ``e.op`` when used on exclusive properties:

.. code-block:: typescript

  e.select(e.Movie, (movie) => ({
    id: true,
    title: true,
    release_year: true,

    filter_single: { id: "2053a8b4-49b1-437a-84c8-e1b0291ccd9f" },
  }));

This also works if an object type has a composite exclusive constraint:

.. code-block:: typescript

  /*
    type Movie {
      ...
      constraint exclusive on (.title, .release_year);
    }
  */

  e.select(e.Movie, (movie) => ({
    title: true,
    filter_single: {
      title: "The Avengers",
      release_year: 2012
    },
  }));


Ordering and pagination
^^^^^^^^^^^^^^^^^^^^^^^

The special keys ``order_by``, ``limit``, and ``offset`` correspond to
equivalent EdgeQL clauses.

.. code-block:: typescript

  const query = e.select(e.Movie, (movie) => ({
    id: true,
    title: true,

    order_by: movie.title,
    limit: 10,
    offset: 10
  }));

  const result = await query.run(client);
  // { id: true; title: true }[]

Operators
^^^^^^^^^

Note that the filter expression above uses ``e.op`` function, which is how to
use *operators* like ``=``, ``>=``, ``++``, and ``and``.

.. code-block:: typescript

  // prefix (unary) operators
  e.op("not", e.bool(true));      // not true
  e.op("exists", e.set("hi"));    // exists {"hi"}

  // infix (binary) operators
  e.op(e.int64(2), "+", e.int64(2)); // 2 + 2
  e.op(e.str("Hello "), "++", e.str("World!")); // "Hello " ++ "World!"

  // ternary operator (if/else)
  e.op(e.str("😄"), "if", e.bool(true), "else", e.str("😢"));
  // "😄" if true else "😢"


Update objects
^^^^^^^^^^^^^^

.. code-block:: typescript

  const query = e.update(e.Movie, (movie) => ({
    filter_single: { title: "Doctor Strange 2" },
    set: {
      title: "Doctor Strange in the Multiverse of Madness",
    },
  }));

  const result = await query.run(client);

Delete objects
^^^^^^^^^^^^^^

.. code-block:: typescript

  const query = e.delete(e.Movie, (movie) => ({
    filter: e.op(movie.title, 'ilike', "the avengers%"),
  }));

  const result = await query.run(client);
  // { id: string }[]

Delete multiple objects using an array of properties:

.. code-block:: typescript

  const titles = ["The Avengers", "Doctor Strange 2"];
  const query = e.delete(e.Movie, (movie) => ({
    filter: e.op(
      movie.title,
      "in",
      e.array_unpack(e.literal(e.array(e.str), titles))
    )
  }));
  const result = await query.run(client);
  // { id: string }[]

Note that we have to use ``array_unpack`` to cast our ``array<str>`` into a
``set<str>`` since the ``in`` operator works on sets. And we use ``literal`` to
create a custom literal since we're inlining the titles array into our query.

Here's an example of how to do this with params:

.. code-block:: typescript

  const query = e.params({ titles: e.array(e.str) }, ({ titles }) =>
    e.delete(e.Movie, (movie) => ({
      filter: e.op(movie.title, "in", e.array_unpack(titles)),
    }))
  );

  const result = await query.run(client, {
    titles: ["The Avengers", "Doctor Strange 2"],
  });
  // { id: string }[]

Compose queries
^^^^^^^^^^^^^^^

All query expressions are fully composable; this is one of the major
differentiators between this query builder and a typical ORM. For instance, we
can ``select`` an ``insert`` query in order to fetch properties of the object we
just inserted.


.. code-block:: typescript

  const newMovie = e.insert(e.Movie, {
    title: "Iron Man",
    release_year: 2008
  });

  const query = e.select(newMovie, () => ({
    title: true,
    release_year: true,
    num_actors: e.count(newMovie.actors)
  }));

  const result = await query.run(client);
  // { title: string; release_year: number; num_actors: number }

Or we can use subqueries inside mutations.

.. code-block:: typescript

  // select Doctor Strange
  const drStrange = e.select(e.Movie, (movie) => ({
    filter_single: { title: "Doctor Strange" }
  }));

  // select actors
  const actors = e.select(e.Person, (person) => ({
    filter: e.op(
      person.name,
      "in",
      e.set("Benedict Cumberbatch", "Rachel McAdams")
    )
  }));

  // add actors to cast of drStrange
  const query = e.update(drStrange, () => ({
    actors: { "+=": actors }
  }));

  const result = await query.run(client);


Parameters
^^^^^^^^^^

.. code-block:: typescript

  const query = e.params({
    title: e.str,
    release_year: e.int64,
  },
  (params) => {
    return e.insert(e.Movie, {
      title: params.title,
      release_year: params.release_year,
    }))
  };

  const result = await query.run(client, {
    title: "Thor: Love and Thunder",
    release_year: 2022,
  });
  // { id: string }

.. note::

  Continue reading for more complete documentation on how to express any
  EdgeQL query with the query builder.


.. _ref_edgedbjs_globals:

Globals
^^^^^^^

Reference global variables.

.. code-block:: typescript

  e.global.user_id;
  e.default.global.user_id;  // same as above
  e.my_module.global.some_value;

Other modules
^^^^^^^^^^^^^

Reference entities in modules other than ``default``.

The ``Vampire`` type in a module named ``characters``:

.. code-block:: typescript

  e.characters.Vampire;

As shown in "Globals," a global ``some_value`` in a module ``my_module``:

.. code-block:: typescript

  e.my_module.global.some_value;
