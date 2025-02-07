.. _edgedb-js-generators:

Generators
==========

The ``@edgedb/generate`` package provides a set of code generation tools that
are useful when developing an EdgeDB-backed applications with
TypeScript/JavaScript.

To get started with generators, first initialize an :ref:`EdgeDB project
<ref_guide_using_projects>` in the root of your application. Generators will
look for an ``edgedb.toml`` file to determine the root of your application. See
the :ref:`Overview <edgedb-js-installation>` page for details on installing.

.. note::

   Generators work by connecting to the database to get information about the current state of the schema. Make sure you run the generators again any time the schema changes so that the generated code is in-sync with the current state of the schema.

Run a generator with the following command.

.. tabs::

  .. code-tab:: bash
    :caption: npm

    $ npx @edgedb/generate <generator> [options]

  .. code-tab:: bash
    :caption: yarn

    $ yarn run -B generate <generator> [options]

  .. code-tab:: bash
    :caption: pnpm

    $ pnpm exec generate <generator> [options]

  .. code-tab:: bash
    :caption: Deno

    $ deno run \
      --allow-all \
      --unstable \
      https://deno.land/x/edgedb/generate.ts <generator> [options]

  .. code-tab:: bash
    :caption: bun

    $ bunx @edgedb/generate <generator> [options]

The value of ``<generator>`` should be one of the following:

.. list-table::
   :class: funcoptable

   * - ``edgeql-js``
     - Generates the query builder which provides a **code-first** way to write
       **fully-typed** EdgeQL queries with TypeScript. We recommend it for
       TypeScript users, or anyone who prefers writing queries with code.
     - :ref:`docs <edgedb-js-qb>`

   * - ``queries``
     - Scans your project for ``*.edgeql`` files and generates functions that
       allow you to execute these queries in a typesafe way.
     - :ref:`docs <edgedb-js-queries>`

   * - ``interfaces``
     - Introspects your schema and generates file containing *TypeScript
       interfaces* that correspond to each object type. This is useful for
       writing typesafe code to interact with EdgeDB.
     - :ref:`docs <edgedb-js-interfaces>`

Connection
^^^^^^^^^^

The generators require a connection to an active EdgeDB database. It does
**not** simply read your local ``.esdl`` schema files. Generators rely on the
database to introspect the schema and analyze queries. Doing so without a
database connection would require implementing a full EdgeQL parser and static
analyzer in JavaScript—which we don't intend to do anytime soon.

.. note::

  Make sure your development database is up-to-date with your latest schema
  before running a generator!

If you're using ``edgedb project init``, the connection is automatically handled
for you. Otherwise, you'll need to explicitly pass connection information via
environment variables or CLI flags, just like any other CLI command. See
:ref:`Client Libraries > Connection <edgedb_client_connection>` for guidance.

.. _edgedb_qb_target:

Targets
^^^^^^^

All generators look at your environment and guess what kind of files to generate
(``.ts`` vs ``.js + .d.ts``) and what module system to use (CommonJS vs ES
modules). You can override this with the ``--target`` flag.

.. list-table::

  * - ``--target ts``
    - Generate TypeScript files (``.ts``)
  * - ``--target mts``
    - Generate TypeScript files (``.mts``) with extensioned ESM imports
  * - ``--target esm``
    - Generate ``.js`` with ESM syntax and ``.d.ts`` declaration files
  * - ``--target cjs``
    - Generate JavaScript with CommonJS syntax and and ``.d.ts`` declaration
      files
  * - ``--target deno``
    - Generate TypeScript files with Deno-style ESM imports

Help
^^^^

To see helptext for the ``@edgedb/generate`` command, run the following.

.. code-block:: bash

  $ npx @edgedb/generate --help
