.. _ref_quickstart_setup:

===========================
Setting up your environment
===========================

.. edb:split-section::

  Use git to clone the Next.js starter template into a new directory called ``flashcards``. This will create a fully configured Next.js project and a local Gel instance with an empty schema. You will see the database instance being installed and the project being initialized. You are now ready to start building the application.

  .. code-block:: sh

      $ git clone \
          git@github.com:geldata/quickstart-nextjs.git \
          flashcards
      $ cd flashcards
      $ npm install
      $ npx gel project init


.. edb:split-section::

  Explore the empty database by starting our REPL from the project root.

  .. code-block:: sh

      $ npx gel

.. edb:split-section::

  Try the following queries which will work without any schema defined.

  .. code-block:: edgeql-repl

      db> select 42;
      {42}
      db> select sum({1, 2, 3});
      {6}
      db> with cards := {
      ...   (
      ...     front := "What is the highest mountain in the world?",
      ...     back := "Mount Everest",
      ...   ),
      ...   (
      ...     front := "Which ocean contains the deepest trench on Earth?",
      ...     back := "The Pacific Ocean",
      ...   ),
      ... }
      ... select cards order by random() limit 1;
      {
        (
          front := "What is the highest mountain in the world?",
          back := "Mount Everest",
        )
      }

.. edb:split-section::

  Fun! You will create a proper data model for the application in the next step, but for now, take a look around the project you've just created. Most of the project files will be familiar if you've worked with Next.js before. Here are the new files that integrate Gel:

  - ``gel.toml``: The configuration file for the Gel project instance.
  - ``dbschema/``: This directory contains the schema for the database, and later supporting files like migrations, and generated code.
  - ``dbschema/default.gel``: The default schema file that you'll use to define your data model. It is empty for now, but you'll add your data model to this file in the next step.
  - ``lib/gel.ts``: A utility module that exports the Gel client, which you'll use to interact with the database.

  .. code-block:: sh

    $ tree


.. edb:split-section::

  With TypeScript, there are three ways to run a query: use a string EdgeQL query, use the ``queries`` generator to turn a string of EdgeQL into a TypeScript function, or use the query builder API to build queries dynamically in a type-safe manner. In this tutorial, you will use the TypeScript query builder API.

  This query builder must be generated any time the schema changes, so before you start building your application, add a hook in your ``gel.toml`` file to generate the query builder when the schema is updated.

  .. code-block:: toml-diff
    :caption: gel.toml

      [instance]
      server-version = 6.0
    +
    + [hooks]
    + schema.update.after = "npx @gel/generate edgeql-js"
