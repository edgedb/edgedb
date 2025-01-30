.. _ref_quickstart_setup:

===========================
Setting up your environment
===========================

.. edb:split-section::

  We will use our project starter CLI to scaffold our Next.js application with everything we need to get started with Gel. This will create a new directory called ``flashcards`` with a fully configured Next.js project and a local Gel database with an empty schema. You should see the test suite pass, indicating that the database instance was created successfully, and we're ready to start building our application.

  .. note::

    If you run into any issues at this point, look back at the output of the ``npm create @gel`` command for any error messages. Feel free to ask for help in the `Gel Discord <https://discord.gg/gel>`_.

  .. code-block:: sh

      $ npm create @gel \
        --environment=nextjs \
        --project-name=flashcards --yes
      $ cd flashcards
      $ npm run test


.. edb:split-section::

  Let's quickly take a poke around the empty database with our CLI REPL.

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
        (
          front := "What is the highest mountain in the world?",
          back := "Mount Everest",
        ),
        (
          front := "Which ocean contains the deepest trench on Earth?",
          back := "The Pacific Ocean",
        ),
      }
      select cards order by random() limit 1;
      {
        (
          front := "What is the highest mountain in the world?",
          back := "Mount Everest",
        )
      }

.. edb:split-section::

  Fun! We'll create a proper data model for this in the next step, but for now, let's take a look around the project we've just created. Most of the generated files will be familiar to you if you've worked with Next.js before. So let's focus on the new files that were created to integrate Gel.

  - ``gel.toml``: This is the configuration file for the Gel database. It contains the configuration for the local database instance, so that if another developer on your team wants to run the project, they can easily do so and have a compatible database version.
  - ``dbschema/``: This directory contains the schema for the database, and later supporting files like migrations, and generated code.
  - ``dbschema/default.gel``: This is the default schema file that we'll use to define our data model. It is empty for now, but we'll add our data model to this file in the next step.
  - ``lib/gel.ts``: This file contains the Gel client, which we'll use to interact with the database.

  .. code-block:: sh

    $ tree

