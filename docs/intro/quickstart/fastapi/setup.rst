.. _ref_quickstart_setup:

#############################
 Setting up your environment
#############################

.. edb:split-section::

  Use git to clone the Next.js starter template into a new directory called ``flashcards``. This will create a fully configured Next.js project and a local Gel instance with an empty schema. You will see the database instance being installed and the project being initialized. You are now ready to start building the application.

  .. code-block:: sh

    $ git clone \
        git@github.com:geldata/quickstart-fastapi.git \
        flashcards
    $ cd flashcards
    $ python -m venv venv
    $ source venv/bin/activate # or venv\Scripts\activate on Windows
    $ pip install -r requirements.txt

.. edb:split-section::

  Next, you need to install the Gel CLI. The Gel CLI is a tool that helps you manage your Gel project. You will use it to run migrations, generate code, and interact with the database.

  .. code-block:: sh

    $ curl --proto '=https' --tlsv1.2 -sSf https://sh.edgedb.com | sh

.. edb:split-section::

  Once you've installed the Gel CLI, you can initialize the project by running the following command from the project root. This will create a new Gel project instance in the current directory.


  .. code-block:: sh

    $ gel project init

.. edb:split-section::

  Explore the empty database by starting our REPL from the project root.

  .. code-block:: sh

    $ gel

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

  Fun! You will create a proper data model for the application in the next step, but for now, take a look around the project we have. Here are the new files that integrate Gel:

  - ``gel.toml``: The configuration file for the Gel project instance.
  - ``dbschema/``: This directory contains the schema for the database, and later supporting files like migrations, and generated code.
  - ``dbschema/default.gel``: The default schema file that you'll use to define your data model. It is empty for now, but you'll add your data model to this file in the next step.

  .. code-block:: sh

    $ tree
