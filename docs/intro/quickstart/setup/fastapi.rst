.. _ref_quickstart_fastapi_setup:

===========================
Setting up your environment
===========================

.. edb:split-section::

  Use git to clone the `FastAPI starter template <https://github.com/geldata/quickstart-fastapi>`_ into a new directory called ``flashcards``. This will create a fully configured FastAPI project and a local |Gel| instance with an empty schema. You will see the database instance being created and the project being initialized. You are now ready to start building the application.

  .. code-block:: sh

    $ git clone \
        git@github.com:geldata/quickstart-fastapi.git \
        flashcards
    $ cd flashcards
    $ python -m venv venv
    $ source venv/bin/activate # or venv\Scripts\activate on Windows
    $ pip install -r requirements.txt
    $ uvx gel project init

.. edb:split-section::

  Explore the empty database by starting our REPL from the project root.

  .. code-block:: sh

    $ uvx gel

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

  Fun! You will create a proper data model for the application in the next step, but for now, take a look around the project we have. Here are the files that integrate |Gel|:

  - ``gel.toml``: The configuration file for the |Gel| project instance. Notice that we have a ``hooks.migration.apply.after`` hook that will run ``uvx gel-py`` after migrations are applied. This will run the code generator that you will use later to get fully type-safe queries you can run from your FastAPI backend. More details on that to come!
  - ``dbschema/``: This directory contains the schema for the database, and later supporting files like migrations, and generated code.
  - :dotgel:`dbschema/default`: The default schema file that you'll use to define your data model. It is empty for now, but you'll add your data model to this file in the next step.

  .. tabs::

    .. code-tab:: toml
      :caption: gel.toml

      [instance]
      server-version = 6.0

      [hooks]
      schema.update.after = "uvx gel-py"

    .. code-tab:: sdl
      :caption: dbschema/default.gel

      module default {

      }
