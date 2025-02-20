.. _ref_quickstart_modeling:

=================
Modeling the data
=================

.. edb:split-section::

  The flashcards application has a simple data model, but it's interesting enough to utilize many unique features of the |Gel| schema language.

  Looking at the mock data in the example JSON file ``./deck-edgeql.json``, you can see this structure in the JSON. There is a ``Card`` type that describes a single flashcard, which contains two required string properties: ``front`` and ``back``. Each ``Deck`` object has zero or more ``Card`` objects in an array.

  .. code-block:: typescript

    interface Card {
      front: string;
      back: string;
    }

    interface Deck {
      name: string;
      description: string | null;
      cards: Card[];
    }

.. edb:split-section::

  Starting with this simple model, add these types to the :dotgel:`dbschema/default` schema file. As you can see, the types closely mirror the JSON mock data.

  Also of note, the link between ``Card`` and ``Deck`` objects creates a "1-to-n" relationship, where each ``Deck`` object has a link to zero or more ``Card`` objects. When you query the ``Deck.cards`` link, the cards will be unordered, so the ``Card`` type needs an explicit ``order`` property to allow sorting them at query time.

  By default, when you try to delete an object that is linked to another object, the database will prevent you from doing so. We want to support removing a ``Card``, so we define a deletion policy on the ``cards`` link that allows deleting the target of this link.

  .. code-block:: sdl-diff
    :caption: dbschema/default.gel

      module default {
    +   type Card {
    +     required order: int64;
    +     required front: str;
    +     required back: str;
    +   };
    +
    +   type Deck {
    +     required name: str;
    +     description: str;
    +     multi cards: Card {
    +       constraint exclusive;
    +       on target delete allow;
    +     };
    +   };
      };

.. edb:split-section::

  Congratulations! This first version of the data model's schema is *stored in a file on disk*. Now you need to signal the database to actually create types for ``Deck`` and ``Card`` in the database.

  To make |Gel| do that, you need to do two quick steps:

  1. **Create a migration**: a "migration" is a file containing a set of low level instructions that define how the database schema should change. It records any additions, modifications, or deletions to your schema in a way that the database can understand.

     .. note::

       When you are changing existing schema, the CLI migration tool might ask questions to ensure that it understands your changes exactly. Since the existing schema was empty, the CLI will skip asking any questions and simply create the migration file.

  2. **Apply the migration**: This executes the migration file on the database, instructing |Gel| to implement the recorded changes in the database. Essentially, this step updates the database structure to match your defined schema, ensuring that the ``Deck`` and ``Card`` types are created and ready for use.

     .. note::

       Notice that after the migration is applied, the CLI will automatically run the script to generate the query builder. This is a convenience feature that is enabled by the ``schema.update.after`` hook in the ``gel.toml`` file.

  .. code-block:: sh

    $ npx gel migration create
    Created ./dbschema/migrations/00001-m125ajr.edgeql, id: m125ajrbqp7ov36s7aniefxc376ofxdlketzspy4yddd3hrh4lxmla
    $ npx gel migrate
    Applying m125ajrbqp7ov36s7aniefxc376ofxdlketzspy4yddd3hrh4lxmla (00001-m125ajr.edgeql)
    ... parsed
    ... applied
    Generating query builder...
    Detected tsconfig.json, generating TypeScript files.
      To override this, use the --target flag.
      Run `npx @gel/generate --help` for full options.
    Introspecting database schema...
    Generating runtime spec...
    Generating cast maps...
    Generating scalars...
    Generating object types...
    Generating function types...
    Generating operators...
    Generating set impl...
    Generating globals...
    Generating index...
    Writing files to ./dbschema/edgeql-js
    Generation complete! 🤘


.. edb:split-section::

  Take a look at the schema you've generated in the built-in database UI. Use this tool to visualize your data model and see the object types and links you've defined.

  .. edb:split-point::

  .. code-block:: sh

    $ npx gel ui

  .. image:: images/schema-ui.png
