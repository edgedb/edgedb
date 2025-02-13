.. _ref_quickstart_modeling:

=================
Modeling the data
=================

.. edb:split-section::

  The flashcards application has a simple data model, but it's interesting enough to get a taste of many of the features of the Gel schema language. You have a ``Card`` type that describes a single flashcard, which for now contains two required string properties: ``front`` and ``back``. Each ``Card`` belongs to a ``Deck``, and there is an explicit ordering to the cards in a given deck.

  Looking at the mock data, you can see this structure in the JSON.

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

  Starting with this simple model, add these types to the ``default.gel`` schema file. As you can see, the types closely mirror the JSON mock data.

  Also of note, the link between ``Card`` and ``Deck`` objects creates a "1-to-n" relationship, where each ``Deck`` object has a link to zero or more ``Card`` objects. When you query the ``Deck.cards`` link, the cards will be unordered, so the ``Card`` type needs an explicit ``order`` property to allow sorting them at query time.

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
    +     };
    +   };
      };

.. edb:split-section::

  Now that you've written the first version of your data model's schema, create a migration to apply this schema to the database. When making changes to your schema, the CLI migration tool will ask some questions to ensure that the changes you are making are what you expect. Since you had an empty schema before, the CLI will skip asking any questions and simply create the migration file.

  .. code-block:: sh

      $ npx gel migration create
      Created ./dbschema/migrations/00001-m125ajr.edgeql, id: m125ajrbqp7ov36s7aniefxc376ofxdlketzspy4yddd3hrh4lxmla
      $ npx gel migrate
      Applying m125ajrbqp7ov36s7aniefxc376ofxdlketzspy4yddd3hrh4lxmla (00001-m125ajr.edgeql)
      ... parsed
      ... applied


.. edb:split-section::

  Take a look at the schema you've generated in the built-in database UI. Use this tool to visualize your data model and see the object types and links you've defined.

  .. code-block:: sh

      $ npx gel ui
