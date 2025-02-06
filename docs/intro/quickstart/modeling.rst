.. _ref_quickstart_modeling:

=================
Modeling our data
=================

.. edb:split-section::

  Our flashcards application has a simple data model, but it's interesting enough to get a taste of many of the features of the Gel schema language. We have a ``Card`` type that describes an single flashcard, which for now contains two required string properties: ``front`` and ``back``. Each ``Card`` belongs to a ``Deck``, and there is an explicit ordering to the cards in a given deck.

  Starting with this simple model, let's express these types in the ``default.gel`` schema file.

  .. code-block:: sdl-diff
    :caption: dbschema/default.gel

      module default {
    +   type Deck {
    +     required name: str;
    +     description: str;
    +   };

    +   type Card {
    +     required order: int64;
    +     required front: str;
    +     required back: str;

    +     required deck: Deck;
    +   }
      };

.. edb:split-section::

  Now that we've written our first version of our data model's schema, we will create a migration to apply this schema to the database. When making changes to our schema, the CLI migration tool will ask some questions to ensure that the changes we are making are what we expect. Since we had an empty schema before, the CLI will skip asking any questions and simply create the migration file.

  .. code-block:: sh

      $ npx gel migration create
      Created ./dbschema/migrations/00001-m125ajr.edgeql, id: m125ajrbqp7ov36s7aniefxc376ofxdlketzspy4yddd3hrh4lxmla
      $ npx gel migrate
      Applying m125ajrbqp7ov36s7aniefxc376ofxdlketzspy4yddd3hrh4lxmla (00001-m125ajr.edgeql)
      ... parsed
      ... applied


.. edb:split-section::

  Let's take a look at the schema we've generated in our built-in database UI. We can use this tool to visualize our data model and see the object types and links we've defined.

  .. code-block:: sh

      $ npx gel ui
