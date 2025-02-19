.. _ref_quickstart_inheritance:

========================
Adding shared properties
========================

.. edb:split-section::

  One common pattern in applications is to add shared properties to the schema that are used by multiple objects. For example, you might want to add a ``created_at`` and ``updated_at`` property to every object in your schema. You can do this by adding an abstract type and using it as a mixin for your other object types.

  .. code-block:: sdl-diff
    :caption: dbschema/default.gel

      module default {
    +   abstract type Timestamped {
    +     required created_at: datetime {
    +       default := datetime_of_statement();
    +     };
    +     required updated_at: datetime {
    +       default := datetime_of_statement();
    +     };
    +   }
    +
    -   type Deck {
    +   type Deck extending Timestamped {
          required name: str;
          description: str;

          multi cards: Card {
            constraint exclusive;
            on target delete allow;
          };
        };

    -   type Card {
    +   type Card extending Timestamped {
          required order: int64;
          required front: str;
          required back: str;
        }
      }

.. edb:split-section::

  Since you don't have historical data for when these objects were actually created or modified, the migration will fall back to the default values set in the ``Timestamped`` type.

  .. code-block:: sh

    $ npx gel migration create
    did you create object type 'default::Timestamped'? [y,n,l,c,b,s,q,?]
    > y
    did you alter object type 'default::Card'? [y,n,l,c,b,s,q,?]
    > y
    did you alter object type 'default::Deck'? [y,n,l,c,b,s,q,?]
    > y
    Created /home/strinh/projects/flashcards/dbschema/migrations/00004-m1d2m5n.edgeql, id: m1d2m5n5ajkalyijrxdliioyginonqbtfzihvwdfdmfwodunszstya

    $ npx gel migrate
    Applying m1d2m5n5ajkalyijrxdliioyginonqbtfzihvwdfdmfwodunszstya (00004-m1d2m5n.edgeql)
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

  Update the ``getDecks`` query to sort the decks by ``updated_at`` in descending order.

  .. code-block:: typescript-diff
    :caption: app/queries.ts

      import { client } from "@/lib/gel";
      import e from "@/dbschema/edgeql-js";

      export async function getDecks() {
        const decks = await e.select(e.Deck, (deck) => ({
          id: true,
          name: true,
          description: true,
          cards: e.select(deck.cards, (card) => ({
            id: true,
            front: true,
            back: true,
            order_by: card.order,
          })),
    +     order_by: {
    +       expression: deck.updated_at,
    +       direction: e.DESC,
    +     },
        })).run(client);

        return decks;
      }

.. edb:split-section::

  Now when you look at the data in the UI, you will see the new properties on each of your object types.

  .. image:: images/timestamped.png
