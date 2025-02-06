.. _ref_quickstart_workflow:

===============================
A Smoother Development Workflow
===============================

Part of building an application involves making changes to the schema, so let's take some time to make a comfortable development workflow for ourselves. Spending a bit of time now will make it easier to stay in the flow of development.

Staying in sync
===============

.. edb:split-section::

  When your schema changes, we need to regenerate the query builder files to pick up the new changes. This slows us down a bit, so the next workflow improvement is to add a hook script so that any time the schema changes, the query builder files are regenerated automatically after the migration is applied.

  .. code-block:: toml-diff
    :caption: gel.toml

      [gel]
      server-version = 6.0
    +
    + [hooks]
    + schema.update.after = "npx @gel/generate edgeql-js"

.. edb:split-section::

  Let's make some changes to our schema and update our code. Our first change will be to add a property to our ``Deck`` type that stores the link to all of the cards in the deck ordered by the ``order`` property on the ``Card`` type. We will create a computed property, and use a back link from the ``Card`` type to the ``Deck`` type.

  .. code-block:: sdl-diff
    :caption: dbschema/default.gel

      module default {
        type Deck {
          required name: str;
          description: str;
    +
    +     cards := (select .<deck[is Card] order by .order);
        };

        type Card {
          required order: int64;
          required front: str;
          required back: str;

          required deck: Deck;
        }
      };

.. edb:split-section::

  Now that we've made our changes, we can create a migration to apply the changes to the database, and we'll see that it also regenerates the query builder files.

  .. code-block:: sh

      $ npx gel migration create
      $ npx gel migrate

.. edb:split-section::

  At the moment, in our ``getDeck`` query, we are defining this ``cards`` property explicitly. Now that we've added the computed property, we can remove the explicit definition.

  .. code-block:: typescript-diff
    :caption: app/deck/[id]/page.tsx

      import { redirect } from "next/navigation";
      import { client } from "@/lib/gel";
      import e from "@/dbschema/edgeql-js";

      const getDeckQuery = e.params({ deckId: e.uuid }, (params) =>
        e.select(e.Deck, (d) => ({
          filter_single: e.op(d.id, "=", params.deckId),
          id: true,
          name: true,
          description: true,
    -     cards: e.select(d["<deck[is Card]"], (c) => ({
    +     cards: {
            id: true,
            front: true,
            back: true,
            order: true,
    -       order_by: c.order,
    -     })),
          },
        }))
      );

      export default async function DeckPage(
        { params }: { params: Promise<{ id: string }> }
      ) {
        const { id: deckId } = await params;
        const deck = await getDeckQuery.run(client, { deckId });

        if (!deck) {
          redirect("/");
        }

        return (
          <div>
            <h1>{deck.name}</h1>
            <p>{deck.description}</p>
            <ul>
              {deck.cards.map((card) => (
                <dl key={card.id}>
                  <dt>{card.front}</dt>
                  <dd>{card.back}</dd>
                </dl>
              ))}
            </ul>
          </div>
        )
      }
