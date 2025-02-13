.. _ref_quickstart_working:

=====================
Working with the data
=====================

.. edb:split-section::

  With TypeScript, there are three ways to run a query: use a string EdgeQL query, use the ``queries`` generator to turn a string of EdgeQL into a TypeScript function, or use the query builder API to build queries dynamically in a type-safe manner. In this tutorial, you will use the TypeScript query builder API.

  This query builder must be generated any time the schema changes, so add a hook in your ``gel.toml`` file to generate the query builder when the schema is updated.

  .. code-block:: toml-diff
    :caption: gel.toml

      [instance]
      server-version = 6.0
    +
    + [hooks]
    + schema.update.after = "npx @gel/generate edgeql-js"

.. edb:split-section::

  Since our schema migration has already run, we will run the generator once now to generate the query builder files, but subsequent migrations will automatically generate the files as needed.

  .. code-block:: sh

    $ npx @gel/generate edgeql-js

.. edb:split-section::

  Now that the schema has been defined, and the query builder has been generated, update the server action for importing a deck with cards.

  .. code-block:: typescript-diff
    :caption: app/actions.ts

      "use server";

    - import { readFile, writeFile } from "node:fs/promises";
    + import { client } from "@/lib/gel";
    + import e from "@/dbschema/edgeql-js";
      import { revalidatePath } from "next/cache";
    - import { RawJSONDeck, Deck } from "@/lib/models";
    + import { RawJSONDeck } from "@/lib/models";

      export async function importDeck(formData: FormData) {
        const file = formData.get("file") as File;
        const rawDeck = JSON.parse(await file.text()) as RawJSONDeck;
        const deck = {
          ...rawDeck,
    -     id: crypto.randomUUID(),
    -     cards: rawDeck.cards.map((card) => ({
    +     cards: rawDeck.cards.map((card, index) => ({
            ...card,
    -       id: crypto.randomUUID(),
    +       order: index,
          })),
        };
    -
    -   const existingDecks = JSON.parse(
    -     await readFile("./decks.json", "utf-8")
    -   ) as Deck[];
    -
    -   await writeFile(
    -     "./decks.json",
    -     JSON.stringify([...existingDecks, deck], null, 2)
    -   );
    +   const cardIds: string[] = [];
    +   for (const card of deck.cards) {
    +     const createdCard = await e
    +       .insert(e.Card, {
    +         front: card.front,
    +         back: card.back,
    +         order: card.order,
    +       })
    +       .run(client);
    +
    +     cardIds.push(createdCard.id);
    +   }
    +
    +   await e
    +     .params({ cardIds: e.array(e.uuid) }, (params) =>
    +       e.insert(e.Deck, {
    +         name: deck.name,
    +         description: deck.description,
    +         cards: e.select(e.Card, (c) => ({
    +           filter: e.contains(params.cardIds, c.id),
    +         })),
    +       })
    +     )
    +     .run(client, { cardIds });

        revalidatePath("/");
      }

.. edb:split-section::

  This works, but you might notice that it is not atomic. If one of the ``Card`` objects fails to insert, the entire operation will fail and the ``Deck`` will not be inserted. To make this operation atomic, you can use a transaction.

  .. code-block:: typescript-diff
    :caption: app/actions.ts

      "use server";

      import { client } from "@/lib/gel";
      import e from "@/dbschema/edgeql-js";
      import { revalidatePath } from "next/cache";
      import { RawJSONDeck } from "@/lib/models";

      export async function importDeck(formData: FormData) {
        const file = formData.get("file") as File;
        const rawDeck = JSON.parse(await file.text()) as RawJSONDeck;
        const deck = {
          ...rawDeck,
          cards: rawDeck.cards.map((card, index) => ({
            ...card,
            order: index,
          })),
        };
    +   await client.transaction(async (tx) => {
        const cardIds: string[] = [];
        for (const card of deck.cards) {
          const createdCard = await e
            .insert(e.Card, {
              front: card.front,
              back: card.back,
              order: card.order,
            })
    -       .run(client);
    +       .run(tx);

          cardIds.push(createdCard.id);
        }

        await e
          .params({ cardIds: e.array(e.uuid) }, (params) =>
            e.insert(e.Deck, {
              name: deck.name,
              description: deck.description,
              cards: e.select(e.Card, (c) => ({
                filter: e.contains(params.cardIds, c.id),
              })),
            })
          )
    -     .run(client, { cardIds });
    +     .run(tx, { cardIds });
    +   });

        revalidatePath("/");
      }

.. edb:split-section::

  You might think this is as good as it gets, and many ORMs will basically create exactly this set of queries. However, the query builder allows you to do even better by creating a single query that will insert the ``Deck`` and ``Card`` objects, and the link between them all as a single fast query.

  .. code-block:: typescript-diff
    :caption: app/actions.ts

      "use server";

      import { client } from "@/lib/gel";
      import e from "@/dbschema/edgeql-js";
      import { revalidatePath } from "next/cache";
      import { RawJSONDeck } from "@/lib/models";

      export async function importDeck(formData: FormData) {
        const file = formData.get("file") as File;
        const rawDeck = JSON.parse(await file.text()) as RawJSONDeck;
        const deck = {
          ...rawDeck,
          cards: rawDeck.cards.map((card, index) => ({
            ...card,
            order: index,
          })),
        };
    -   await client.transaction(async (tx) => {
    -   const cardIds: string[] = [];
    -   for (const card of deck.cards) {
    -     const createdCard = await e
    -       .insert(e.Card, {
    -         front: card.front,
    -         back: card.back,
    -         order: card.order,
    -       })
    -       .run(tx);
    -
    -     cardIds.push(createdCard.id);
    -   }
    -
    -   await e
    -     .params({ cardIds: e.array(e.uuid) }, (params) =>
    -       e.insert(e.Deck, {
    -         name: deck.name,
    -         description: deck.description,
    -         cards: e.select(e.Card, (c) => ({
    -           filter: e.contains(params.cardIds, c.id),
    -         })),
    -       })
    -     )
    -     .run(tx, { cardIds });
    -   });
    +   await e
    +     .params(
    +       {
    +         name: e.str,
    +         description: e.optional(e.str),
    +         cards: e.array(e.tuple({ front: e.str, back: e.str, order: e.int64 })),
    +       },
    +       (params) =>
    +         e.insert(e.Deck, {
    +           name: params.name,
    +           description: params.description,
    +           cards: e.for(e.array_unpack(params.cards), (card) =>
    +             e.insert(e.Card, {
    +               front: card.front,
    +               back: card.back,
    +               order: card.order,
    +             })
    +           ),
    +         })
    +     )
    +     .run(client, deck);

        revalidatePath("/");
      }

.. edb:split-section::

  Next, update the Server Actions associated with each ``Deck`` object, ``updateDeck``, ``addCard``, and ``deleteCard``. Starting with ``updateDeck``, which is the most complex since it is dynamic. You can set either the ``title`` or ``description`` fields in an update, so we will use the dynamic nature of the query builder to generate separate queries depending on which fields are present in the form data.

  This may look a little intimidating at first, but the part that is making this query dynamic is the ``nameSet`` and ``descriptionSet`` variables. These variables are used to conditionally add the ``name`` or ``description`` fields to the ``set`` parameter of the ``update`` call.

  .. code-block:: typescript-diff
    :caption: app/(authenticated)/deck/[id]/actions.ts

      "use server";

      import { revalidatePath } from "next/cache";
    - import { readFile, writeFile } from "node:fs/promises";
    + import { client } from "@/lib/gel";
    + import e from "@/dbschema/edgeql-js";
      import { Deck } from "@/lib/models";

      export async function updateDeck(formData: FormData) {
        const id = formData.get("id");
        const name = formData.get("name");
        const description = formData.get("description");

        if (
          typeof id !== "string" ||
          (typeof name !== "string" &&
          typeof description !== "string")
        ) {
          return;
        }

    -   const decks = JSON.parse(
    -     await readFile("./decks.json", "utf-8")
    -   ) as Deck[];
    -   decks[index].name = name ?? decks[index].name;
    +   const nameSet = typeof name === "string" ? { name } : {};
    -   decks[index].description = description ?? decks[index].description;
    +   const descriptionSet =
    +     typeof description === "string" ? { description: description || null } : {};

    +   await e
    +     .update(e.Deck, (d) => ({
    +       filter_single: e.op(d.id, "=", id),
    +       set: {
    +         ...nameSet,
    +         ...descriptionSet,
    +       },
    +     })).run(client);
    -   await writeFile("./decks.json", JSON.stringify(decks, null, 2));
        revalidatePath(`/deck/${id}`);
      }

    + const addCardQuery = e.params(
    +   {
    +     front: e.str,
    +     back: e.str,
    +     deckId: e.uuid,
    +   },
    +   (params) => {
    +     const deck = e.assert_exists(
    +       e.select(e.Deck, (d) => ({
    +         filter_single: e.op(d.id, "=", params.deckId),
    +       }))
    +     );
    +
    +     const order = e.cast(e.int64, e.max(deck.cards.order));
    +     return e.insert(e.Card, {
    +       front: params.front,
    +       back: params.back,
    +       deck: e.cast(e.Deck, params.deckId),
    +       order: e.op(order, "+", 1),
    +     });
    +   }
    + );
    +
      export async function addCard(formData: FormData) {
    +   const client = await getAuthenticatedClient();
    +   if (!client) {
    +     return;
    +   }
    +
        const deckId = formData.get("deckId");
        const front = formData.get("front");
        const back = formData.get("back");

        if (
          typeof deckId !== "string" ||
          typeof front !== "string" ||
          typeof back !== "string"
        ) {
          return;
        }

    -   const decks = JSON.parse(await readFile("./decks.json", "utf-8")) as Deck[];
    -
    -   const deck = decks.find((deck) => deck.id === deckId);
    -   if (!deck) {
    -     return;
    -   }
    -
    -   deck.cards.push({ front, back, id: crypto.randomUUID() });
    -   await writeFile("./decks.json", JSON.stringify(decks, null, 2));
    +   await addCardQuery.run(client, {
    +     front,
    +     back,
    +     deckId,
    +   });

        revalidatePath(`/deck/${deckId}`);
      }

    + const deleteCardQuery = e.params({ id: e.uuid }, (params) =>
    +   e.delete(e.Card, (c) => ({
    +     filter_single: e.op(c.id, "=", params.id),
    +   }))
    + );
    +
      export async function deleteCard(formData: FormData) {
    +   const client = await getAuthenticatedClient();
    +   if (!client) {
    +     return;
    +   }
    +
        const cardId = formData.get("cardId");

        if (typeof cardId !== "string") {
          return;
        }

    -   const decks = JSON.parse(await readFile("./decks.json", "utf-8")) as Deck[];
    -   const deck = decks.find((deck) => deck.cards.some((card) => card.id === cardId));
    -   if (!deck) {
    -     return;
    -   }
    -
    -   deck.cards = deck.cards.filter((card) => card.id !== cardId);
    -   await writeFile("./decks.json", JSON.stringify(decks, null, 2));
    +   await deleteCardQuery.run(client, { id: cardId });

        revalidatePath(`/`);
      }

.. edb:split-section::

  Next, update the ``queries.ts`` module to get decks from the database. Notice that the cards are ordered by the ``order`` property.

  .. tabs::

    .. code-tab:: typescript-diff
      :caption: app/queries.ts

      - import { readFile } from "node:fs/promises";
      + import { client } from "@/lib/gel";
      + import e from "@/dbschema/edgeql-js";

      - import { Deck } from "@/lib/models";
      + const getDecksQuery = e.select(e.Deck, (deck) => ({
      +   id: true,
      +   name: true,
      +   description: true,
      +   cards: e.select(deck.cards, (card) => ({
      +     id: true,
      +     front: true,
      +     back: true,
      +     order_by: card.order,
      +   })),
      + }));

        export async function getDecks() {
      -   const decks = JSON.parse(await readFile("./decks.json", "utf-8")) as Deck[];
      +   const decks = await getDecksQuery.run(client);

          return decks;
        }

.. edb:split-section::

  In a terminal, run the Next.js development server.

  .. code-block:: sh

    $ npm run dev

.. edb:split-section::

  A static JSON file to seed your database with a deck of trivia cards is included in the project. Open your browser and navigate to the app at <http://localhost:3000>_. Use the "Import JSON" button to import this JSON file into your database.

  .. image:: https://placehold.co/600x400?text=Show+import+deck+ui
