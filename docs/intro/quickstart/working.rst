.. _ref_quickstart_working:

=====================
Working with the data
=====================

In this section, you will update the existing application to use |Gel| to store and query data, instead of a static JSON file. Having a working application with mock data allows you to focus on learning how |Gel| works, without getting bogged down by the details of the application.

.. edb:split-section::

  Begin by updating the server action to import a deck with cards. Loop through each card in the deck and insert it, building an array of IDs as you go. This array of IDs will be used to set the ``cards`` link on the ``Deck`` object after all cards have been inserted.

  The array of card IDs is initially an array of strings. To satisfy the |Gel| type system, which expects the ``id`` property of ``Card`` objects to be a ``uuid`` rather than a ``str``, you need to cast the array of strings to an array of UUIDs. Use the ``e.literal(e.array(e.uuid), cardIds)`` function to perform this casting.

  The function ``e.includes(cardIdsLiteral, c.id)`` from our standard library checks if a value is present in an array and returns a boolean. When inserting the ``Deck`` object, set the ``cards`` to the result of selecting only the ``Card`` objects whose ``id`` is included in the ``cardIds`` array.

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
    +   const cardIdsLiteral = e.literal(e.array(e.uuid), cardIds);
    +
    +   await e.insert(e.Deck, {
    +     name: deck.name,
    +     description: deck.description,
    +     cards: e.select(e.Card, (c) => ({
    +       filter: e.contains(cardIdsLiteral, c.id),
    +     })),
    +   }).run(client);

        revalidatePath("/");
      }

.. edb:split-section::

  This works, but you might notice that it is not atomic. If one of the ``Card`` objects fails to insert, the entire operation will fail and the ``Deck`` will not be inserted. To make this operation atomic, update the ``importDeck`` action to use a transaction.

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
    -         .run(client);
    +         .run(tx);

            cardIds.push(createdCard.id);
          }

          const cardIdsLiteral = e.literal(e.array(e.uuid), cardIds);

          await e.insert(e.Deck, {
            name: deck.name,
            description: deck.description,
            cards: e.select(e.Card, (c) => ({
              filter: e.contains(cardIdsLiteral, c.id),
            })),
    -     }).run(client);
    +     }).run(tx);
    +   });

        revalidatePath("/");
      }

.. edb:split-section::

  You might think this is as good as it gets, and many ORMs will create a similar set of queries. However, with the query builder, you can improve this by crafting a single query that inserts the ``Deck`` and ``Card`` objects, along with their links, in one efficient query.

  The first thing to notice is that the ``e.params`` function is used to define parameters for your query instead of embedding literal values directly. This approach eliminates the need for casting, as was necessary with the ``cardIds`` array. By defining the ``cards`` parameter as an array of tuples, you ensure full type safety with both TypeScript and the database.

  Another key feature of this query builder expression is the ``e.for(e.array_unpack(params.cards), (card) => {...})`` construct. This expression converts the array of tuples into a set of tuples and generates a set containing an expression for each element. Essentially, you assign the ``Deck.cards`` set of ``Card`` objects to the result of inserting each element from the ``cards`` array. This is similar to what you were doing before by selecting all ``Card`` objects by their ``id``, but is more efficient since you are inserting the ``Deck`` and all ``Card`` objects in one query.

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
    -     const cardIds: string[] = [];
    -     for (const card of deck.cards) {
    -       const createdCard = await e
    -         .insert(e.Card, {
    -           front: card.front,
    -           back: card.back,
    -           order: card.order,
    -         })
    -         .run(tx);
    -
    -       cardIds.push(createdCard.id);
    -     }
    -
    -     const cardIdsLiteral = e.literal(e.array(e.uuid), cardIds);
    -
    -     await e.insert(e.Deck, {
    -       name: deck.name,
    -       description: deck.description,
    -       cards: e.select(e.Card, (c) => ({
    -         filter: e.contains(cardIdsLiteral, c.id),
    -       })),
    -     }).run(tx);
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

  Next, you will update the Server Actions for each ``Deck`` object: ``updateDeck``, ``addCard``, and ``deleteCard``. Start with ``updateDeck``, which is the most complex because it is dynamic. You can set either the ``title`` or ``description`` fields in an update. Use the dynamic nature of the query builder to generate separate queries based on which fields are present in the form data.

  This may seem a bit intimidating at first, but the key to making this query dynamic is the ``nameSet`` and ``descriptionSet`` variables. These variables conditionally add the ``name`` or ``description`` fields to the ``set`` parameter of the ``update`` call.

  .. code-block:: typescript-diff
    :caption: app/deck/[id]/actions.ts

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

      export async function addCard(formData: FormData) {
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
    +   await e
    +     .params(
    +       {
    +         front: e.str,
    +         back: e.str,
    +         deckId: e.uuid,
    +       },
    +       (params) => {
    +         const deck = e.assert_exists(
    +           e.select(e.Deck, (d) => ({
    +             filter_single: e.op(d.id, "=", params.deckId),
    +           }))
    +         );
    +
    +         const order = e.cast(e.int64, e.max(deck.cards.order));
    +         const card = e.insert(e.Card, {
    +           front: params.front,
    +           back: params.back,
    +           order: e.op(order, "+", 1),
    +         });
    +         return e.update(deck, (d) => ({
    +           set: {
    +             cards: {
    +               "+=": card
    +             },
    +           },
    +         }))
    +       }
    +     )
    +     .run(client, {
    +       front,
    +       back,
    +       deckId,
    +     });

        revalidatePath(`/deck/${deckId}`);
      }

      export async function deleteCard(formData: FormData) {
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
    +   await e
    +     .params({ id: e.uuid }, (params) =>
    +       e.delete(e.Card, (c) => ({
    +         filter_single: e.op(c.id, "=", params.id),
    +       }))
    +     )
    +     .run(client, { id: cardId });
    +

        revalidatePath(`/`);
      }

.. edb:split-section::

  Next, update the two ``queries.ts`` methods: ``getDecks`` and ``getDeck``.

  .. tabs::

    .. code-tab:: typescript-diff
      :caption: app/queries.ts

      - import { readFile } from "node:fs/promises";
      + import { client } from "@/lib/gel";
      + import e from "@/dbschema/edgeql-js";
      -
      - import { Deck } from "@/lib/models";

        export async function getDecks() {
      -   const decks = JSON.parse(await readFile("./decks.json", "utf-8")) as Deck[];
      +   const decks = await e.select(e.Deck, (deck) => ({
      +     id: true,
      +     name: true,
      +     description: true,
      +     cards: e.select(deck.cards, (card) => ({
      +       id: true,
      +       front: true,
      +       back: true,
      +       order_by: card.order,
      +     })),
      +   })).run(client);

          return decks;
        }

    .. code-tab:: typescript-diff
      :caption: app/deck/[id]/queries.ts

      - import { readFile } from "node:fs/promises";
      - import { Deck } from "@/lib/models";
      + import { client } from "@/lib/gel";
      + import e from "@/dbschema/edgeql-js";

        export async function getDeck({ id }: { id: string }) {
      -   const decks = JSON.parse(await readFile("./decks.json", "utf-8")) as Deck[];
      -   return decks.find((deck) => deck.id === id) ?? null;
      +   return await e
      +     .select(e.Deck, (deck) => ({
      +       filter_single: e.op(deck.id, "=", id),
      +       id: true,
      +       name: true,
      +       description: true,
      +       cards: e.select(deck.cards, (card) => ({
      +         id: true,
      +         front: true,
      +         back: true,
      +         order_by: card.order,
      +       })),
      +     }))
      +     .run(client);
        }

.. edb:split-section::

  In a terminal, run the Next.js development server.

  .. code-block:: sh

    $ npm run dev

.. edb:split-section::

  A static JSON file to seed your database with a deck of trivia cards is included in the project. Open your browser and navigate to the app at <http://localhost:3000>_. Use the "Import JSON" button to import this JSON file into your database.

  .. image:: https://placehold.co/600x400?text=Show+import+deck+ui
