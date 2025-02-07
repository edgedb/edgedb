.. _ref_quickstart_working:

=====================
Working with our data
=====================

.. edb:split-section::

  With TypeScript, there are three ways to run a query: use a string EdgeQL query, use our ``queries`` generator to turn a string of EdgeQL into a TypeScript function, or use our query builder API to build queries dynamically in a type-safe manner. In the next example, we'll show you each of these methods, but for the rest of the tutorial we'll use the query builder API.

  .. tabs::

    .. code-tab:: sh
      :caption: Query builder

      $ npx @gel/generate edgeql-js

    .. code-tab:: sh
      :caption: Queries generator

      $ npx @gel/generate queries

.. edb:split-section::

  Now that we have a schema defined, let's create a simple page with a button that allows users to import a deck of cards from a JSON file. We'll use Next.js server actions to handle the file upload and insert the data into our database. The JSON file will contain the deck name, optional description, and an array of cards with front and back text.

  .. note::
      If you are seeing TypeScript or ESLint errors, you may need to restart the TypeScript language server, or the ESLint server. Sometimes when adding new files, the language server or ESLint will not pick up the new files until you restart the server. This will be true for the rest of the tutorial, but the majority of development is not creating new files, so after this initial onboarding pain, you'll find that editor tooling works well. This is not a Gel-specific issue, but rather a general issue with starting a new project.

  .. edb:split-point::

  .. tabs::

    .. code-tab:: typescript
      :caption: app/page.tsx

        import { ImportForm } from "./form";

        export default function Page() {
          return <ImportForm />;
        }

    .. code-tab:: typescript
      :caption: app/form.tsx

        "use client";
        import { useTransition, useState } from "react";
        import { importDeck } from "./actions";

        export function ImportForm() {
          const [isPending, startTransition] = useTransition();
          const [importState, setImportState] = useState<
            "idle" | "loading" | "success" | "error"
          >("idle");

          const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
            const file = event.target.files?.[0];
            if (!file) return;

            setImportState("loading");
            startTransition(async () => {
              const deck = await file.text();

              const formData = new FormData();
              formData.set("deck", deck);
              try {
                await importDeck(formData);
                setImportState("success");
                event.target.form?.reset();
              } catch (error) {
                console.error(error);
                setImportState("error");
              }
            });
          };

          return (
            <form>
              <label htmlFor="file">Upload a deck of cards</label>
              {importState === "loading" && <p>Importing...</p>}
              {importState === "success" && <p>Imported successfully</p>}
              {importState === "error" && <p>Error importing</p>}
              <input
                type="file"
                id="file"
                onChange={handleFileChange}
                disabled={isPending}
              />
            </form>
          );
        }


    .. code-tab:: typescript
      :caption: app/actions.ts

        "use server";
        import { client } from "@/lib/gel";
        import { createDeck } from "./create-deck.query";

        export async function importDeck(formData: FormData) {
          const deck = formData.get("deck");
          if (typeof deck !== "string") {
            return;
          }

          await createDeck(client, JSON.parse(deck));
        }

    .. code-tab:: typescript
      :caption: app/create-deck.query.ts (query builder)

        // Run `npm generate edgeql-js` to generate the `e` query builder module.
        import e from "@/dbschema/edgeql-js";

        const createDeckQuery = e.params(
          {
            name: e.str,
            description: e.optional(e.str),
            cards: e.array(e.tuple({ order: e.int64, front: e.str, back: e.str })),
          },
          ({
            cards,
            ...deckData
          }) => {
            const newDeck = e.insert(e.Deck, deckData);
            const newCards = e.for(e.array_unpack(cards), (card) =>
              e.insert(e.Card, {
                ...card,
                deck: newDeck,
              })
            );
            return e.with([newCards], e.select(newDeck));
          }
        );

        export const createDeck = createDeckQuery.run.bind(createDeckQuery);

    .. code-tab:: typescript
      :caption: app/create-deck.query.ts (string query)

        import { type Client } from "@/lib/gel";

        const createDeckQuery = `
          with
            name := <str>$name,
            description := <optional str>$description,
            cards := array_unpack(<array<tuple<front: str, back: str>>>$cards),
            new_deck := (
              insert Deck {
                name := name,
                description := description,
              }
            ),
            new_cards := (
              for card in cards
              insert Card {
                order := card.order,
                front := card.front,
                back := card.back,
                deck := new_deck,
              }
            ),
          select new_deck;
        `;

        export async function createDeck(
          client: Client,
          args: {
            name: string;
            description?: string;
            cards: { order: number; front: string; back: string }[];
          }
        ): Promise<{ id: string }> {
          return client.queryRequiredSingle(createDeckQuery, args);
        }

    .. code-tab:: edgeql
      :caption: app/create-deck.edgeql (queries)

        # Run `npm generate queries` to generate the create-deck.query.ts file.
        with
          name := <str>$name,
          description := <optional str>$description,
          cards := array_unpack(<array<tuple<front: str, back: str>>>$cards),
          new_deck := (
            insert Deck {
              name := name,
              description := description,
            }
          ),
          new_cards := (
            for card in cards
            insert Card {
              order := card.order,
              front := card.front,
              back := card.back,
              deck := new_deck,
            }
          ),
        select new_deck;

.. edb:split-section::

  Let's make a static JSON file to seed our database with a deck of trivia cards.

  .. code-block:: json
    :caption: deck-edgeql.json

      {
        "name": "Learning EdgeQL",
        "description": "A progressive guide to learning EdgeQL and SDL from basics to advanced concepts",
        "cards": [
          {
            "front": "What data structure is used as a container for all values in EdgeQL?",
            "back": "Sets. Even single values are treated as sets with one element (singletons)."
          },
          {
            "front": "Can EdgeQL sets contain the same value multiple times?",
            "back": "Yes, EdgeQL sets are mutli-sets."
          },
          {
            "front": "How does EdgeQL represent no value?",
            "back": "A typed empty set."
          },
          {
            "front": "What are the string scalar types in EdgeQL?",
            "back": "str"
          },
          {
            "front": "What are the numeric scalar types in EdgeQL?",
            "back": "int16, int32, int64, float32, float64, bigint, decimal"
          },
          {
            "front": "By default, are properties of an Object type required?",
            "back": "No, unless marked as required, properties are optional."
          },
          {
            "front": "How do you define a one-to-one relationship between two object types?",
            "back": "You define a single, exclusive link from one of the types to the other."
          },
          {
            "front": "How do you define a one-to-many relationship between two object types?",
            "back": "You define a multi, exclusive link from the one-typed object to the many-typed object."
          },
          {
            "front": "How do you define a many-to-one relationship between two object types?",
            "back": "You define a single, non-exclusive link from the many-type to the one-type."
          },
          {
            "front": "How do you define a many-to-many relationship between two object types?",
            "back": "You define a multi, non-exclusive link from one of the types to the other."
          }
        ]
      }


.. edb:split-section::

  In the terminal, we will run the Next.js development server.

  .. code-block:: sh

    $ npm run dev

.. edb:split-section::

  We should see our app running at http://localhost:3000.

  .. image:: https://placehold.co/600x400?text=Show+import+form+ui

.. edb:split-section::

  Next, let's define a page for viewing a deck of cards, and update our import form to redirect to the deck page after importing.

  .. tabs::

    .. code-tab:: typescript-diff
      :caption: app/actions.ts

        "use server";
      + import { redirect } from "next/navigation";
        import { client } from "@/lib/gel";
        import { createDeck } from "./create-deck.query";

        export async function importDeck(formData: FormData) {
          const deck = formData.get("deck");
          if (typeof deck !== "string") {
            return;
          }

          await createDeck(client, JSON.parse(deck));
      +   redirect(`/deck/${id}`);
        }

    .. code-tab:: typescript
      :caption: app/deck/[id]/page.tsx

        import { notFound } from "next/navigation";
        import { client } from "@/lib/gel";
        import e from "@/dbschema/edgeql-js";
        import { Fragment } from "react";

        const getDeckQuery = e.params({ id: e.uuid }, (params) =>
          e.select(e.Deck, (d) => ({
            filter_single: e.op(d.id, "=", params.id),
            id: true,
            name: true,
            description: true,
            cards: e.select(d["<deck[is Card]"], (c) => ({
              id: true,
              front: true,
              back: true,
              order: true,
              order_by: c.order,
            }))
          }))
        );

        export default async function DeckPage(
          { params }: { params: Promise<{ id: string }> }
        ) {
          const { id } = await params;
          const deck = await getDeckQuery.run(client, { id });

          if (!deck) {
            notFound();
          }

          return (
            <div>
              <h1>{deck.name}</h1>
              <p>{deck.description}</p>
              <dl>
                {deck.cards.map((card) => (
                  <Fragment key={card.id}>
                    <dt>{card.front}</dt>
                    <dd>{card.back}</dd>
                  </Fragment>
                ))}
              </dl>
            </div>
          )
        }

    .. code-tab:: typescript-diff
      :caption: app/form.tsx

        "use client";
      - import { useTransition, useState } from "react";
      + import { useTransition } from "react";
        import { importDeck } from "./actions";

        export function ImportForm() {
          const [isPending, startTransition] = useTransition();
      -   const [importState, setImportState] = useState<
      -     "idle" | "loading" | "success" | "error"
      -   >("idle");

          const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
            const file = event.target.files?.[0];
            if (!file) return;

      -     setImportState("loading");
            startTransition(async () => {
              const deck = await file.text();

              const formData = new FormData();
              formData.set("deck", deck);
              try {
                await importDeck(formData);
      -         setImportState("success");
                event.target.form?.reset();
              } catch (error) {
                console.error(error);
      -         setImportState("error");
              }
            });
          };

          return (
            <form>
              <label htmlFor="file">Upload a deck of cards</label>
      -       {importState === "loading" && <p>Importing...</p>}
      +       {isPending && <p>Importing...</p>}
      -       {importState === "success" && <p>Imported successfully</p>}
      -       {importState === "error" && <p>Error importing</p>}
              <input
                type="file"
                id="file"
                onChange={handleFileChange}
                disabled={isPending}
              />
            </form>
          );
        }


.. edb:split-section::

  Which should look something like this:

  .. image:: https://placehold.co/600x400?text=Show+deck+page

.. edb:split-section::

  Now that we have some data of various types in our database, let's explore that data in the UI. We can use the Data Explorer view to see the ``Deck`` and ``Card`` objects we've created and even directly mutate the data.

  .. code-block:: sh

        $ npx gel ui

