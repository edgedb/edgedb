.. _ref_quickstart_workflow:

===============================
A Smoother Development Workflow
===============================

.. edb:split-section::

  We'd like to make a few changes to the data model, but before we do, we're going to explore a slightly different workflow that leverages a few unique features of Gel: our watch mode, schema hooks, and code generation.

  Let's first move our two queries into separate EdgeQL files, and set up our code generation tool to generate fully-typed functions for each query. Starting with the query to create a deck with cards.

  .. code-block:: typescript-diff
    :caption: app/api/deck/route.ts

      import { NextRequest, NextResponse } from "next/server";
      import { client } from "@/lib/gel";
    +
    + import { createDeck } from "./create-deck.query";

      interface CreateDeckBody {
        name: string;
        description?: string;
        cards: { front: string; back: string }[];
      }

      interface CreateDeckResponse {
        id: string;
      }

    - const createDeckQuery = `
    -   with
    -     name := <str>$name,
    -     description := <optional str>$description,
    -     cards := array_unpack(<array<tuple<front: str, back: str>>>$cards),
    -     new_deck := (
    -       insert Deck {
    -         name := name,
    -         description := description,
    -       }
    -     ),
    -     new_cards := (
    -       for card in cards
    -       insert Card {
    -         order := card.order,
    -         front := card.front,
    -         back := card.back,
    -         deck := new_deck,
    -       }
    -     ),
    -   select new_deck;
    - `;
    -
      export async function POST(req: NextRequest): Promise<NextResponse<CreateDeckResponse>> {
        // Note: For production, validate the request body with a tool like Zod
        const body = await req.json() as CreateDeckBody;
    -   const deck = await client.querySingle<CreateDeckResponse>(
    +   const deck = await createDeck(
    -     createDeckQuery,
    +     client,
          {
            name: body.name,
            description: body.description,
            cards: body.cards,
          },
        );
        return NextResponse.json(deck);
      }

.. edb:split-section::

  After removing the query from the route file, we move it into a separate file.

  .. code-block:: edgeql
    :caption: app/api/deck/create-deck.edgeql

      with
        name := <str>$name,
        description := <optional str>$description,
        cards := enumerate(array_unpack(<array<tuple<front: str, back: str>>>$cards)),
        new_deck := (
          insert Deck {
            name := name,
            description := description,
          }
        ),
        new_cards := (
          for card in cards
          insert Card {
            order := card.0,
            front := card.1.front,
            back := card.1.back,
            deck := new_deck,
          }
        ),
      select new_deck;

.. edb:split-section::

  We will do the same for the query to fetch a deck by its ID.

  .. tabs::

    .. code-tab:: typescript-diff
      :caption: app/api/deck/[id]/route.ts

        import { NextRequest, NextResponse } from "next/server";
        import { client } from "@/lib/gel";
      +
      + import { getDeck } from "./get-deck.query";

        interface GetDeckSuccessResponse {
          id: string;
          name: string;
          description: string | null;
          cards: {
            id: string;
            front: string;
            back: string;
          }[];
        }

        interface GetDeckErrorResponse {
          error: string;
        }

        type GetDeckResponse = GetDeckSuccessResponse | GetDeckErrorResponse;

      - const getDeckQuery = `
      -   with deckId := <uuid>$deckId,
      -   select Deck {
      -     id,
      -     name,
      -     description,
      -     cards := (select .<deck[is Card] {
      -       id,
      -       front,
      -       back,
      -     } order by .order),
      -   } filter .id = deckId
      - `;
      -
        export async function GET(
          req: NextRequest,
          { params }: { params: Promise<{ id: string }> }
        ): Promise<NextResponse<GetDeckResponse>> {
          const { id: deckId } = await params;
      -   const deck = await client.querySingle<GetDeckResponse>(
      +   const deck = await getDeck(
      -     getDeckQuery,
      +     client,
            { deckId }
          );

          if (!deck) {
            return NextResponse.json(
              { error: `Deck (${deckId}) not found` },
              { status: 404 }
            );
          }

          return NextResponse.json(deck);
        }

    .. code-tab:: edgeql
      :caption: app/api/deck/[id]/get-deck.edgeql


        with deckId := <uuid>$deckId,
        select Deck {
          id,
          name,
          description,
          cards := (select .<deck[is Card] {
            id,
            front,
            back,
          } order by .order),
        } filter .id = deckId

.. edb:split-section::

  And the same again for the update card route and query.

  .. tabs::

    .. code-tab:: typescript-diff
      :caption: app/api/card/[id]/route.ts

        import { NextRequest, NextResponse } from "next/server";
        import { client } from "@/lib/gel";
      +
      + import { updateCard } from "./update-card.query";

        interface UpdateCardBody {
          front: string;
          back: string;
        }

        interface UpdateCardSuccessResponse {
          id: string;
        }

        interface UpdateCardErrorResponse {
          error: string;
        }

        type UpdateCardResponse = UpdateCardSuccessResponse | UpdateCardErrorResponse;

      - const updateCardQuery = `
      -   with
      -     cardId := <uuid>$cardId,
      -     front := <str>$front,
      -     back := <str>$back,
      -   update Card
      -   filter .id = cardId
      -   set {
      -     front := front,
      -     back := back,
      -   };
      - `;
      -
        export async function PUT(
          req: NextRequest,
          { params }: { params: Promise<{ id: string }> }
        ): Promise<NextResponse<UpdateCardResponse>> {
          const { id: cardId } = await params;
          const body = (await req.json()) as UpdateCardBody;
      -   const card = await client.querySingle<UpdateCardSuccessResponse>(
      +   const card = await updateCard(
      -     updateCardQuery,
      +     client,
            { cardId, front: body.front, back: body.back }
          );

          if (!card) {
            return NextResponse.json({ error: "Card not found" }, { status: 404 });
          }

          return NextResponse.json(card);
        }

    .. code-tab:: edgeql
      :caption: app/api/card/[id]/update-card.edgeql

        with
          cardId := <uuid>$cardId,
          front := <str>$front,
          back := <str>$back,
        update Card
        filter .id = cardId
        set {
          front := front,
          back := back,
        };

.. edb:split-section::

  Now that the queries are in separate files, we can generate the functions for each query. This will create a file next to the EdgeQL file with a fully type-safe function based on introspecting the query with the server.

  .. code-block:: sh

      $ npx @gel/generate queries

.. edb:split-section::

  We will need to run this command any time the schema changes, and we plan on making a few changes in this section, so let's set up a schema change hook in our ``gel.toml`` to ensure that any schema changes will regenerate the query files automatically for you.

  .. code-block:: toml-diff
    :caption: gel.toml

      [gel]
      server-version = 6.0

      [project-hooks]
    + migration.apply.after = "npx @edgedb/generate queries"

.. edb:split-section::

  With all of that out of the way, let's start a new terminal session, and start a watch mode process that will automatically apply our schema changes to the database as we update the schema file. The hook will run after each change, so we can make changes to the schema file and see the changes applied to the database immediately.

  .. code-block:: sh

      $ npx gel watch

.. edb:split-section::

  Our first change will be to add a property to our ``Deck`` type that stores the link to all of the cards in the deck. We will create a computed property, and use a back link from the ``Card`` type to the ``Deck`` type.

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

  You'll notice that in our ``getDeck`` query, we are defining this ``cards`` property explicitly. Now that we've added the computed property, we can remove the explicit definition. We'll need to run the ``generate queries`` command again to regenerate the query files.

  .. code-block:: edgeql-diff
    :caption: app/api/deck/[id]/get-deck.edgeql

      with deckId := <uuid>$deckId,
      select Deck {
        id,
        name,
        description,
    -   cards := (select .<deck[is Card] {
    +   cards: {
          id,
          front,
          back,
    -   } order by .order),
    +   },
      } filter .id = deckId
