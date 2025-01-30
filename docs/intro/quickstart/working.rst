.. _ref_quickstart_working:

=====================
Working with our data
=====================

.. edb:split-section::

  Now that we have a schema defined, let's create an API endpoint to insert a ``Deck`` of ``Card`` objects into the database. We'll show you how to query the database by constructing an EdgeQL query string, but we also have a TypeScript query builder that will help you build queries in a type-safe manner. You can switch tabs to see what this same query looks like with our query builder. We will cover how to generate this query builder later in the tutorial.

  .. note::
      If you are seeing TypeScript or ESLint errors, you may need to restart the TypeScript language server, or the ESLint server. Sometimes when adding new files, the language server or ESLint will not pick up the new files until you restart the server. This will be true for the rest of the tutorial, but the majority of development is not creating new files, so after this initial onboarding pain, you'll find that editor tooling works well. This is not a Gel-specific issue, but rather a general issue with starting a new project.

  .. tabs::

    .. code-tab:: typescript
      :caption: app/api/deck/route.ts

        import { NextRequest, NextResponse } from "next/server";
        import { client } from "@/lib/gel";

        interface CreateDeckBody {
          name: string;
          description?: string;
          cards: { front: string; back: string }[];
        }

        interface CreateDeckResponse {
          id: string;
        }

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

        export async function POST(req: NextRequest): Promise<NextResponse<CreateDeckResponse>> {
          // Note: For production, validate the request body with a tool like Zod
          const body = await req.json() as CreateDeckBody;
          const deck = await client.querySingle<CreateDeckResponse>(
            createDeckQuery,
            {
              name: body.name,
              description: body.description,
              cards: body.cards.map((card, index) => ({
                order: index,
                ...card,
              })),
            },
          );
          return NextResponse.json(deck);
        }

    .. code-tab:: typescript
      :caption: With Query Builder

        import { NextRequest, NextResponse } from "next/server";
        import { client } from "@/lib/gel";
        import e from "@/dbschema/edgeql-js";

        interface CreateDeckBody {
          name: string;
          description?: string;
          cards: { order: number; front: string; back: string }[];
        }

        interface CreateDeckResponse {
          id: string;
        }

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

        export async function POST(
          req: NextRequest
        ): Promise<NextResponse<CreateDeckResponse>> {
          // Note: For production, validate the request body with a tool like Zod
          const body = (await req.json()) as CreateDeckBody;
          const deck = await createDeckQuery.run(client, {
            name: body.name,
            description: body.description,
            cards: body.cards,
          });
          return NextResponse.json(deck);
        }



.. edb:split-section::

  Let's make a static JSON file to seed our database with a deck of trivia cards.

  .. code-block:: json
    :caption: trivia-geography.json

      {
        "name": "Geography",
        "description": "Questions about countries, cities, and other geographical features.",
        "cards": [
          {
            "front": "What is the tallest mountain on Earth?",
            "back": "Mount Everest"
          },
          {
            "front": "What is the deepest trench on Earth?",
            "back": "The Mariana Trench"
          },
          {
            "front": "What is the widest river on Earth?",
            "back": "The Amazon River"
          },
          {
            "front": "What is the largest ocean on Earth?",
            "back": "The Pacific Ocean"
          },
          {
            "front": "What is the highest freshwater lake on Earth?",
            "back": "Lake Titicaca"
          }
        ]
      }

.. edb:split-section::

  In one terminal, we will run the Next.js development server.

  .. code-block:: sh

    $ npm run dev

.. edb:split-section::

  We can use a tool like Postman, httpie, or curl to insert the deck into the database using the API endpoint we just created. Since curl is a common tool, here's an example of how to do this. Start a new terminal session, and run this curl command to send the JSON file we created earlier to the API endpoint.

  .. code-block:: sh

      $ curl -X POST \
        --header "Content-Type: application/json" \
        --data @trivia-geography.json \
        http://localhost:3000/api/deck
      {
        "id": "123e4567-e89b-12d3-a456-426614173000"
      }

.. edb:split-section::

  Next, let's define a route to fetch a deck by its ID, which will return an ordered list of cards along with the deck's name and description.

  .. code-block:: typescript
    :caption: app/api/deck/[id]/route.ts

      import { NextRequest, NextResponse } from "next/server";
      import { client } from "@/lib/gel";

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

      const getDeckQuery = `
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
      `;

      export async function GET(
        req: NextRequest,
        { params }: { params: Promise<{ id: string }> }
      ): Promise<NextResponse<GetDeckResponse>> {
        const { id: deckId } = await params;
        const deck = await client.querySingle<GetDeckResponse>(
          getDeckQuery,
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

.. edb:split-section::

  Now we can fetch the deck we created earlier by referencing its ID in the URL.

  .. code-block:: sh

      $ curl http://localhost:3000/api/deck/123e4567-e89b-12d3-a456-426614173000
      {
        "id": "123e4567-e89b-12d3-a456-426614173000",
        "name": "Geography",
        "description": "Questions about countries, cities, and other geographical features.",
        "cards": [
          {
            "id": "123e4567-e89b-12d3-a456-426614174000",
            "front": "What is the tallest mountain on Earth?",
            "back": "Mount Everest"
          },
          {
            "id": "123e4567-e89b-12d3-a456-426614174001",
            "front": "What is the deepest trench on Earth?",
            "back": "The Mariana Trench"
          },
          {
            "id": "123e4567-e89b-12d3-a456-426614174002",
            "front": "What is the widest river on Earth?",
            "back": "The Amazon River"
          },
          {
            "id": "123e4567-e89b-12d3-a456-426614174003",
            "front": "What is the largest ocean on Earth?",
            "back": "The Pacific Ocean"
          },
          {
            "id": "123e4567-e89b-12d3-a456-426614174004",
            "front": "What is the highest freshwater lake on Earth?",
            "back": "Lake Titicaca"
          }
        ]
      }

.. edb:split-section::

  As time goes on, and our planet changes, perhaps we'll want to update one of the cards with the latest in geographical knowledge. Let's add a route to update a card by its ID.

  .. code-block:: typescript
    :caption: app/api/card/[id]/route.ts

      import { NextRequest, NextResponse } from "next/server";
      import { client } from "@/lib/gel";

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

      const updateCardQuery = `
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
      `;

      export async function PUT(
        req: NextRequest,
        { params }: { params: Promise<{ id: string }> }
      ): Promise<NextResponse<UpdateCardResponse>> {
        const { id: cardId } = await params;
        const body = (await req.json()) as UpdateCardBody;
        const card = await client.querySingle<UpdateCardSuccessResponse>(
          updateCardQuery,
          { cardId, front: body.front, back: body.back }
        );

        if (!card) {
          return NextResponse.json({ error: "Card not found" }, { status: 404 });
        }

        return NextResponse.json(card);
      }

.. edb:split-section::

  Now we can update a card by referencing its ID in the URL.

  .. code-block:: sh

      $ curl -X PUT \
        --header "Content-Type: application/json" \
        --data '{"front": "What is the tallest mountain on Earth?", "back": "Mount Quux"}' \
        http://localhost:3000/api/card/123e4567-e89b-12d3-a456-426614174000
      {
        "id": "123e4567-e89b-12d3-a456-426614174000"
      }

.. edb:split-section::

  Now that we have some data of various types in our database, let's explore that data in the UI. We can use the Data Explorer view to see the ``Deck`` and ``Card`` objects we've created and even directly mutate the data.

  .. code-block:: sh

        $ npx gel ui

