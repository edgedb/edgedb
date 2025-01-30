.. _ref_quickstart_access:

=====================
Adding Access Control
=====================

.. edb:split-section::

  Let's add a concept of a user to our application, and update our data model to limit access to the decks and cards to only the user's own decks. Our ``User`` type will be very simple, and for authentication we will use a simple ``AccessToken`` type that gets returned from the user creation endpoint when you make a new user. Gel has some really powerful tools available in our authentication extension, but for now we will just use a simple token that we will store in the database.

  Along with this user type, we will add some ``global`` values that will use the access token provided by the client to set a global ``current_user`` variable that we can use in our queries to limit access to the decks and cards to only the user's own decks.

  .. note::

    Deck creators should be required, but since we are adding this to an existing dataset, we will set the new ``creator`` property to optional. That will effectively make the existing cards and decks invisible since they don't have a creator. We can update the existing data in the database to set the ``creator`` property for all of the existing decks and cards after making the first user, or reinsert the deck and the creator will be set in our updated query.

  .. code-block:: sdl-diff
    :caption: dbschema/default.gel

      module default {
    +   single optional global access_token: str;
    +   single optional global current_user := (
    +     select AccessToken filter .token = access_token
    +   ).user;
    +
    +   type User {
    +     required name: str;
    +
    +     tokens := (select .<user[is AccessToken]);
    +   }
    +
    +   type AccessToken {
    +     required user: User;
    +     required token: str {
    +       constraint exclusive;
    +     };
    +   }
    +
        type Deck {
          required name: str;
          description: str;
    +
    +     creator: User;

          cards := (select .<deck[is Card] order by .order);
    +
    +     access policy creator_has_full_access
    +       allow all
    +       using (
    +         .creator ?= global current_user
    +       );
        };

        type Card {
          required order: int64;
          required front: str;
          required back: str;

          required deck: Deck;
    +
    +     access policy deck_creator_has_full_access
    +       allow all
    +       using (
    +         .deck.creator ?= global current_user
    +       );
        }
      }

.. edb:split-section::

  Let's create the route for creating a new user and getting an access token. Let's start by creating the query to create a new user which will return a randomly generated access token using the ``uuid_generate_v4()`` function. Don't forget to run the code generator after creating the query file.

  .. tabs::

    .. code-tab:: edgeql
      :caption: app/api/user/create-user.edgeql

        with
          name := <str>$name,
          new_user := (
            insert User {
              name := name,
            }
          ),
          new_access_token := (
            insert AccessToken {
              user := new_user,
              token := <str>uuid_generate_v4(),
            }
          ),
        select new_access_token.token;

    .. code-tab:: typescript
      :caption: app/api/user/route.ts

        import { NextRequest, NextResponse } from "next/server";
        import { client } from "@/lib/gel";

        import { createUser } from "./create-user.query";

        interface CreateUserBody {
          name: string;
        }

        interface CreateUserSuccessResponse {
          access_token: string;
        }

        interface CreateUserErrorResponse {
          error: string;
        }

        type CreateUserResponse = CreateUserSuccessResponse | CreateUserErrorResponse;

        export async function POST(req: NextRequest): Promise<NextResponse<CreateUserResponse>> {
          const body = (await req.json()) as CreateUserBody;
          try {
            const access_token = await createUser(client, body.name);

            return NextResponse.json({ access_token });
          } catch (error) {
            console.error(error);
            return NextResponse.json(
              { error: "Failed to create user" },
              { status: 500 }
            );
          }
        }

.. edb:split-section::

  Let's create a new user and get the access token.

  .. code-block:: sh

    $ curl -X POST \
      --header "Content-Type: application/json" \
      --data '{"name": "John Doe"}' \
      http://localhost:3000/api/user
    {
      "access_token": "..."
    }

    $ export FLASHCARDS_ACCESS_TOKEN="..."

.. edb:split-section::

  Next we'll update the create deck query and route with our new authentication logic and ``creator`` property. We add a new function to our ``gel`` library module which will extract our access token from the ``Authorization`` header, and return a new client with the access token global set. That will cause the ``current_user`` global variable to be set to the user that owns the access token.

  .. note::

    We could insist that the ``creator`` link is set by using ``assert_exists`` around our ``global current_user`` in our query, but for now, we'll allow decks to be created without a creator using this query, even though we will block it at the API layer.

  .. tabs::

    .. code-tab:: typescript-diff
      :caption: app/lib/gel.ts

      + import { createClient, type Client } from "gel";
      - import { createClient } from "gel";
      + import { type NextRequest } from "next/server";

        export const client = createClient();

      + export function getAuthenticatedClient(request: NextRequest): Client | null {
      +   const access_token = request.headers.get("Authorization")?.split(" ")[1];
      +   if (!access_token) {
      +     return null;
      +   }
      +   return client.withGlobals({ access_token });
      + }

    .. code-tab:: typescript-diff
      :caption: app/api/deck/route.ts

        import { NextRequest, NextResponse } from "next/server";
      + import { getAuthenticatedClient } from "@/lib/gel";
      - import { client } from "@/lib/gel";

        import { createDeck } from "./create-deck.query";

        interface CreateDeckBody {
          name: string;
          description?: string;
          cards: { front: string; back: string }[];
        }

        interface CreateDeckResponse {
          id: string;
        }

        export async function POST(req: NextRequest): Promise<NextResponse<CreateDeckResponse>> {
      +   const client = getAuthenticatedClient(req);
      +
      +   if (!client) {
      +     return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
      +   }
      +
          // Note: For production, validate the request body with a tool like Zod
          const body = await req.json() as CreateDeckBody;
          const deck = await createDeck(
            client,
            {
              name: body.name,
              description: body.description,
              cards: body.cards,
            },
          );
          return NextResponse.json(deck);
        }

    .. code-tab:: edgeql-diff
      :caption: app/api/deck/create-deck.edgeql

        with
          name := <str>$name,
          description := <optional str>$description,
          cards := enumerate(array_unpack(<array<tuple<front: str, back: str>>>$cards)),
          new_deck := (
            insert Deck {
              name := name,
              description := description,
      +       creator := global current_user,
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

  After running the code generator again, we can create a deck and see that it is created successfully. First we will try to create a deck without an access token and notice that it is rejected. Adding our access token to the request will allow us to create a deck successfully.

  .. code-block:: sh

    $ npx @gel/generate queries

    $ curl -X POST \
        --header "Content-Type: application/json" \
        --data @trivia-geography.json \
        http://localhost:3000/api/deck
    {
      "error": "Unauthorized"
    }

    $ curl -X POST \
        --header "Content-Type: application/json" \
        --header "Authorization: Bearer $FLASHCARDS_ACCESS_TOKEN" \
        --data @trivia-geography.json \
        http://localhost:3000/api/deck
    {
      "id": "..."
      ...
    }

.. edb:split-section::

  Let's update the rest of the application and queries with the authentication logic.

  .. tabs::

    .. code-tab:: typescript-diff
      :caption: app/api/deck/[id]/route.ts

        import { NextRequest, NextResponse } from "next/server";
      + import { getAuthenticatedClient } from "@/lib/gel";
      - import { client } from "@/lib/gel";

        import { getDeck } from "./get-deck.query";

        interface GetDeckSuccessResponse {
          id: string;
          name: string;
          description: string | null;
      +   creator: {
      +     id: string;
      +     name: string;
      +   } | null;
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

        export async function GET(
          req: NextRequest,
          { params }: { params: Promise<{ id: string }> }
        ): Promise<NextResponse<GetDeckResponse>> {
      +   const client = getAuthenticatedClient(req);
      +
      +   if (!client) {
      +     return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
      +   }
      +
          const { id: deckId } = await params;
          const deck = await getDeck(
            client,
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

    .. code-tab:: edgeql-diff
      :caption: app/api/deck/[id]/get-deck.edgeql

        with deckId := <uuid>$deckId,
        select Deck {
          id,
          name,
          description,
      +   creator: {
      +     id,
      +     name,
      +   },
          cards: {
            id,
            front,
            back,
          },
        } filter .id = deckId

    .. code-tab:: typescript-diff
      :caption: app/api/card/[id]/route.ts

        import { NextRequest, NextResponse } from "next/server";
      + import { getAuthenticatedClient } from "@/lib/gel";
      - import { client } from "@/lib/gel";

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

        export async function PUT(
          req: NextRequest,
          { params }: { params: Promise<{ id: string }> }
        ): Promise<NextResponse<UpdateCardResponse>> {
      +   const client = getAuthenticatedClient(req);
      +
      +   if (!client) {
      +     return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
      +   }
      +
          const { id: cardId } = await params;
          const body = (await req.json()) as UpdateCardBody;
          const card = await client.querySingle<UpdateCardSuccessResponse>(
            `
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
            `,
            { cardId, front: body.front, back: body.back }
          );

          if (!card) {
            return NextResponse.json({ error: "Card not found" }, { status: 404 });
          }

          return NextResponse.json(card);
        }

.. edb:split-section::

  Let's run the code generator again to update the generated functions with the changes we made to the query files. Feel free to play around at this point. Make some more decks, create a new user, and try to update a card that you don't own.

  .. code-block:: sh

    $ npx @gel/generate queries


