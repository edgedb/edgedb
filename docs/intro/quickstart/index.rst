.. _ref_quickstart:

==========
Quickstart
==========

================
What we'll build
================

Welcome to our quickstart tutorial! Together, we'll create a simple HTTP API for a Flashcards application using Next.js and Gel. This practical project will let users build and manage their own study decks, with each flashcard featuring customizable text on both sides - making it perfect for studying, memorization practice, or creating educational games.

Don't worry if you're new to Gel - we'll have you up and running with a Next.js starter project and a local Gel database in just about 5 minutes. From there, we'll guide you through building the complete application in roughly 60-90 minutes.

Our Flashcards app will be a modern web application with the following features:

* Create, edit and delete flashcard decks
* Add and remove cards from decks
* Display cards with front/back text content
* Simple HTTP API for managing cards and decks
* Clean, type-safe data modeling using Gel's schema system

Before you start, you'll need:

* TypeScript, Next.js, and React experience
* Node.js 20+
* Unix-like OS (Linux, macOS, or WSL)
* Your preferred code editor

Why Gel for Next.js?
====================
This tutorial will show you how Gel enhances your Next.js development workflow by providing a robust data layer that feels natural in a TypeScript environment. Here's why Gel is an ideal choice:

* **Type Safety**: Gel's strict type system catches data errors before runtime
* **Rich Data Modeling**: Object types and links make it natural to model related data
* **Modern Tooling**: First-class TypeScript support and migrations management
* **Performance**: Efficient query execution for complex data relationships
* **Developer Experience**: Clean query language that's more intuitive than raw SQL

You'll learn how Gel's schema system lets you model your data intuitively - a refreshing alternative to mapping between SQL tables and TypeScript types. As we build the application, you'll discover how Gel's query language, EdgeQL, makes complex data operations straightforward and type-safe. We'll also explore how to evolve your schema over time using Gel's migration system.

Need Help?
==========
If you run into issues while following this tutorial:

* Check the `Gel documentation <https://docs.geldata.com>`_
* Visit our `community Discord <https://discord.gg/gel>`_
* File an issue on `GitHub <https://github.com/geldata/gel>`_

Next Steps
==========
Let's set up your development environment and create a new Next.js project.

===========================
Setting up your environment
===========================

.. edb:split-section::

  We will use our project starter CLI to scaffold our Next.js application with everything we need to get started with Gel. This will create a new directory called ``flashcards`` with a fully configured Next.js project and a local Gel database with an empty schema. You should see the test suite pass, indicating that the database instance was created successfully, and we're ready to start building our application.

  .. note::

    If you run into any issues at this point, look back at the output of the ``npm create @gel`` command for any error messages. Feel free to ask for help in the `Gel Discord <https://discord.gg/gel>`_.

  .. code-block:: sh

      $ npm create @gel \
        --environment=nextjs \
        --project-name=flashcards --yes
      $ cd flashcards
      $ npm run test


.. edb:split-section::

  Let's quickly take a poke around the empty database with our CLI REPL.

  .. code-block:: sh

      $ npx gel

.. edb:split-section::

  Try the following queries which will work without any schema defined.

  .. code-block:: edgeql-repl

      db> select 42;
      {42}
      db> select sum({1, 2, 3});
      {6}
      db> with cards := {
        (
          front := "What is the highest mountain in the world?",
          back := "Mount Everest",
        ),
        (
          front := "Which ocean contains the deepest trench on Earth?",
          back := "The Pacific Ocean",
        ),
      }
      select cards order by random() limit 1;
      {
        (
          front := "What is the highest mountain in the world?",
          back := "Mount Everest",
        )
      }

.. edb:split-section::

  Fun! We'll create a proper data model for this in the next step, but for now, let's take a look around the project we've just created. Most of the generated files will be familiar to you if you've worked with Next.js before. So let's focus on the new files that were created to integrate Gel.

  - ``gel.toml``: This is the configuration file for the Gel database. It contains the configuration for the local database instance, so that if another developer on your team wants to run the project, they can easily do so and have a compatible database version.
  - ``dbschema/``: This directory contains the schema for the database, and later supporting files like migrations, and generated code.
  - ``dbschema/default.gel``: This is the default schema file that we'll use to define our data model. It is empty for now, but we'll add our data model to this file in the next step.
  - ``lib/gel.ts``: This file contains the Gel client, which we'll use to interact with the database.

  .. code-block:: sh

    $ tree


=================
Modeling our data
=================

.. edb:split-section::

  Our flashcards application has a simple data model, but it's interesting enough to get a taste of many of the features of the Gel schema language. We have a ``Card`` type that describes an single flashcard, which for now contains two required string properties: ``front`` and ``back``. Each ``Card`` belongs to a ``Deck``, and there is a natural ordering to the cards in a given deck.

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

  .. code-block:: typescript-diff
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

.. edb:split-section::

  And now the query file.

  .. code-block:: edgeql
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

  And finally, we'll move the update card query into a separate file.

  .. code-block:: typescript-diff
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

.. edb:split-section::

  And now here is the same query moved into a separate file.

  .. code-block:: edgeql
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

Adding some access control
==========================

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

  Let's create the route for creating a new user and getting an access token. Let's start by creating the query to create a new user which will return a randomly generated access token using the ``uuid_generate_v4()`` function.

  .. code-block:: edgeql
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

.. edb:split-section::

  Now we can generate the query file.

  .. code-block:: sh

      $ npx @gel/generate queries

.. edb:split-section::

  Now we can create the route for creating a new user.

  .. code-block:: typescript
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


Adding some shared properties
=============================

.. edb:split-section::

  One common pattern in applications is to add shared properties to the schema that are used by multiple modules. For example, we might want to add a ``created_at`` and ``updated_at`` property to every object in our schema. We can do this by adding an abstract type and using it as a mixin for our other object types.

  .. code-block:: sdl-diff
    :caption: dbschema/default.gel

      module default {
        single optional global access_token: str;
        single optional global current_user := (
          select AccessToken filter .token = access_token
        ).user;

    +   abstract type Timestamped {
    +     required created_at: datetime {
    +       default := datetime_of_statement();
    +     };
    +     required updated_at: datetime {
    +       default := datetime_of_statement();
    +     };
    +   }
    +
    -   type User {
    +   type User extends Timestamped {
          required name: str;

          tokens := (select .<user[is AccessToken]);
        }

    -   type AccessToken {
    +   type AccessToken extends Timestamped {
          required user: User;
          required token: str {
            constraint exclusive;
          };
        }

    -   type Deck {
    +   type Deck extends Timestamped {
          required name: str;
          description: str;

          creator: User;

          cards := (select .<deck[is Card] order by .order);

          access policy creator_has_full_access
            allow all
            using (
              .creator ?= global current_user
            );
        };

    -   type Card {
    +   type Card extends Timestamped {
          required order: int64;
          required front: str;
          required back: str;

          required deck: Deck;

          access policy deck_creator_has_full_access
            allow all
            using (
              .deck.creator ?= global current_user
            );
        }
      }

.. edb:split-section::

  This will require that we make a manual migration since we will need to backfill the ``created_at`` and ``updated_at`` properties for all existing objects. We will just set the value to be the current wall time since we do not have a meaningful way to backfill the values for existing objects.

  .. code-block:: sh

    $ npx gel migration create
    fill_expr> datetime_of_statement()

    $ npx gel migrate

.. edb:split-section::

  Now when we look at the data in the UI, we will see the new properties on each of our object types.

Dynamic queries
===============

.. edb:split-section::

  Maybe we only want to update one side of an existing card, or just edit the description of a deck. One approach is writing a very complicated single query that tries to handle all of the dynamic cases. Another approach is to build the query dynamically in the application code. This has the benefit of often being better for performance, and it's easier to understand and maintain. We provide another very powerful code generator, our TypeScript query builder, that allows you to build queries dynamically in the application code, while giving you strict type safety.

  First, we will generate the query builder. This will generate a module in our ``dbschema`` directory called ``edgeql-js``, which we can import in our route and use to build a dynamic query.

  .. code-block:: sh

    $ npx @gel/generate edgeql-js


.. edb:split-section::

  Now let's use the query builder in a new route for updating a deck's ``name`` and/or ``description``. We will treat the request body as a partial update, and only update the fields that are provided. Since the description is optional, we will use a nullable string for the type, so you can "unset" the description by passing in ``null``.

  .. code-block:: typescript-diff
    :caption: app/api/deck/[id]/route.ts

      import { NextRequest, NextResponse } from "next/server";
      import { getAuthenticatedClient } from "@/lib/gel";
    + import e from "@/dbschema/edgeql-js";

      import { getDeck } from "./get-deck.query";

      interface GetDeckSuccessResponse {
        id: string;
        name: string;
        description: string | null;
        creator: {
          id: string;
          name: string;
        } | null;
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
        const client = getAuthenticatedClient(req);

        if (!client) {
          return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
        }

        const { id: deckId } = await params;
        const deck = await getDeck(client, { deckId });

        if (!deck) {
          return NextResponse.json(
            { error: `Deck (${deckId}) not found` },
            { status: 404 }
          );
        }

        return NextResponse.json(deck);
      }

    + interface UpdateDeckBody {
    +   name?: string;
    +   description?: string | null;
    + }
    +
    + interface UpdateDeckSuccessResponse {
    +   id: string;
    + }
    +
    + interface UpdateDeckErrorResponse {
    +   error: string;
    + }
    +
    + type UpdateDeckResponse = UpdateDeckSuccessResponse | UpdateDeckErrorResponse;
    +
    + export async function PATCH(
    +   req: NextRequest,
    +   { params }: { params: Promise<{ id: string }> }
    + ): Promise<NextResponse<UpdateDeckResponse>> {
    +   const client = getAuthenticatedClient(req);
    +
    +   if (!client) {
    +     return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    +   }
    +
    +   const { id: deckId } = await params;
    +   const body = (await req.json()) as UpdateDeckBody;
    +
    +   const nameSet = body.name !== undefined ? { name: body.name } : {};
    +   const descriptionSet =
    +     body.description !== undefined ? { description: body.description } : {};
    +
    +   const updated = await e
    +     .update(e.Deck, (deck) => ({
    +       filter_single: e.op(deck.id, "=", deckId),
    +       set: {
    +         ...nameSet,
    +         ...descriptionSet,
    +       },
    +     }))
    +     .run(client);
    +
    +   if (!updated) {
    +     return NextResponse.json(
    +       { error: `Deck (${deckId}) not found` },
    +       { status: 404 }
    +     );
    +   }
    +
    +   return NextResponse.json(updated);
    + }
