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
    +   single optional global access_token: uuid;
    +   single optional global current_user := (
    +     select AccessToken filter .id = global access_token
    +   ).user;
    +
    +   type User {
    +     required name: str;
    +   }
    +
    +   type AccessToken {
    +     required user: User;
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

  Let's create a page for creating a new user and getting an access token. Let's start by creating the query to create a new user which will return the ``AccessToken.id`` which we will use as the access token itself. We will save this access token in a cookie so that we can authenticate requests in other server actions and route handlers.

  .. tabs::

    .. code-tab:: typescript
      :caption: app/signup/actions.ts

        "use server";

        import { redirect } from "next/navigation";
        import { cookies } from "next/headers";

        import { client } from "@/lib/gel";
        import e from "@/dbschema/edgeql-js";

        const createUser = e.params(
          {
            name: e.str,
          },
          (params) =>
            e.insert(e.AccessToken, {
              user: e.insert(e.User, { name: params.name }),
            })
        );

        export async function signUp(formData: FormData) {
          const name = formData.get("name");
          if (typeof name !== "string") {
            console.error("Name is required");
            return;
          }

          const access_token = await createUser(client, { name });
          (await cookies()).set("flashcards_access_token", access_token.id);
          redirect("/");
        }


    .. code-tab:: typescript
      :caption: app/signup/page.tsx

        import { Button } from "@/components/ui/button";
        import {
          Card,
          CardContent,
          CardDescription,
          CardHeader,
          CardTitle,
        } from "@/components/ui/card";
        import { Input } from "@/components/ui/input";
        import { Label } from "@/components/ui/label";

        import { signUp } from "./actions";

        export default function SignUpPage() {
          return (
            <div className="flex flex-col items-center justify-center gap-6">
              <Card className="w-full max-w-md">
                <CardHeader>
                  <CardTitle className="text-2xl">Sign Up</CardTitle>
                  <CardDescription>
                    Enter your name below to create an account
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <form action={signUp}>
                    <div className="flex flex-col gap-6">
                      <div className="grid gap-2">
                        <Label htmlFor="name">Name</Label>
                        <Input
                          id="name"
                          name="name"
                          type="text"
                          placeholder="John Doe"
                          required
                        />
                      </div>
                      <Button type="submit" className="w-full">
                        Sign Up
                      </Button>
                    </div>
                  </form>
                </CardContent>
              </Card>
            </div>
          );
        }


.. edb:split-section::

  We should see this page when we navigate to the signup page.

  .. code-block:: sh

    $ echo

Limiting access
===============

.. edb:split-section::

  Now that we have our access token in a cookie, we can create a helper function to extract it and add it as a global to our client.

  .. code-block:: typescript-diff
    :caption: app/lib/gel.ts

    + import { createClient, type Client } from "gel";
    - import { createClient } from "gel";
    + import { cookies } from "next/headers";

      export const client = createClient();

    + export async function getAuthenticatedClient(): Promise<Client | null> {
    +   const access_token = (await cookies()).get("flashcards_access_token")?.value;
    +   if (!access_token) {
    +     return null;
    +   }
    +   return client.withGlobals({ access_token });
    + }

.. edb:split-section::

  Along with allowing us to take advantage of our access policies in our queries, this will also allow us to redirect unauthenticated users to the signup page from any of our pages which should require authentication. Let's update our ``page.tsx`` file to redirect to the signup page if the user is not authenticated.

  .. code-block:: typescript-diff
    :caption: app/page.tsx

      import { ImportForm } from "./form";
    + import { getAuthenticatedClient } from "@/lib/gel";
    + import { redirect } from "next/navigation";

      export default async function Page() {
    +   const client = await getAuthenticatedClient();
    +   if (!client) {
    +     redirect("/signup");
    +   }
    +
        return <ImportForm />;
      }

.. edb:split-section::

  Next we'll update the create deck query and server action with our new authentication logic and ``creator`` property.

  .. tabs::

    .. code-tab:: typescript-diff
      :caption: app/actions.ts

        "use server";
        import { redirect } from "next/navigation";
      - import { client } from "@/lib/gel";
      + import { getAuthenticatedClient } from "@/lib/gel";
        import { createDeck } from "./create-deck.query";

        export async function createDeck(formData: FormData) {
          const deck = formData.get("deck");
          if (typeof deck !== "string") {
            return;
          }

          const client = await getAuthenticatedClient();
          if (!client) {
            return;
          }

          const { id } = await createDeck(client, JSON.parse(deck));
          redirect(`/deck/${id}`);
        }

    .. code-tab:: typescript-diff
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
      -     const newDeck = e.insert(e.Deck, deckData);
      +     const newDeck = e.insert(e.Deck, {
      +       ...deckData,
      +       creator: e.assert_exists(e.global.current_user),
      +     });
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

.. edb:split-section::

  Finally, let's update the deck page to require an authenticated user, and to return the deck's creator.

  .. code-block:: typescript-diff
    :caption: app/deck/[id]/page.tsx

      import { redirect } from "next/navigation";
    - import { client } from "@/lib/gel";
    + import { getAuthenticatedClient } from "@/lib/gel";
      import e from "@/dbschema/edgeql-js";

      const getDeckQuery = e.params({ deckId: e.uuid }, (params) =>
        e.select(e.Deck, (d) => ({
          filter_single: e.op(d.id, "=", params.deckId),
          id: true,
          name: true,
          description: true,
          cards: {
            id: true,
            front: true,
            back: true,
            order: true,
          },
    +     creator: {
    +       id: true,
    +       name: true,
    +     },
        }))
      );

      export default async function DeckPage(
        { params }: { params: Promise<{ id: string }> }
      ) {
        const { id: deckId } = await params;
    +   const client = await getAuthenticatedClient();
    +   if (!client) {
    +     redirect("/signup");
    +   }

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
