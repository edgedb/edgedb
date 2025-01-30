.. _ref_quickstart_dynamic:

===============
Dynamic Queries
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
