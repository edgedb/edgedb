.. _ref_quickstart_dynamic:

===============
Dynamic Queries
===============

When updating data, we often want to modify only specific fields while leaving others unchanged. For example, we might want to update just the front text of a flashcard or only the description of a deck. There are two main approaches to handle these partial updates:

1. Write a single complex query that conditionally handles optional parameters
2. Build the query dynamically in the application code based on which fields need updating

The second approach using dynamic queries tends to be more performant and maintainable. EdgeDB's TypeScript query builder excels at this use case. It allows you to construct queries dynamically while maintaining full type safety. Let's see how to implement this pattern.

.. edb:split-section::

  Let's create a server action that updates a deck's ``name`` and/or ``description``. Since the description is optional, we will treat clearing the ``description`` form field as unsetting the ``description`` property.

  Let's update the deck page to allow updating a deck's ``name`` and/or ``description``. We will treat the request body as a partial update, and only update the fields that are provided. Since the description is optional, we will treat clearing the ``description`` form field as unsetting the ``description`` property.

  .. tabs::

    .. code-tab:: typescript
      :caption: app/deck/[id]/actions.ts

        "use server";

        import { revalidatePath } from "next/cache";
        import e from "@/dbschema/edgeql-js";

        export async function updateDeck(data: FormData) {
          const id = data.get("id");
          if (!id) {
            return;
          }

          const name = data.get("name");
          const description = data.get("description");

          const nameSet = typeof name === "string" ? { name } : {};
          const descriptionSet =
            typeof description === "string"
              ? { description: description || null }
              : {};

          await e
            .update(e.Deck, (d) => ({
              filter_single: e.op(d.id, "=", id),
              set: {
                ...nameSet,
                ...descriptionSet,
              },
            }))
            .run(client);

          revalidatePath(`/deck/${id}`);
        }

    .. code-tab:: typescript-diff
      :caption: app/deck/[id]/page.tsx

        import { redirect } from "next/navigation";
        import { getAuthenticatedClient } from "@/lib/gel";
        import e from "@/dbschema/edgeql-js";
      + import { updateDeck } from "./actions";

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
            creator: {
              id: true,
              name: true,
            },
          }))
        );

        export default async function DeckPage(
          { params }: { params: Promise<{ id: string }> }
        ) {
          const { id: deckId } = await params;
          const client = await getAuthenticatedClient();
          if (!client) {
            redirect("/signup");
          }

          const deck = await getDeckQuery.run(client, { deckId });

          if (!deck) {
            redirect("/");
          }

          return (
            <div>
      -       <h1>{deck.name}</h1>
      -       <p>{deck.description}</p>
      +       <form action={updateDeck}>
      +         <input
      +           type="hidden"
      +           name="id"
      +           value={deck.id}
      +         />
      +         <input
      +           name="name"
      +           initialValue={deck.name}
      +         />
      +         <textarea
      +           name="description"
      +           initialValue={deck.description}
      +         />
      +         <button type="submit">Update</button>
      +       </form>
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
