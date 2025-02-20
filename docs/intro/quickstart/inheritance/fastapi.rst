.. _ref_quickstart_fastapi_inheritance:

========================
Adding shared properties
========================

.. edb:split-section::

  One common pattern in applications is to add shared properties to the schema that are used by multiple objects. For example, you might want to add a ``created_at`` and ``updated_at`` property to every object in your schema. You can do this by adding an abstract type and using it as a mixin for your other object types.

  .. code-block:: sdl-diff
    :caption: dbschema/default.gel

      module default {
    +   abstract type Timestamped {
    +     required created_at: datetime {
    +       default := datetime_of_statement();
    +     };
    +     required updated_at: datetime {
    +       default := datetime_of_statement();
    +     };
    +   }
    +
    -   type Deck {
    +   type Deck extending Timestamped {
          required name: str;
          description: str;

          cards := (
            select .<deck[is Card]
            order by .order
          );
        };

    -   type Card {
    +   type Card extending Timestamped {
          required order: int64;
          required front: str;
          required back: str;

          required deck: Deck;
        }
      }

.. edb:split-section::

  Since you don't have historical data for when these objects were actually created or modified, the migration will fall back to the default values set in the ``Timestamped`` type.

  .. code-block:: sh

    $ gel migration create
    did you create object type 'default::Timestamped'? [y,n,l,c,b,s,q,?]
    > y
    did you alter object type 'default::Card'? [y,n,l,c,b,s,q,?]
    > y
    did you alter object type 'default::Deck'? [y,n,l,c,b,s,q,?]
    > y
    Created /home/strinh/projects/flashcards/dbschema/migrations/00004-m1d2m5n.edgeql, id: m1d2m5n5ajkalyijrxdliioyginonqbtfzihvwdfdmfwodunszstya

    $ gel migrate
    Applying m1d2m5n5ajkalyijrxdliioyginonqbtfzihvwdfdmfwodunszstya (00004-m1d2m5n.edgeql)
    ... parsed
    ... applied

.. edb:split-section::

  Update the ``get_decks`` query to sort the decks by ``updated_at`` in descending order.

  .. code-block:: python-diff
    :caption: main.py

      @app.get("/decks", response_model=List[Deck])
      async def get_decks():
          decks = await client.query("""
              select Deck {
                  id,
                  name,
                  description,
                  cards := (
                      select .cards {
                          id,
                          front,
                          back
                      }
                      order by .order
                  )
              }
    +         order by .updated_at desc
          """)
          return decks
