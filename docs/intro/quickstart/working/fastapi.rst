.. _ref_quickstart_fastapi_working:

=====================
Working with the data
=====================

In this section, you will update the existing FastAPI application to use |Gel| to store and query data, instead of a JSON file. Having a working application with mock data allows you to focus on learning how |Gel| works, without getting bogged down by the details of the application.

Bulk importing of data
======================

.. edb:split-section::

  First, you need to update the imports and Pydantic models to use UUID instead of string for ID fields, since this is what |Gel| returns. You also need to initialize the |Gel| client and import the asyncio module to work with async functions.

  .. code-block:: python-diff
    :caption: main.py

      from fastapi import FastAPI, HTTPException
      from pydantic import BaseModel
      from typing import List, Optional
    - import json
    - from pathlib import Path
    + from uuid import UUID
    + from gel import create_async_client
    + import asyncio

      app = FastAPI(title="Flashcards API")

      # Pydantic models
      class CardBase(BaseModel):
          front: str
          back: str

      class Card(CardBase):
    -     id: str
    +     id: UUID

      class DeckBase(BaseModel):
          name: str
          description: Optional[str] = None

      class DeckCreate(DeckBase):
          cards: List[CardBase]

      class Deck(DeckBase):
    -     id: str
    +     id: UUID
          cards: List[Card]

    - DATA_DIR = Path(__file__).parent / "data"
    - DECKS_FILE = DATA_DIR / "decks.json"
    + client = create_async_client()


.. edb:split-section::

   Next, update the deck import operation to use |Gel| to create the deck and cards. The operation creates cards first, then creates a deck with links to the cards. Finally, it fetches the newly created deck with all required fields.

   .. note::

      Notice the ``{ ** }`` in the query. This is a shorthand for selecting all fields of the object. It's useful when you want to return the entire object without specifying each field. In our case, we want to return the entire deck object with all the nested fields.

   .. code-block:: python-diff
    :caption: main.py

      from fastapi import FastAPI, HTTPException
      from pydantic import BaseModel
      from typing import List, Optional
      from uuid import UUID
      from gel import create_async_client
      import asyncio

      app = FastAPI(title="Flashcards API")

      # Pydantic models
      class CardBase(BaseModel):
          front: str
          back: str

      class Card(CardBase):
          id: UUID

      class DeckBase(BaseModel):
          name: str
          description: Optional[str] = None

      class DeckCreate(DeckBase):
          cards: List[CardBase]

      class Deck(DeckBase):
          id: UUID
          cards: List[Card]

      client = create_client()

    - DATA_DIR.mkdir(exist_ok=True)
    - if not DECKS_FILE.exists():
    -     DECKS_FILE.write_text("[]")

    - def read_decks() -> List[Deck]:
    -     content = DECKS_FILE.read_text()
    -     data = json.loads(content)
    -     return [Deck(**deck) for deck in data]
    -
    - def write_decks(decks: List[Deck]) -> None:
    -     data = [deck.model_dump() for deck in decks]
    -     DECKS_FILE.write_text(json.dumps(data, indent=2))

      @app.post("/decks/import", response_model=Deck)
      async def import_deck(deck: DeckCreate):
    -     decks = read_decks()
    -     new_deck = Deck(
    -         id=str(uuid.uuid4()),
    -         name=deck.name,
    -         description=deck.description,
    -         cards=[Card(id=str(uuid.uuid4()), **card.model_dump())
    -                for card in deck.cards]
    -     )
    -     decks.append(new_deck)
    -     write_decks(decks)
    -     return new_deck
    +     card_ids = []
    +     for i, card in enumerate(deck.cards):
    +         created_card = await client.query_single("""
    +             insert Card {
    +                 front := <str>$front,
    +                 back := <str>$back,
    +                 order := <int64>$order
    +             }
    +         """, front=card.front, back=card.back, order=i)
    +         card_ids.append(created_card.id)
    +
    +     new_deck = await client.query_single("""
    +         select(
    +             insert Deck {
    +                 name := <str>$name,
    +                 description := <optional str>$description,
    +                 cards := (
    +                     select Card
    +                     filter .id IN array_unpack(<array<uuid>>$card_ids)
    +                 )
    +             }
    +         ) { ** }
    +     """, name=deck.name, description=deck.description,
    +          card_ids=card_ids)
    +
    +     return new_deck

.. edb:split-section::

  The above works but isn't atomic - if creating a card fails, you could end up with partial data. Let's wrap it in a transaction:

  .. code-block:: python-diff
    :caption: main.py

      @app.post("/decks/import", response_model=Deck)
      async def import_deck(deck: DeckCreate):
    -     card_ids = []
    -     for i, card in enumerate(deck.cards):
    -         created_card = await client.query_single("""
    -             insert Card {
    -                 front := <str>$front,
    -                 back := <str>$back,
    -                 order := <int64>$order
    -             }
    -         """, front=card.front, back=card.back, order=i)
    -         card_ids.append(created_card.id)
    -
    -     new_deck = await client.query_single("""
    -         select(
    -             insert Deck {
    -                 name := <str>$name,
    -                 description := <optional str>$description,
    -                 cards := (
    -                     select Card
    -                     filter .id IN array_unpack(<array<uuid>>$card_ids)
    -                 )
    -             }
    -         ) { ** }
    -     """, name=deck.name, description=deck.description,
    -          card_ids=card_ids)
    +     async for tx in client.transaction():
    +         async with tx:
    +         card_ids = []
    +         for i, card in enumerate(deck.cards):
    +              created_card = await tx.query_single(
    +                  """
    +                  insert Card {
    +                      front := <str>$front,
    +                      back := <str>$back,
    +                      order := <int64>$order
    +                  }
    +                  """,
    +                  front=card.front,
    +                  back=card.back,
    +                  order=i,
    +              )
    +              card_ids.append(created_card.id)
    +
    +         new_deck = await client.query_single("""
    +             select(
    +                 insert Deck {
    +                     name := <str>$name,
    +                     description := <optional str>$description,
    +                     cards := (
    +                         select Card
    +                         filter .id IN array_unpack(<array<uuid>>$card_ids)
    +                     )
    +                 }
    +             ) { ** }
    +             """,
    +             name=deck.name,
    +             description=deck.description,
    +             card_ids=card_ids,
    +         )

          return new_deck

.. edb:split-section::

  We can make this even more efficient by doing everything in a single query:

  .. code-block:: python-diff
    :caption: main.py

      @app.post("/decks/import", response_model=Deck)
      async def import_deck(deck: DeckCreate):
    -     async for tx in client.transaction():
    -         async with tx:
    -         card_ids = []
    -         for i, card in enumerate(deck.cards):
    -              created_card = await tx.query_single(
    -                  """
    -                  insert Card {
    -                      front := <str>$front,
    -                      back := <str>$back,
    -                      order := <int64>$order
    -                  }
    -                  """,
    -                  front=card.front,
    -                  back=card.back,
    -                  order=i,
    -              )
    -              card_ids.append(created_card.id)
    -
    -         new_deck = await client.query_single("""
    -             select(
    -                 insert Deck {
    -                     name := <str>$name,
    -                     description := <optional str>$description,
    -                     cards := (
    -                         select Card
    -                         filter .id IN array_unpack(<array<uuid>>$card_ids)
    -                     )
    -                 }
    -             ) { ** }
    -             """,
    -             name=deck.name,
    -             description=deck.description,
    -             card_ids=card_ids,
    -         )
    +     cards_data = [(c.front, c.back, i) for i, c in enumerate(deck.cards)]
    +
    +     new_deck = await client.query_single("""
    +         select(
    +             with cards := <array<tuple<str, str, int64>>>$cards_data
    +             insert Deck {
    +                 name := <str>$name,
    +                 description := <optional str>$description,
    +                 cards := (
    +                     for card in array_unpack(cards)
    +                     union (
    +                         insert Card {
    +                             front := card.0,
    +                             back := card.1,
    +                             order := card.2
    +                         }
    +                     )
    +                 )
    +             }
    +         ) { ** }
    +     """, name=deck.name, description=deck.description,
    +          cards_data=cards_data)

          return new_deck

Updating data
=============

.. edb:split-section::

  Next, update the deck operations. The update operation needs to handle partial updates of name and description:

  .. code-block:: python-diff
    :caption: main.py

      @app.put("/decks/{deck_id}", response_model=Deck)
      async def update_deck(deck_id: UUID, deck_update: DeckBase):
    -     decks = read_decks()
    -     deck = next((deck for deck in decks if deck.id == deck_id), None)
    -     if not deck:
    -         raise HTTPException(status_code=404, detail="Deck not found")
    -
    -     deck.name = deck_update.name
    -     deck.description = deck_update.description
    -     write_decks(decks)
    -     return deck
    +     # Build update sets based on provided fields
    +     sets = []
    +     params = {"id": deck_id}
    +
    +     if deck_update.name is not None:
    +         sets.append("name := <str>$name")
    +         params["name"] = deck_update.name
    +
    +     if deck_update.description is not None:
    +         sets.append("description := <optional str>$description")
    +         params["description"] = deck_update.description
    +
    +     if not sets:
    +         return await get_deck(deck_id)
    +
    +     updated_deck = await client.query(f"""
    +         UPDATE Deck
    +         FILTER .id = <uuid>$id
    +         SET {{ {', '.join(sets)} }}
    +     """, **params)
    +
    +     if not updated_deck:
    +         raise HTTPException(status_code=404, detail="Deck not found")
    +
    +     query = """
    +         select(
    +             update Deck
    +             filter .id = <uuid>$id
    +             set { %s }
    +         ) { ** }
    +     """ % ", ".join(sets)
    +
    +     return updated_deck


Adding linked data
==================

.. edb:split-section::

  Now, update the card operations to use |Gel| to add cards to a deck:

  .. code-block:: python-diff
      :caption: main.py

        @app.post("/decks/{deck_id}/cards", response_model=Card)
        async def add_card(deck_id: UUID, card: CardBase):
      -     decks = read_decks()
      -     deck = next((deck for deck in decks if deck.id == deck_id), None)
      -     if not deck:
      -         raise HTTPException(status_code=404, detail="Deck not found")
      -
      -     new_card = Card(id=str(uuid.uuid4()), **card.model_dump())
      -     deck.cards.append(new_card)
      -     write_decks(decks)
      -     return new_card
      +     # Get max order and increment
      +     deck = await client.query_single("""
      +         select max(.cards.order)
      +         from Deck
      +         filter .id = <uuid>$id
      +     """, id=deck_id)
      +
      +     new_order = (deck.max_order or -1) + 1
      +
      +     new_card = await client.query_single("""
      +         insert Card {
      +             front := <str>$front,
      +             back := <str>$back,
      +             order := <int64>$order,
      +         }
      +     """, front=card.front, back=card.back,
      +          order=new_order, deck_id=deck_id)
      +
      +     new_deck = await client.query_single(
      +         """
      +         select(
      +             update Deck
      +             filter .id = <uuid>$id
      +             set {
      +                 cards += (select Card { id, front, back } filter .id = <uuid>$card_id)
      +             }
      +         ) { ** }
      +         """,
      +         id=deck_id,
      +         card_id=new_card.id,
      +     )
      +
      +     if not new_card:
      +         raise HTTPException(status_code=404, detail="Deck not found")
      +
      +     return new_card


Deleting linked data
====================

.. edb:split-section::

  As the next step, update the card deletion operation to use |Gel| to remove a card from a deck:

  .. code-block:: python-diff
    :caption: main.py

      @app.delete("/cards/{card_id}")
      async def delete_card(card_id: str):
    -     decks = read_decks()
    -     deck = next((deck for deck in decks if deck.id == deck_id), None)
    -     if not deck:
    -         raise HTTPException(status_code=404, detail="Deck not found")
    -
    -     deck.cards = [card for card in deck.cards if card.id != card_id]
    -     write_decks(decks)
    -     return {"message": "Card deleted"}
    +     deleted = await client.query("""
    +         delete Card
    +         filter
    +             .id = <uuid>$card_id
    +     """, card_id=card_id)
    +
    +     if not deleted:
    +         raise HTTPException(status_code=404, detail="Card not found")
    +
    +     return "Card deleted"

Querying data
=============

.. edb:split-section::

  Finally, update the query endpoints to fetch data from |Gel|:

  .. code-block:: python-diff
    :caption: main.py

      @app.get("/decks", response_model=List[Deck])
      async def get_decks():
    -     return read_decks()
    +     decks = await client.query("""
    +         select Deck {
    +             id,
    +             name,
    +             description,
    +             cards := (
    +                 select .cards {
    +                     id,
    +                     front,
    +                     back
    +                 }
    +                 order BY .order
    +             )
    +         }
    +     """)
    +     return decks

      @app.get("/decks/{deck_id}", response_model=Deck)
      async def get_deck(deck_id: UUID):
    -     decks = read_decks()
    -     deck = next((deck for deck in decks if deck.id == deck_id), None)
    -     if not deck:
    -         raise HTTPException(status_code=404, detail=f"Deck with id {deck_id} not found")
    -     return deck
    +     deck = await client.query_single("""
    +         select Deck {
    +             id,
    +             name,
    +             description,
    +             cards := (
    +                 select .cards {
    +                     id,
    +                     front,
    +                     back
    +                 }
    +                 order BY .order
    +             )
    +         }
    +         FILTER .id = <uuid>$id
    +     """, id=deck_id)
    +
    +     if not deck:
    +         raise HTTPException(
    +             status_code=404,
    +             detail=f"Deck with id {deck_id} not found"
    +         )
    +
    +     return deck

.. edb:split-section::

  You can now run your FastAPI application with:

  .. code-block:: sh

    $ uvicorn main:app --reload

.. edb:split-section::

  The API documentation will be available at http://localhost:8000/docs. You can use this interface to test your endpoints and import the sample flashcard deck.

  .. image:: https://github.com/user-attachments/assets/707ba9e3-4c58-40a4-b5e9-7bb95d9d9d6e
