.. _ref_quickstart_ai:

======================
Using the built-in RAG
======================

.. edb:split-section::

    In this section we'll use |Gel|'s built-in vector search and
    retrieval-augmented generation capabilities to decorate our flashcard app
    with a couple AI features. We're going to create a ``/fetch_similar``
    endpoint that's going to look up flashcards similar to a text search query,
    as well as a ``/fetch_rag`` endpoint that's going to enable us to talk to
    an LLM about the content of our flashcard deck.

    We're going to start with the same schema we left off with in the primary
    quickstart.


    .. code-block:: sdl
        :caption: dbschema/default.gel

        module default {
            abstract type Timestamped {
                required created_at: datetime {
                    default := datetime_of_statement();
                };
                required updated_at: datetime {
                    default := datetime_of_statement();
                };
            }

            type Deck extending Timestamped {
                required name: str;
                description: str;
                cards := (
                    select .<deck[is Card]
                    order by .order
                );
            };

            type Card extending Timestamped {
                required order: int64;
                required front: str;
                required back: str;
                required deck: Deck;
            }
        }


.. edb:split-section::

    AI-related features in |Gel| come packaged in the extension called ``ai``.
    Let's enable it by adding the following line on top of the
    ``dbschema/default.gel`` and running a migration.

    This does a few things. First, it enables us to use features from the extension by prefixing them with ``ext::ai::``.


    .. code-block:: sdl-diff
        :caption: dbschema/default.gel

        + using extension ai;

          module default {
              abstract type Timestamped {
                  required created_at: datetime {
                      default := datetime_of_statement();
                  };
                  required updated_at: datetime {
                      default := datetime_of_statement();
                  };
              }

              type Deck extending Timestamped {
                  required name: str;
                  description: str;
                  cards := (
                      select .<deck[is Card]
                      order by .order
                  );
              };

              type Card extending Timestamped {
                  required order: int64;
                  required front: str;
                  required back: str;
                  required deck: Deck;
              }
          }

.. edb:split-section::

    This enabled us to use features in the ``ext::ai::`` namespace. Here's a
    notable one: ``ProviderConfig``, which we can use to configure our API
    keys. |Gel| supports a variety of external APIs for creating embedding
    vectors for text and fetching LLM completions.

    Let's configure an API key for OpenAI by running the following query in the
    REPL:

    .. note::

        Once the extension is active, we can also access the dedicated AI tab
        in the UI. There we can manage provider configurations and try out
        different RAG configuraton in the Playground.


    .. code-block:: edgeql-repl

        db> configure current database
            insert ext::ai::OpenAIProviderConfig {
                secret := 'sk-....',
            };


.. edb:split-section::

    Once last thing before we move on. Let's add some sample data to give the
    embedding model something to work with. You can copy and run this command
    in the terminal, or come up with your own sample data.


    .. code-block:: edgeql
        :class: collapsible

        $ cat << 'EOF' | gel query --file -
        with deck := (
            insert Deck {
                name := 'Smelly Cheeses',
                description := 'To impress everyone with stinky cheese trivia.'
            }
        )
        for card_data in {(
            1,
            'Époisses de Bourgogne',
            'Known as the "king of cheeses", this French cheese is so pungent it\'s banned on public transport in France. Washed in brandy, it becomes increasingly funky as it ages. Orange-red rind, creamy interior.'
        ), (
            2,
            'Vieux-Boulogne',
            'Officially the smelliest cheese in the world according to scientific studies. This northern French cheese has a reddish-orange rind from being washed in beer. Smooth, creamy texture with a powerful aroma.'
        ), (
            3,
            'Durian Cheese',
            'This Malaysian creation combines durian fruit with cheese, creating what some consider the ultimate "challenging" dairy product. Combines the pungency of blue cheese with durian\'s notorious aroma.'
        ), (
            4,
            'Limburger',
            'German cheese famous for its intense smell, often compared to foot odor due to the same bacteria. Despite its reputation, has a surprisingly mild taste with notes of mushroom and grass.'
        ), (
            5,
            'Roquefort',
            'The "king of blue cheeses", aged in limestone caves in southern France. Contains Penicillium roqueforti mold. Strong, tangy, and salty with a crumbly texture. Legend says it was discovered when a shepherd left his lunch in a cave.'
        ), (
            6,
            'What makes washed-rind cheeses so smelly?',
            'The process of washing cheese rinds in brine, alcohol, or other solutions promotes the growth of Brevibacterium linens, the same bacteria responsible for human body odor. This bacteria contributes to both the orange color and distinctive aroma.'
        ), (
            7,
            'Stinking Bishop',
            'Named after the Stinking Bishop pear (not a religious figure). This English cheese is washed in perry made from these pears. Known for its powerful aroma and sticky, pink-orange rind. Gained fame after being featured in Wallace & Gromit.'
        )}
        union (
            insert Card {
                deck := deck,
                order := card_data.0,
                front := card_data.1,
                back := card_data.2
            }
        );
        EOF


.. edb:split-section::

    Now we can finally start producing embedding vectors. Since |Gel| is fully
    aware of when your data gets inserted, updated and deleted, it's perfectly
    equipped to handle all the tedious work of keeping those vectors up to
    date. All that's left for us is to create a special ``deferred index`` on
    the data we would like to perform similarity search on.


    .. code-block:: sdl-diff
        :caption: dbschema/default.gel

          using extension ai;

          module default {
              abstract type Timestamped {
                  required created_at: datetime {
                      default := datetime_of_statement();
                  };
                  required updated_at: datetime {
                      default := datetime_of_statement();
                  };
              }

              type Deck extending Timestamped {
                  required name: str;
                  description: str;
                  cards := (
                      select .<deck[is Card]
                      order by .order
                  );
              };

              type Card extending Timestamped {
                  required order: int64;
                  required front: str;
                  required back: str;
                  required deck: Deck;

        +         deferred index ext::ai::index(embedding_model := 'text-embedding-3-small')
        +             on (.front ++ ' ' ++ .back);
              }
          }


.. edb:split-section::

    It's time to start running queries.

    Let's begin by creating the ``/fetch_similar`` endpoint we mentioned
    earlier. It's job is going to be to find 3 flashcards that are the most
    similar to the provided text query. We can use this endpoint to implement a
    "recommended flashcards" on the frontend.

    The AI extension contains a function called ``ext::ai::search(Type,
    embedding_vector)`` that we can use to do our fetch. Note that the second
    argument is an embedding vector, not a text query. To transform our text
    query into a vector, we will use the ``generate_embeddings`` function from
    the ``ai`` module of |Gel|'s Python binding.

    Gathered together, here are the modifications we need to do to the
    ``main.py`` function:


    .. code-block:: python-diff
        :caption: main.py

          import gel
        + import gel.ai

          from fastapi import FastAPI


          client = gel.create_async_client()

          app = FastAPI()


        + @app.get("/fetch_similar")
        + async def fetch_similar_cards(query: str):
        +     rag = await gel.ai.create_async_rag_client(client, model="gpt-4-turbo-preview")
        +     embedding_vector = await rag.generate_embeddings(
        +         query, model="text-embedding-3-small"
        +     )

        +     similar_cards = await client.query(
        +         "select ext::ai::search(Card, <array<float32>>$embedding_vector)",
        +         embedding_vector=embedding_vector,
        +     )

        +     return similar_cards


.. edb:split-section::

    Let's test the endpoint to see that everything works the way we expect.


    .. code-block:: bash

        $ curl -X 'GET' \
          'http://localhost:8000/fetch_similar?query=the%20stinkiest%20cheese' \
          -H 'accept: application/json'


.. edb:split-section::

    Finally, let's create the second endpoint we mentioned, called
    ``/fetch_rag``. We'll be able to use this one to, for example, ask an LLM
    to quiz us on the contents of our deck.

    The RAG feature is represented in the Python binding with the ``query_rag``
    method of the ``GelRAG`` class. To use it, we're going to instantiate the
    class and call the method... And that's it!


    .. code-block:: python-diff
        :caption: main.py

          import gel
          import gel.ai

          from fastapi import FastAPI


          client = gel.create_async_client()

          app = FastAPI()


          @app.get("/fetch_similar")
          async def fetch_similar_cards(query: str):
              rag = await gel.ai.create_async_rag_client(client, model="gpt-4-turbo-preview")
              embedding_vector = await rag.generate_embeddings(
                  query, model="text-embedding-3-small"
              )

              similar_cards = await client.query(
                  "select ext::ai::search(Card, <array<float32>>$embedding_vector)",
                  embedding_vector=embedding_vector,
              )

              return similar_cards


        + @app.get("/fetch_rag")
        + async def fetch_rag_response(query: str):
        +     rag = await gel.ai.create_async_rag_client(client, model="gpt-4-turbo-preview")
        +     response = await rag.query_rag(
        +         message=query,
        +         context=gel.ai.QueryContext(query="select Card"),
        +     )
        +     return response


.. edb:split-section::

    Let's test the endpoint to see if it works:


    .. code-block:: bash

        $ curl -X 'GET' \
          'http://localhost:8000/fetch_rag?query=what%20cheese%20smells%20like%20feet' \
          -H 'accept: application/json'


.. edb:split-section::

    Congratulations! We've now implemented AI features in our flashcards app.
    Of course, there's more to learn when it comes to using the AI extension.
    Make sure to check out the Reference manual, or build an LLM-powered search
    bot from the ground up with the FastAPI Gel AI tutorial.
