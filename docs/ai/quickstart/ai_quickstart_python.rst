.. _ref_ai_quickstart_python:

================
Gel AI in Python
================

:edb-alt-title: Gel AI Quickstart in Python

Gel AI brings vector search capabilities and retrieval-augmented generation
directly into the database. It's integrated into the Gel Python binding via the
``gel.ai`` module.

.. code-block:: bash

  $ pip install 'gel[ai]'


Enable and configure the extension
==================================

AI is an Gel extension. To enable it, we will need to add the extension
to the app’s schema:

.. code-block:: sdl

    using extension ai;


Gel AI uses external APIs in order to get vectors and LLM completions. For it
to work, we need to configure an API provider and specify their API key. Let's
open EdgeQL REPL and run the following query:

.. code-block:: edgeql

    configure current database
    insert ext::ai::OpenAIProviderConfig {
      secret := 'sk-....',
    };


Now our Gel application can take advantage of OpenAI's API to implement AI
capabilities.


.. note::

   Gel AI comes with its own Admin panel that can be used to configure
   providers, set up prompts and test them in a sandbox. Learn more.


.. note::

   Most API providers charge money, make sure you have that.


Add vectors
===========

Before we start introducing AI capabilities, let's set up our database with a
schema and populate it with some data (we're going to be helping Komi-san keep
track of her friends).

.. code-block:: sdl

    module default {
        type Friend {
            required name: str {
                constraint exclusive;
            };

            summary: str;               # A brief description of personality and role
            relationship_to_komi: str;  # Relationship with Komi
            defining_trait: str;        # Primary character trait or quirk
        }
    }

.. code-block:: bash
    :class: collapsible

    $ cat << 'EOF' > populate_db.edgeql
    insert Friend {
        name := 'Tadano Hitohito',
        summary := 'An extremely average high school boy with a remarkable ability to read the atmosphere and understand others\' feelings, especially Komi\'s.',
        relationship_to_komi := 'First friend and love interest',
        defining_trait := 'Perceptiveness',
    };

    insert Friend {
        name := 'Osana Najimi',
        summary := 'An extremely outgoing person who claims to have been everyone\'s childhood friend. Gender: Najimi.',
        relationship_to_komi := 'Second friend and social catalyst',
        defining_trait := 'Universal childhood friend',
    };

    insert Friend {
        name := 'Yamai Ren',
        summary := 'An intense and sometimes obsessive classmate who is completely infatuated with Komi.',
        relationship_to_komi := 'Self-proclaimed guardian and admirer',
        defining_trait := 'Obsessive devotion',
    };

    insert Friend {
        name := 'Katai Makoto',
        summary := 'A intimidating-looking but shy student who shares many communication problems with Komi.',
        relationship_to_komi := 'Fellow communication-challenged friend',
        defining_trait := 'Scary appearance but gentle nature',
    };

    insert Friend {
        name := 'Nakanaka Omoharu',
        summary := 'A self-proclaimed wielder of dark powers who acts like an anime character and is actually just a regular gaming enthusiast.',
        relationship_to_komi := 'Gaming buddy and chuunibyou friend',
        defining_trait := 'Chuunibyou tendencies',
    };
    EOF
    $ gel query -f populate_db.edgeql


In order to get Gel to produce embedding vectors, we need to create a special
``deferred index`` on the type we would like to perform similarity search on.
More specifically, we need to specify an EdgeQL expression that produces a
string that we're going to create an embedding vector for. This is how we would
set up an index if we wanted to perform similarity search on
``Friend.summary``:

.. code-block:: sdl-diff

      module default {
          type Friend {
              required name: str {
                  constraint exclusive;
              };

              summary: str;               # A brief description of personality and role
              relationship_to_komi: str;  # Relationship with Komi
              defining_trait: str;        # Primary character trait or quirk

    +         deferred index ext::ai::index(embedding_model := 'text-embedding-3-small')
    +             on (.summary);
          }
      }


But actually, in our case it would be better if we could similarity search
across all properties at the same time. We can define the index on a more
complex expression - like a concatenation of string properties - like this:


.. code-block:: sdl-diff

      module default {
          type Friend {
              required name: str {
                  constraint exclusive;
              };

              summary: str;               # A brief description of personality and role
              relationship_to_komi: str;  # Relationship with Komi
              defining_trait: str;        # Primary character trait or quirk

              deferred index ext::ai::index(embedding_model := 'text-embedding-3-small')
    -             on (.summary);
    +             on (
    +                 .name ++ ' ' ++ .summary ++ ' '
    +                 ++ .relationship_to_komi ++ ' '
    +                 ++ .defining_trait
    +             );
          }
      }


Once we're done with schema modification, we need to apply them by going
through a migration:

.. code-block:: bash

    $ gel migration create
    $ gel migrate


That's it! Gel will make necessary API requests in the background and create an
index that will enable us to perform efficient similarity search.


Perform similarity search in Python
===================================

In order to run queries against the index we just created, we need to create a
Gel client and pass it to a Gel AI instance.

.. code-block:: python

    import gel
    import gel.ai

    gel_client = gel.create_client()
    gel_ai = edgedb.ai.create_ai(client)

    text = "Who helps Komi make friends?"
    vector = gel_ai.generate_embeddings(
        text,
        "text-embedding-3-small",
    )

    gel_client.query(
        "select ext::ai::search(Friend, <array<float32>>$embedding_vector",
        embedding_vector=vector,
    )


We are going to execute a query that calls a single function:
``ext::ai::search(<type>, <search_vector>)``. That function accepts an
embedding vector as the second argument, not a text string. This means that in
order to similarity search for a string, we need to create a vector embedding
for it using the same model as we used to create the index. The Gel AI binding
in Python comes with a ``generate_embeddings`` function that does exactly that:


.. code-block:: python-diff

      import gel
      import gel.ai

      gel_client = gel.create_client()
      gel_ai = edgedb.ai.create_ai(client)

    + text = "Who helps Komi make friends?"
    + vector = gel_ai.generate_embeddings(
    +     text,
    +     "text-embedding-3-small",
    + )


Now we can plug that vector directly into our query to get similarity search
results:


.. code-block:: python-diff

      import gel
      import gel.ai

      gel_client = gel.create_client()
      gel_ai = edgedb.ai.create_ai(client)

      text = "Who helps Komi make friends?"
      vector = gel_ai.generate_embeddings(
          text,
          "text-embedding-3-small",
      )

    + gel_client.query(
    +     "select ext::ai::search(Friend, <array<float32>>$embedding_vector",
    +     embedding_vector=vector,
    + )


Use the built-in RAG
====================

One more feature Gel AI offers is built-in retrieval-augmented generation, also
known as RAG.

Gel comes preconfigured to be able to process our text query, perform
similarity search across the index we just created, pass the results to an LLM
and return a response. In order to access the built-in RAG, we need to start by
selecting an LLM and passing its name to the Gel AI instance constructor:


.. code-block:: python-diff

      import gel
      import gel.ai

      gel_client = gel.create_client()
      gel_ai = edgedb.ai.create_ai(
          client,
    +     model="gpt-4-turbo-preview"
      )


Now we can access the RAG using the ``query_rag`` function like this:


.. code-block:: python-diff

      import gel
      import gel.ai

      gel_client = gel.create_client()
      gel_ai = edgedb.ai.create_ai(
          client,
          model="gpt-4-turbo-preview"
      )

    + gel_ai.query_rag(
    +     "Who helps Komi make friends?",
    +     context="Friend",
    + )

We can also stream the response like this:


.. code-block:: python-diff

      import gel
      import gel.ai

      gel_client = gel.create_client()
      gel_ai = edgedb.ai.create_ai(
          client,
          model="gpt-4-turbo-preview"
      )

    - gel_ai.query_rag(
    + gel_ai.stream_rag(
          "Who helps Komi make friends?",
          context="Friend",
      )

Keep going!
===========

You are now sufficiently equipped to use Gel AI in your applications.

If you'd like to build something on your own, make sure to check out the
Reference manual in order to learn the details about using different APIs and
models, configuring prompts or using the UI.

And if you would like more guidance for how Gel AI can be fit into an
application, take a look at the FastAPI Gel AI Tutorial, where we're building a
search bot using features you learned about above.


