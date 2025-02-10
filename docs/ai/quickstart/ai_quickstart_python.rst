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


Add vectors and perform similarity search
=========================================

To start using EdgeDB AI on a type, create an index:

.. code-block:: sdl-diff

      module default {
        type Astronomy {
          content: str;
    +     deferred index ext::ai::index(embedding_model := 'text-embedding-3-small')
    +       on (.content);
        }
      };

In this example, we have added an AI index on the ``Astronomy`` type's
``content`` property using the ``text-embedding-3-small`` model. Once you have
the index in your schema, :ref:`create <ref_cli_edgedb_migration_create>` and
:ref:`apply <ref_cli_edgedb_migration_apply>` your migration, and you're ready
to start running queries!

You may want to include multiple properties in your AI index. Fortunately, you
can define an AI index on an expression:

.. code-block:: sdl

      module default {
        type Astronomy {
          climate: str;
          atmosphere: str;
          deferred index ext::ai::index(embedding_model := 'text-embedding-3-small')
            on (.climate ++ ' ' ++ .atmosphere);
        }
      };

Once your index has been migrated, running a query against the embeddings is
super simple:

.. code-block:: python

    client = edgedb.create_async_client()


    select ext::ai::search(Astronomy, query_vector)

Simple, but you'll still need to generate embeddings from your query or pass in
existing embeddings. This is how we can procure an embedding via HTTP:

.. code-block:: python

   $ curl


Use the built-in RAG
====================

By making an HTTP request to
``https://<edgedb-host>:<port>/branch/<branch-name>/ai/rag``, you can generate
text via the generative AI API of your choice within the context of a type with
a deferred embedding index.

.. note::

    Making HTTP requests to EdgeDB requires :ref:`authentication
    <ref_http_auth>`.

.. code-block:: python

    gpt4ai = await edgedb.ai.create_async_ai(
        client,
        model="gpt-4-turbo-preview"
    )

    astronomy_ai = gpt4ai.with_context(
        query="Astronomy"
    )


The default text generation prompt will ask your selected provider to limit
answer to information provided in the context and will pass the queried
objects' AI index as context along with that prompt.

.. code-block:: python

    import asyncio  # alongside the EdgeDB imports

    client = edgedb.create_async_client()

    async def main():
        gpt4ai = await edgedb.ai.create_async_ai(
            client,
            model="gpt-4-turbo-preview"
        )
        astronomy_ai = gpt4ai.with_context(
            query="Astronomy"
        )
        query = "What color is the sky on Mars?"
        print(
            await astronomy_ai.query_rag(query)
        );

        #or streamed
        async for data in blog_ai.stream_rag(query):
            print(data)

    asyncio.run(main())
