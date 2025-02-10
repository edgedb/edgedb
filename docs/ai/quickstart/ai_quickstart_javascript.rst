.. _ref_ai_quickstart_javascript:

====================
Gel AI in JavaScript
====================

:edb-alt-title: Gel AI Quickstart in JavaScript

Gel AI brings vector search capabilities and retrieval-augmented generation
directly into the database.


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

.. code-block:: edgeql

    select ext::ai::search(Astronomy, query_vector)

Simple, but you'll still need to generate embeddings from your query or pass in
existing embeddings. This is how we can procure an embedding via HTTP:

.. code-block:: bash

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

.. code-block:: bash

    $ curl --json '{
        "query": "What color is the sky on Mars?",
        "model": "gpt-4-turbo-preview",
        "context": {"query":"select Astronomy"}
      }' https://<edgedb-host>:<port>/branch/<branch-name>/ai/rag
    {"response": "The sky on Mars is red."}

Since LLMs are often slow, it may be useful to stream the response. To do this,
add ``"stream": true`` to your request JSON.




Use RAG via JavaScript
----------------------

``@edgedb/ai`` offers a convenient wrapper around ``ext::ai``. Install it with
``npm install @edgedb/ai`` (or via your package manager of choice) and
implement it like this example:

.. code-block:: typescript

    import { createClient } from "edgedb";
    import { createAI } from "@edgedb/ai";

    const client = createClient();

    const gpt4AI = createAI(client, {
      model: "gpt-4-turbo-preview",
    });

    const blogAI = gpt4AI.withContext({
      query: "select Astronomy"
    });

    console.log(await blogAI.queryRag(
      "What color is the sky on Mars?"
    ));
