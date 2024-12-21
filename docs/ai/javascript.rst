.. _ref_ai_javascript:

==========
JavaScript
==========

:edb-alt-title: EdgeDB AI JavaScript library

``@edgedb/ai`` offers a convenient wrapper around ``ext::ai``. Install it with
npm or via your package manager of choice:

.. tabs::

  .. code-tab:: npm
    
    npm i @edgedb/ai 

  .. code-tab:: pnpm

    pnpm add @edgedb/ai 

  .. code-tab:: yarn

    yarn add @edgedb/ai

  .. code-tab:: bun 

    bun add @edgedb/ai


Usage
=====

Start by importing ``createClient`` from ``edgedb`` and ``createAI`` from
``@edgedb/ai``:

.. code-block:: typescript

    import { createClient } from "edgedb";
    import { createAI } from "@edgedb/ai";

Create an EdgeDB client. Create an instance of the AI client by passing in the
EdgeDB client and any options for the AI provider:

.. code-block:: typescript

    const client = createClient();

    const gpt4Ai = createAI(client, {
      model: "gpt-4-turbo-preview",
    });

You may use any of the supported :ref:`chat language models
<ref_ai_supported_llm_models_chat_language_models>`.
Add the context query:

.. code-block:: typescript

    const astronomyAi = gpt4Ai.withContext({
      query: "Astronomy"
    });

.. todo: add reference to context

Call your AI client's ``queryRag`` method, passing in a text query.

.. code-block:: typescript

    console.log(
      await astronomyAi.queryRag("What color is the sky on Mars?")
    );

You can chain multiple calls of ``withContext`` or ``withConfig`` to create
additional AI clients, identical except for the newly specified values.

.. code-block:: typescript

    const fastAstronomyAi = astronomyAi.withConfig({
      model: "gpt-3.5-turbo",
    });
    console.log(
      await fastAstronomyAi.queryRag("What color is the sky on Mars?")
    );

    const fastChemistryAi = fastAstronomyAi.withContext({
      query: "Chemistry"
    });
    console.log(
      await fastChemistryAi.queryRag("What is the atomic number of gold?")
    );


API Reference
=============

.. js:function:: createAI( \
                   client: Client, \
                   options: AIOptions \
          ): EdgeDBAI

    Creates an instance of ``EdgeDBAI`` with the specified client and options.

    :param Client client:
        Required. An EdgeDB client instance.

    :param string options.model:
        Required. Specifies the AI model to use. This could be any chat model supported by EdgeDB AI.

    :param Prompt options.prompt:
        Optional. Defines the input prompt for the AI model. If not provided, the built-in system prompt (``builtin::rag-default``) will be used. The input prompt specifies a system message or an array of messages that always precede user messages.

        ::

          type Prompt =
            | { name: string; custom?: EdgeDBMessage[] }
            | { id: string; custom?: EdgeDBMessage[] }
            | { custom: EdgeDBMessage[] }

        If you want to use a different input prompt (configured through the EdgeDB UI or EdgeQL), you should provide it's ID or name (but only one of these). Alongside the ID or name, you can optionally include a ``custom`` array of messages that will also precede user messages sent to the AI model.

        .. raw:: html

          <div style="line-height: 20px">
            <br>
          </div>

        Alternatively, you can provide only the ``custom`` messages, in which case no configured input prompt will be used.

        .. raw:: html

          <div style="line-height: 20px">
            <br>
          </div>

        While you can use ``custom`` to provide the chat history, the more idiomatic approach is to include the history with the ``messages`` array in ``streamRag`` or ``queryRag``.

EdgeDBAI
--------

Instances of ``EdgeDBAI`` offer methods for client configuration and utilizing
RAG.

Public methods
^^^^^^^^^^^^^^

.. js:method:: withConfig(options?: Partial<AIOptions>): EdgeDBAI

    Returns a new ``EdgeDBAI`` instance with updated configuration options.

    :param string options.model:
        Optional. Specifies the AI model to use. 
    :param Prompt options.prompt:
        Optional. Refer to the ``createAI`` function above for the prompt's structure and details.

.. js:method:: withContext(context: Partial<QueryContext>): EdgeDBAI

    Returns a new ``EdgeDBAI`` instance with an updated query context.

    :param string context.query:
        Required. Specifies an expression to identify the objects from which relevant context is extracted for the user's question. This can be any valid expression that produces a set of objects, even if it is not a standalone query:

        - A simple expression like ``"Astronomy"`` (equivalent to ``"select   Astronomy"``) will include all Astronomy objects. 
        - You can use filtering to narrow down the set of objects.

    :param object context.variables optional:
        Optional. An object of variables for use in the context query.
    :param object context.globals optional:
        Optional. An object of globals for use in the context query.
    :param number context.max_object_count optional:
        Optional. A maximum number of objects to return. Default is 5.

.. js:method:: async queryRag( \
                   request: RagRequestPrompt | RagRequestMessages, \
                   context: QueryContext = this.context \
                 ): Promise<string>

    Sends a query with context to the configured AI model and returns the
    response as a string.

    :param RagRequest request:
        Required. You can provide either ``prompt`` or ``messages`` and any other property that is supported by EdgeDB AI (like ``tools``, ``max_tokens`` etc). Prompt is a string (user question/query), and messages is an array of ``EdgeDBMessage`` (for example when you want to include the chat history).
    :param QueryContext context:
        Optional. By default will use the context previously provided in ``withContext``.
        Howewer you can also provide a ``context`` object here. Refer to the ``withContext`` method definition to see the shape of the ``context`` object.

.. js:method:: async streamRag( \
                   request: RagRequestPrompt | RagRequestMessages, \
                   context: QueryContext = this.context \
                 ): AsyncIterable<StreamingMessage> & PromiseLike<Response>

    Can be used in two ways:

    - as **an async iterator** - if you want to process streaming data in     real-time as it arrives, ideal for handling long-running streams.

    - as **a Promise that resolves to a full Response object** - you have     complete control over how you want to handle the stream, this might be useful when you want to manipulate the raw stream or parse it in a custom way.


    :param RagRequestPrompt request:
        Required. You can provide either ``prompt`` or ``messages`` and any other property that is supported by EdgeDB AI (like ``tools``, ``max_tokens`` etc). Prompt is a string (user question/query), and messages is an array of ``EdgeDBMessage`` (for example when you want to include the chat history).
    :param QueryContext context:
        Optional. By default will use the context previously provided in ``withContext``.
        Howewer you can also provide a ``context`` object here. Refer to the ``withContext`` method definition to see the shape of the ``context`` object.
        
.. js:method:: async generateEmbeddings( \
                   inputs: string[], \
                   model: string \
                 ): Promise<number[]>

    Generates embeddings for the array of strings.

    :param string[] inputs:
        Required. Strings array to generate embeddings for.
    :param string model:
        Required. Specifies the AI embedding model to use.
