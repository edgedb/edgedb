.. _ref_ai_javascript:

==========
JavaScript
==========

:edb-alt-title: EdgeDB AI's JavaScript package

``@edgedb/ai`` offers a convenient wrapper around ``ext::ai``. Install it with
npm or via your package manager of choice:

.. code-block:: bash

    $ npm install @edgedb/ai # or
    $ yarn add @edgedb/ai # or
    $ pnpm add @edgedb/ai # or
    $ bun add @edgedb/ai


Usage
=====

Start by importing ``createClient`` from ``edgedb`` and ``createAI`` from
``@edgedb/ai``:

.. code-block:: typescript

    import { createClient } from "edgedb";
    import { createAI } from "@edgedb/ai";

Create an EdgeDB client. Create an instance of the AI client by passing in the
EdgeDB client and any options for the AI provider (like the text generation
model):

.. code-block:: typescript

    const client = createClient();

    const gpt4Ai = createAI(client, {
      model: "gpt-4-turbo-preview",
    });

Add your query as context:

.. code-block:: typescript

    const astronomyAi = gpt4Ai.withContext({
      query: "Astronomy"
    });

The default text generation prompt will ask your selected provider to limit
answer to information provided in the context and will pass the queried
objects' AI index as context along with that prompt.

Call your AI client's ``queryRag`` method, passing in a text query.

.. code-block:: typescript

    console.time("gpt-4 Time");
    console.log(
      await astronomyAi.queryRag("What color is the sky on Mars?")
    );
    console.timeEnd("gpt-4 Time");

You can chain additional calls of ``withContext`` or ``withConfig`` to create
additional AI clients, identical except for the newly specified values.

.. code-block:: typescript

    const fastAstronomyAi = astronomyAi.withConfig({
      model: "gpt-3.5-turbo",
    });

    console.time("gpt-3.5 Time");
    console.log(
      await fastAstronomyAi.queryRag("What color is the sky on Mars?")
    );
    console.timeEnd("gpt-3.5 Time");

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
                   options: Partial<AIOptions> = {} \
                 ): EdgeDBAI

    Creates an instance of ``EdgeDBAI`` with the specified client and options.

    :param client:
        An EdgeDB client instance.

    :param string options.model:
        Required. Specifies the AI model to use. This could be a version of GPT
        or any other model supported by EdgeDB AI.

    :param options.prompt:
        Optional. Defines the input prompt for the AI model. The prompt can be
        a simple string, an ID referencing a stored prompt, or a custom prompt
        structure that includes roles and content for more complex
        interactions. The default is the built-in system prompt.


EdgeDBAI
--------

Instances of ``EdgeDBAI`` offer methods for client configuration and utilizing
RAG.

Public methods
^^^^^^^^^^^^^^

.. js:method:: withConfig(options: Partial<AIOptions>): EdgeDBAI

    Returns a new ``EdgeDBAI`` instance with updated configuration options.

    :param string options.model:
        Required. Specifies the AI model to use. This could be a version of GPT
        or any other model supported by EdgeDB AI.

    :param options.prompt:
        Optional. Defines the input prompt for the AI model. The prompt can be
        a simple string, an ID referencing a stored prompt, or a custom prompt
        structure that includes roles and content for more complex
        interactions. The default is the built-in system prompt.

.. js:method:: withContext(context: Partial<QueryContext>): EdgeDBAI

    Returns a new ``EdgeDBAI`` instance with an updated query context.

    :param string context.query:
        Required. Specifies the query to determine the relevant objects and
        index to serve as context for text generation.
    :param string context.variables:
        Optional. Variable settings required for the context query.
    :param string context.globals:
        Optional. Variable settings required for the context query.
    :param number context.max_object_count:
        Optional. A maximum number of objects to return from the context query.

.. js:method:: async queryRag( \
                   message: string, \
                   context: QueryContext = this.context \
                 ): Promise<string>

    Sends a query with context to the configured AI model and returns the
    response as a string.

    :param string message:
        Required. The message to be sent to the text generation provider's API.
    :param string context.query:
        Required. Specifies the query to determine the relevant objects and
        index to serve as context for text generation.
    :param string context.variables:
        Optional. Variable settings required for the context query.
    :param string context.globals:
        Optional. Variable settings required for the context query.
    :param number context.max_object_count:
        Optional. A maximum number of objects to return from the context query.
