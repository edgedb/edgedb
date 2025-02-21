.. _ref_ai_javascript_reference:

==============
JavaScript API
==============

``@gel/ai`` is a wrapper around the :ref:`AI extension
<ref_ai_extai_reference>` in |Gel|.

.. tabs::

    .. code-tab:: bash
        :caption: npm

        $ npm install @gel/ai

    .. code-tab:: bash
        :caption: yarn

        $ yarn add @gel/ai

    .. code-tab:: bash
        :caption: pnpm

        $ pnpm add @gel/ai

    .. code-tab:: bash
        :caption: bun

        $ bun add @gel/ai


Overview
========

The AI package is built on top of the regular |Gel| client objects.

**Example**:

.. code-block:: typescript

    import { createClient } from "gel";
    import { createAI } from "@gel/ai";


    const client = createClient();

    const gpt4Ai = createAI(client, {
      model: "gpt-4-turbo-preview",
    });

    const astronomyAi = gpt4Ai.withContext({
      query: "Astronomy"
    });

    console.log(
      await astronomyAi.queryRag("What color is the sky on Mars?")
    );


Factory functions
=================

.. js:function:: createAI( \
                   client: Client, \
                   options: Partial<AIOptions> = {} \
                 ): GelAI

    Creates an instance of ``GelAI`` with the specified client and options.

    :param client:
        A |Gel| client instance.

    :param string options.model:
        Required. Specifies the AI model to use. This could be a version of GPT
        or any other model supported by |Gel| AI.

    :param options.prompt:
        Optional. Defines the input prompt for the AI model. The prompt can be
        a simple string, an ID referencing a stored prompt, or a custom prompt
        structure that includes roles and content for more complex
        interactions. The default is the built-in system prompt.


Core classes
============


.. js:class:: GelAI

    Instances of ``GelAI`` offer methods for client configuration and utilizing RAG.

    :ivar client:
        An instance of |Gel| client.

.. js:method:: withConfig(options: Partial<AIOptions>): GelAI

    Returns a new ``GelAI`` instance with updated configuration options.

    :param string options.model:
        Required. Specifies the AI model to use. This could be a version of GPT
        or any other model supported by |Gel| AI.

    :param options.prompt:
        Optional. Defines the input prompt for the AI model. The prompt can be
        a simple string, an ID referencing a stored prompt, or a custom prompt
        structure that includes roles and content for more complex
        interactions. The default is the built-in system prompt.

.. js:method:: withContext(context: Partial<QueryContext>): GelAI

    Returns a new ``GelAI`` instance with an updated query context.

    :param string context.query:
        Required. Specifies an expression to determine the relevant objects and
        index to serve as context for text generation. You may set this to any
        expression that produces a set of objects, even if it is not a
        standalone query.
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
        Required. Specifies an expression to determine the relevant objects and
        index to serve as context for text generation. You may set this to any
        expression that produces a set of objects, even if it is not a
        standalone query.
    :param string context.variables:
        Optional. Variable settings required for the context query.
    :param string context.globals:
        Optional. Variable settings required for the context query.
    :param number context.max_object_count:
        Optional. A maximum number of objects to return from the context query.

.. js:method:: async streamRag( \
                   message: string, \
                   context: QueryContext = this.context \
                 ): AsyncIterable<StreamingMessage> & PromiseLike<Response>

    Can be used in two ways:

    - as **an async iterator** - if you want to process streaming data in
        real-time as it arrives, ideal for handling long-running streams.

    - as **a Promise that resolves to a full Response object** - you have
        complete control over how you want to handle the stream, this might be
        useful when you want to manipulate the raw stream or parse it in a custom way.

    :param string message:
        Required. The message to be sent to the text generation provider's API.
    :param string context.query:
        Required. Specifies an expression to determine the relevant objects and
        index to serve as context for text generation. You may set this to any
        expression that produces a set of objects, even if it is not a
        standalone query.
    :param string context.variables:
        Optional. Variable settings required for the context query.
    :param string context.globals:
        Optional. Variable settings required for the context query.
    :param number context.max_object_count:
        Optional. A maximum number of objects to return from the context query.

.. js:method:: async generateEmbeddings( \
                   inputs: string[], \
                   model: string \
                 ): Promise<number[]>

    Generates embeddings for the array of strings.

    :param string[] inputs:
        Required. Strings array to generate embeddings for.
    :param string model:
        Required. Specifies the AI model to use.
