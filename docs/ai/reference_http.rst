.. _ref_ai_http_reference:

=====================
AI HTTP API Reference
=====================

:edb-alt-title: AI Extension HTTP API

.. note::

    All |Gel| server HTTP endpoints require :ref:`authentication
    <ref_http_auth>`, such as `HTTP Basic Authentication
    <https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme>`_
    with Gel username and password.


Embeddings
==========

``POST``: ``https://<gel-host>:<port>/branch/<branch-name>/ai/embeddings``

Generates text embeddings using the specified embeddings model.


Request headers
---------------

* ``Content-Type: application/json`` (required)


Request body
------------

* ``input`` (array of strings or a single string, required): The text to use as
  the basis for embeddings generation.

* ``model`` (string, required): The name of the embedding model to use. You may
  use any of the supported :ref:`embedding models
  <ref_ai_extai_reference_embedding_models>`.

* ``dimensions`` (number, optional): The number of dimensions to truncate to.

* ``user`` (string, optional): A user identifier for the request.


Example request
---------------

.. code-block:: bash

    curl --user <username>:<password> --json '{
      "input": "What color is the sky on Mars?",
      "model": "text-embedding-3-small"
    }' http://localhost:10931/branch/main/ai/embeddings


Response
--------

* **HTTP status**: 200 OK
* **Content-Type**: application/json
* **Body**:


.. code-block:: json

    {
      "object": "list",
      "data": [
        {
          "object": "embedding",
          "index": 0,
          "embedding": [-0.009434271, 0.009137661]
        }
      ],
      "model": "text-embedding-3-small",
      "usage": {
        "prompt_tokens": 8,
        "total_tokens": 8
      }
    }

.. note::

    The ``embedding`` property is shown here with only two values for brevity,
    but an actual response would contain many more values.


Error response
--------------

* **HTTP status**: 400 Bad Request
* **Content-Type**: application/json
* **Body**:

  .. code-block:: json

      {
        "message": "missing or empty required \"model\" value  in request",
        "type": "BadRequestError"
      }

RAG
===

``POST``: ``https://<gel-host>:<port>/branch/<branch-name>/ai/rag``

Performs retrieval-augmented text generation using the specified model based on
the provided text query and the database content selected using similarity
search.


Request headers
---------------

* ``Content-Type: application/json`` (required)


Request body
------------

* ``context`` (object, required): Settings that define the context of the query.
   * ``query`` (string, required): Specifies an expression to determine the relevant objects and index to serve as context for text generation. You may set this to any expression that produces a set of objects, even if it is not a standalone query.
   * ``variables`` (object, optional): A dictionary of variables for use in the context query.
   * ``globals`` (object, optional): A dictionary of globals for use in the context query.
   * ``max_object_count`` (number, optional): Maximum number of objects to retrieve; default is 5.

* ``model`` (string, required): The name of the text generation model to use.

* ``query`` (string, required): The query string used as the basis for text generation.

* ``stream`` (boolean, optional): Specifies whether the response should be streamed. Defaults to false.

* ``prompt`` (object, optional): Settings that define a prompt. Omit to use the default prompt.
   * ``name`` (string, optional): Name of predefined prompt.
   * ``id`` (string, optional): ID of predefined prompt.
   * ``custom`` (array of objects, optional): Custom prompt messages, each containing a ``role`` and ``content``. If no ``name`` or ``id`` was provided, the custom messages provided here become the prompt. If one of those was provided, these messages will be added to that existing prompt.
      * ``role`` (string): "system", "user", "assistant", or "tool".
      * ``content`` (string|object): Content of the message.
      * ``tool_call_id`` (string): Identifier for tool call.
      * ``tool_calls`` (array): Array of tool calls.

* ``temperature`` (number, optional): Sampling temperature.

* ``top_p`` (number, optional): Nucleus sampling parameter.

* ``max_tokens`` (number, optional): Maximum tokens to generate.

* ``seed`` (number, optional): Random seed.

* ``safe_prompt`` (boolean, optional): Enable safety features.

* ``top_k`` (number, optional): Top-k sampling parameter.

* ``logit_bias`` (object, optional): Token biasing.

* ``logprobs`` (number, optional): Return token log probabilities.

* ``user`` (string, optional): User identifier.


Example request
---------------

.. code-block::

    curl --user <username>:<password> --json '{
      "query": "What color is the sky on Mars?",
      "model": "gpt-4-turbo-preview",
      "context": {"query":"Knowledge"}
    }' http://<gel-host>:<port>/branch/main/ai/rag


Response
--------

* **HTTP status**: 200 OK
* **Content-Type**: application/json
* **Body**:

  .. code-block:: json

      {"response": "The sky on Mars is red."}

Error response
--------------

* **HTTP status**: 400 Bad Request
* **Content-Type**: application/json
* **Body**:

  .. code-block:: json

      {
        "message": "missing required 'query' in request 'context' object",
        "type": "BadRequestError"
      }


Streaming response (SSE)
------------------------

When the ``stream`` parameter is set to ``true``, the server uses `Server-Sent
Events
<https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events>`__
(SSE) to stream responses. Here is a detailed breakdown of the typical
sequence and structure of events in a streaming response:

* **HTTP Status**: 200 OK
* **Content-Type**: text/event-stream
* **Cache-Control**: no-cache

The stream consists of a sequence of five events, each encapsulating part of
the response in a structured format:

1. **Message start**

   * Event type: ``message_start``

   * Data: Starts a message, specifying identifiers and roles.

   .. code-block:: json

      {
        "type": "message_start",
        "message": {
          "id": "<message_id>",
          "role": "assistant",
          "model": "<model_name>"
        }
      }

2. **Content block start**

   * Event type: ``content_block_start``

   * Data: Marks the beginning of a new content block.

   .. code-block:: json

      {
        "type": "content_block_start",
        "index": 0,
        "content_block": {
          "type": "text",
          "text": ""
        }
      }

3. **Content block delta**

   * Event type: ``content_block_delta``

   * Data: Incrementally updates the content, appending more text to the
     message.

   .. code-block:: json

      {
        "type": "content_block_delta",
        "index": 0,
        "delta": {
          "type": "text_delta",
          "text": "The"
        }
      }

   Subsequent ``content_block_delta`` events add more text to the message.

4. **Content block stop**

   * Event type: ``content_block_stop``

   * Data: Marks the end of a content block.

   .. code-block:: json

      {
        "type": "content_block_stop",
        "index": 0
      }

5. **Message stop**

   * Event type: ``message_stop``

   * Data: Marks the end of the message.

   .. code-block:: json

      {"type": "message_stop"}

Each event is sent as a separate SSE message, formatted as shown above. The
connection is closed after all events are sent, signaling the end of the
stream.

**Example SSE response**

.. code-block:: text
    :class: collapsible

    event: message_start
    data: {"type": "message_start", "message": {"id": "chatcmpl-9MzuQiF0SxUjFLRjIdT3mTVaMWwiv", "role": "assistant", "model": "gpt-4-0125-preview"}}

    event: content_block_start
    data: {"type": "content_block_start","index":0,"content_block":{"type":"text","text":""}}

    event: content_block_delta
    data: {"type": "content_block_delta","index":0,"delta":{"type": "text_delta", "text": "The"}}

    event: content_block_delta
    data: {"type": "content_block_delta","index":0,"delta":{"type": "text_delta", "text": " skies"}}

    event: content_block_delta
    data: {"type": "content_block_delta","index":0,"delta":{"type": "text_delta", "text": " on"}}

    event: content_block_delta
    data: {"type": "content_block_delta","index":0,"delta":{"type": "text_delta", "text": " Mars"}}

    event: content_block_delta
    data: {"type": "content_block_delta","index":0,"delta":{"type": "text_delta", "text": " are"}}

    event: content_block_delta
    data: {"type": "content_block_delta","index":0,"delta":{"type": "text_delta", "text": " red"}}

    event: content_block_delta
    data: {"type": "content_block_delta","index":0,"delta":{"type": "text_delta", "text": "."}}

    event: content_block_stop
    data: {"type": "content_block_stop","index":0}

    event: message_delta
    data: {"type": "message_delta", "delta": {"stop_reason": "stop"}}

    event: message_stop
    data: {"type": "message_stop"}


