.. _ref_ai_reference:

=======
ext::ai
=======

To activate EdgeDB AI functionality, you can use the :ref:`extension
<ref_datamodel_extensions>` mechanism:

.. code-block:: sdl

    using extension ai;


.. _ref_ai_reference_config:

Configuration
=============

Use the ``configure`` command to set configuration for the AI extension. Update
the values using the ``configure session`` or the ``configure current branch``
command depending on the scope you prefer:

.. code-block:: edgeql-repl

    db> configure current branch
    ... set ext::ai::Config::indexer_naptime := <duration>'0:00:30';
    OK: CONFIGURE DATABASE

The only property available currently is ``indexer_naptime`` which specifies
the minimum delay between deferred ``ext::ai::index`` indexer runs on any given
branch.

Examine the ``extensions`` link of the ``cfg::Config`` object to check the
current config values:

.. code-block:: edgeql-repl

    db> select cfg::Config.extensions[is ext::ai::Config]{*};
    {
      ext::ai::Config {
        id: 1a53f942-d7ce-5610-8be2-c013fbe704db,
        indexer_naptime: <duration>'0:00:30'
      }
    }

You may also restore the default config value using ``configure session
reset`` if you set it on the session or ``configure current branch reset``
if you set it on the branch:

.. code-block:: edgeql-repl

    db> configure current branch reset ext::ai::Config::indexer_naptime;
    OK: CONFIGURE DATABASE


Providers
---------

Provider configs are required for AI indexes (for embedding generation) and for
RAG (for text generation). They may be added via :ref:`ref_cli_edgedb_ui` or by
via EdgeQL:

.. code-block:: edgeql

    configure current database
    insert ext::ai::OpenAIProviderConfig {
      secret := 'sk-....',
    };

The extension makes available types for each provider and for a custom provider
compatible with one of the supported API styles.

* ``ext::ai::OpenAIProviderConfig``
* ``ext::ai::MistralProviderConfig``
* ``ext::ai::AnthropicProviderConfig``
* ``ext::ai::CustomProviderConfig``

All provider types require the ``secret`` property be set with a string
containing the secret provided by the AI vendor. Other properties may
optionally be set:

* ``name``- A unique provider name
* ``display_name``- A human-friendly provider name
* ``api_url``- The provider's API URL
* ``client_id``- ID for the client provided by model API vendor

In addition to the required ``secret`` property,
``ext::ai::CustomProviderConfig requires an ``api_style`` property be set.
Available values are ``ext::ai::ProviderAPIStyle.OpenAI`` and
``ext::ai::ProviderAPIStyle.Anthropic``.

Prompts
-------

You may add prompts either via :ref:`ref_cli_edgedb_ui` or via EdgeQL. Here's
an example of how you might add a prompt with a single message:

.. code-block:: edgeql

    insert ext::ai::ChatPrompt {
      name := 'test-prompt',
      messages := (
        insert ext::ai::ChatPromptMessage {
          participant_role := ext::ai::ChatParticipantRole.System,
          content := "Your message content"
        }
      )
    };

``participant_role`` may be any of these values:

* ``ext::ai::ChatParticipantRole.System``
* ``ext::ai::ChatParticipantRole.User``
* ``ext::ai::ChatParticipantRole.Assistant``
* ``ext::ai::ChatParticipantRole.Tool``

``ext::ai::ChatPromptMessage`` also has a ``participant_name`` property which
is an optional ``str``.


.. _ref_guide_ai_reference_index:

Index
=====

The ``ext::ai::index`` creates a deferred semantic similarity index of an
expression on a type.

.. code-block:: sdl-diff

      module default {
        type Astronomy {
          content: str;
    +     deferred index ext::ai::index(embedding_model := 'text-embedding-3-small')
    +       on (.content);
        }
      };

It can accept several named arguments:

* ``embedding_model``- The name of the model to use for embedding generation as
  a string.

  .. _ref_ai_reference_embedding_models:

  You may use any of these pre-configured embedding generation models:

  **OpenAI**

  * ``text-embedding-3-small``
  * ``text-embedding-3-large``
  * ``text-embedding-ada-002``

  `Learn more about the OpenAI embedding models <https://platform.openai.com/docs/guides/embeddings/embedding-models>`__

  **Mistral**

  * ``mistral-embed``

  `Learn more about the Mistral embedding model <https://docs.mistral.ai/capabilities/embeddings/#mistral-embeddings-api>`__
* ``distance_function``- The function to use for determining semantic
  similarity. Default: ``ext::ai::DistanceFunction.Cosine``

  The distance function may be any of these:

  * ``ext::ai::DistanceFunction.Cosine``
  * ``ext::ai::DistanceFunction.InnerProduct``
  * ``ext::ai::DistanceFunction.L2``
* ``index_type``- The type of index to create. Currently the only option is the
  default: ``ext::ai::IndexType.HNSW``.
* ``index_parameters``- A named tuple of additional index parameters:

  * ``m``- The maximum number of edges of each node in the graph. Increasing
    can increase the accuracy of searches at the cost of index size. Default:
    ``32``
  * ``ef_construction``- Dictates the depth and width of the search when
    building the index. Higher values can lead to better connections and more
    accurate results at the cost of time and resource usage when building the
    index. Default: ``100``


When indexes aren't working…
----------------------------

If you find your queries are not returning the expected results, try
inspecting your instance logs. On an EdgeDB Cloud instance, use the "Logs"
tab in your instance dashboard. On local or :ref:`CLI-linked remote
instances <ref_cli_edgedb_instance_link>`, use ``edgedb instance logs -I
<instance-name>``. You may find the problem there.

Providers impose rate limits on their APIs which can often be the source of
AI index problems. If index creation hits a rate limit, EdgeDB will wait
the ``indexer_naptime`` (see the docs on :ref:`ext::ai configuration
<ref_ai_reference_config>`) and resume index creation.

If your indexed property contains values that exceed the token limit for a
single request, you may consider truncating the property value in your
index expression. You can do this with a string by slicing it:

.. code-block:: sdl

    module default {
      type Astronomy {
        content: str;
        deferred index ext::ai::index(embedding_model := 'text-embedding-3-small')
          on (.content[0:10000]);
      }
    };

This example will slice the first 10,000 characters of the ``content``
property for indexing.

Tokens are not equivalent to characters. For OpenAI embedding generation,
you may test values via `OpenAI's web-based tokenizer
<https://platform.openai.com/tokenizer>`__. You may alternatively download
the library OpenAI uses for tokenization from that same page if you prefer.
By testing, you can get an idea how much of your content can be sent for
indexing.


Functions
=========

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::ai::to_context`
      - :eql:func-desc:`ext::ai::to_context`

    * - :eql:func:`ext::ai::search`
      - :eql:func-desc:`ext::ai::search`


------------


.. eql:function:: ext::ai::to_context(object: anyobject) -> str

    Evaluates the expression of an :ref:`ai::index
    <ref_guide_ai_reference_index>` on the passed object and returns it.

    This can be useful for confirming the basis of embedding generation for a
    particular object or type.

    Given this schema:

    .. code-block:: sdl

        module default {
          type Astronomy {
            topic: str;
            content: str;
            deferred index ext::ai::index(embedding_model := 'text-embedding-3-small')
              on (.topic ++ ' ' ++ .content);
          }
        };

    and with these inserts:

    .. code-block:: edgeql-repl

        db> insert Astronomy {
        ...   topic := 'Mars',
        ...   content := 'Skies on Mars are red.'
        ... }
        db> insert Astronomy {
        ...   topic := 'Earth',
        ...   content := 'Skies on Earth are blue.'
        ... }

    ``to_context`` returns these results:

    .. code-block:: edgeql-repl

        db> select ext::ai::to_context(Astronomy);
        {'Mars Skies on Mars are red.', 'Earth Skies on Earth are blue.'}
        db> select ext::ai::to_context((select Astronomy limit 1));
        {'Mars Skies on Mars are red.'}


------------


.. eql:function:: ext::ai::search( \
                    object: anyobject, \
                    query: array<float32> \
                  ) -> optional tuple<object: anyobject, distance: float64>

    Search an object using its :ref:`ai::index <ref_guide_ai_reference_index>`
    index.

    Returns objects that match the specified semantic query and the
    similarity score.

    .. note::

        The ``query`` argument should *not* be a textual query but the
        embeddings generated *from* a textual query. To have EdgeDB generate
        the query for you along with a text response, try :ref:`our built-in
        RAG <ref_ai_overview_rag>`.

    .. code-block:: edgeql-repl

        db> with query := <array<float32>><json>$query
        ...   select ext::ai::search(Knowledge, query);
        {
          (
            object := default::Knowledge {id: 9af0d0e8-0880-11ef-9b6b-4335855251c4},
            distance := 0.20410746335983276
          ),
          (
            object := default::Knowledge {id: eeacf638-07f6-11ef-b9e9-57078acfce39},
            distance := 0.7843298847773637
          ),
          (
            object := default::Knowledge {id: f70863c6-07f6-11ef-b9e9-3708318e69ee},
            distance := 0.8560434728860855
          ),
        }


HTTP endpoints
==============

Use the AI extension's HTTP endpoints to perform retrieval-augmented generation
using your AI indexes or to generate embeddings against a model of your choice.

.. note::

    All EdgeDB server HTTP endpoints require :ref:`authentication
    <ref_http_auth>`. By default, you may use `HTTP Basic Authentication
    <https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme>`_
    with your EdgeDB username and password.


RAG
---

``POST``: ``https://<edgedb-host>:<port>/branch/<branch-name>/ai/rag``

Responds with text generated by the specified text generation model in response
to the provided query.


Request
^^^^^^^

Make a ``POST`` request to the endpoint with a JSON body. The body may have
these properties:

* ``model`` (string, required): The name of the text generation model to use.

  .. _ref_ai_reference_text_generation_models:

  You may use any of these text generation models:

  **OpenAI**

  * ``gpt-3.5-turbo``
  * ``gpt-4-turbo-preview``

  `Learn more about the OpenAI text generation models <https://platform.openai.com/docs/guides/text-generation>`__

  **Mistral**

  * ``mistral-small-latest``
  * ``mistral-medium-latest``
  * ``mistral-large-latest``

  `Learn more about the Mistral text generation models <https://docs.mistral.ai/getting-started/models/>`__

  **Anthropic**

  * ``claude-3-haiku-20240307``
  * ``claude-3-sonnet-20240229``
  * ``claude-3-opus-20240229``

  `Learn more about the Athropic text generation models <https://docs.anthropic.com/claude/docs/models-overview>`__

* ``query`` (string, required): The query string use as the basis for text
  generation.

* ``context`` (object, required): Settings that define the context of the
  query.

  * ``query`` (string, required): Specifies an expression to determine the
    relevant objects and index to serve as context for text generation. You may
    set this to any expression that produces a set of objects, even if it is
    not a standalone query.

  * ``variables`` (object, optional): A dictionary of variables for use in the
    context query.

  * ``globals`` (object, optional): A dictionary of globals for use in the
    context query.

  * ``max_object_count`` (int, optional): Maximum number of objects to return;
    default is 5.

* ``stream`` (boolean, optional): Specifies whether the response should be
  streamed. Defaults to false.

* ``prompt`` (object, optional): Settings that define a prompt. Omit to use the
  default prompt.

  You may specify an existing prompt by its ``name`` or ``id``, you may define
  a custom prompt inline by sending an array of objects, or you may do both to
  augment an existing prompt with additional custom messages.

  * ``name`` (string, optional) or ``id`` (string, optional): The ``name`` or
    ``id`` of an existing custom prompt to use. Provide only one of these if
    you want to use or start from an existing prompt.

  * ``custom`` (array of objects, optional): Custom prompt messages, each
    containing a ``role`` and ``content``. If no ``name`` or ``id`` was
    provided, the custom messages provided here become the prompt. If one of
    those was provided, these messages will be added to that existing prompt.

**Example request**

.. code-block::

    curl --user <username>:<password> --json '{
      "query": "What color is the sky on Mars?",
      "model": "gpt-4-turbo-preview",
      "context": {"query":"Knowledge"}
    }' http://<edgedb-host>:<port>/branch/main/ai/rag


Response
^^^^^^^^

**Example successful response**

* **HTTP status**: 200 OK
* **Content-Type**: application/json
* **Body**:

  .. code-block:: json

      {"response": "The sky on Mars is red."}

**Example error response**

* **HTTP status**: 400 Bad Request
* **Content-Type**: application/json
* **Body**:

  .. code-block:: json

      {
        "message": "missing required 'query' in request 'context' object",
        "type": "BadRequestError"
      }


Streaming response (SSE)
^^^^^^^^^^^^^^^^^^^^^^^^

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

.. code-block::

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


Embeddings
----------

``POST``: ``https://<edgedb-host>:<port>/branch/<branch-name>/ai/embeddings``

Responds with embeddings generated by the specified embeddings model in
response to the provided input.

Request
^^^^^^^

Make a ``POST`` request to the endpoint with a JSON body. The body may have
these properties:

* ``input`` (array of strings or a single string, required): The text to use as
  the basis for embeddings generation.

* ``model`` (string, required): The name of the embedding model to use. You may
  use any of the supported :ref:`embedding models
  <ref_ai_reference_embedding_models>`.

**Example request**

.. code-block::

    curl --user <username>:<password> --json '{
      "input": "What color is the sky on Mars?",
      "model": "text-embedding-3-small"
    }' http://localhost:10931/branch/main/ai/embeddings


Response
^^^^^^^^

**Example successful response**

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

**Example error response**

* **HTTP status**: 400 Bad Request
* **Content-Type**: application/json
* **Body**:

  .. code-block:: json

      {
        "message": "missing or empty required \"model\" value  in request",
        "type": "BadRequestError"
      }
