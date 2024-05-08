.. _ref_ai_reference:

=======
ext::ai
=======

To activate EdgeDB AI functionality, you can use the :ref:`extension
<ref_datamodel_extensions>` mechanism:

.. code-block:: sdl

    using extension ai;


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
                    query: <array>float32 \
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


RAG
---

``https://<edgedb-host>:<port>/branch/<branch-name>/ai/rag``

Responds with text generated by the specified text generation model.


Request body
^^^^^^^^^^^^

* ``model`` (string, required): The name of the text generation model to use.

* ``query`` (string, required): The query string use as the basis for text
  generation.

* ``context`` (object, required): Settings that define the context of the
  query.

  * ``query`` (string, required): Specifies the query to determine the relevant
    objects and index to serve as context for text generation.

  * ``variables`` (object, optional): A dictionary of variables for use in the
    context query.

  * ``globals`` (object, optional): A dictionary of globals for use in the
    context query.

  * ``max_object_count`` (int, optional): Maximum number of objects to return;
    default is 5.

* ``stream`` (boolean, optional): Specifies whether the response should be
  streamed. Defaults to false.

* ``prompt`` (object, optional): Settings that define a prompt, overriding the
  default prompt.

  * ``name`` (string, optional): The name of the custom prompt.

  * ``id`` (string, optional): A unique identifier for the custom prompt.

  * ``custom`` (array of objects, optional): Custom prompt messages, each
    containing a ``role`` and ``content``.


Response
^^^^^^^^

A JSON object or a stream of events, depending on the request body's ``stream``
parameter, containing the AI-generated data or error messages if the request
failed.


Embeddings
----------

``https://<edgedb-host>:<port>/branch/<branch-name>/ai/embeddings``

Responds with embeddings by the specified embeddings model.


Request body
^^^^^^^^^^^^

* ``input`` (array of strings or a single string, required): The text to use as
  the basis for embeddings generation.

* ``model`` (string, required): The name of the embedding model to use.


Response
^^^^^^^^

A JSON array containing the embeddings for the provided input.
