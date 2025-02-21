.. _ref_ai_guide_edgeql:

================
Gel AI in EdgeQL
================

:edb-alt-title: How to set up Gel AI in EdgeQL


|Gel| AI brings vector search capabilities and retrieval-augmented generation
directly into the database.


Enable and configure the extension
==================================

.. edb:split-section::

    AI is a |Gel| extension. To enable it, we will need to add the extension
    to the app’s schema:

    .. code-block:: sdl

        using extension ai;


.. edb:split-section::

    |Gel| AI uses external APIs in order to get vectors and LLM completions. For it
    to work, we need to configure an API provider and specify their API key. Let's
    open EdgeQL REPL and run the following query:

    .. code-block:: edgeql

        configure current database
        insert ext::ai::OpenAIProviderConfig {
          secret := 'sk-....',
        };


Now our |Gel| application can take advantage of OpenAI's API to implement AI
capabilities.


.. note::

   |Gel| AI comes with its own :ref:`UI <ref_ai_extai_reference_ui>` that can
   be used to configure providers, set up prompts and test them in a sandbox.


.. note::

   Most API providers require you to set up and account and charge money for
   model use.


Add vectors and perform similarity search
=========================================

.. edb:split-section::

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

.. edb:split-section::

    Here's a shell command you can paste and run that will populate the
    database with some sample data.

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


.. edb:split-section::

    In order to get |Gel| to produce embedding vectors, we need to create a special
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


.. edb:split-section::

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


.. edb:split-section::

    Once we're done with schema modification, we need to apply them by going
    through a migration:

    .. code-block:: bash

        $ gel migration create
        $ gel migrate


.. edb:split-section::

    That's it! |Gel| will make necessary API requests in the background and create an
    index that will enable us to perform efficient similarity search like this:

    .. code-block:: edgeql

        select ext::ai::search(Friend, query_vector);


.. edb:split-section::

    Note that this function accepts an embedding vector as the second argument, not
    a text string. This means that in order to similarity search for a string, we
    need to create a vector embedding for it using the same model as we used to
    create the index. |Gel| offers an HTTP endpoint ``/ai/embeddings`` that can
    handle it for us. All we need to do is to pass the vector it produces into the
    search query:

    .. note::

        Note that we're passing our login and password in order to autheticate the
        request. We can find those using the CLI: :gelcmd:`instance credentials
        --json`. Learn about all the other ways you can authenticate a request
        :ref:`here <ref_http_auth>`.

    .. code-block:: bash

        $ curl --user user:password \
          --json '{"input": "Who helps Komi make friends?", "model": "text-embedding-3-small"}' \
          http://localhost:<port>/branch/main/ai/embeddings \
          | jq -r '.data[0].embedding' \                                                    # extract the embedding out of the JSON
          | tr -d '\n' \                                                                    # remove newlines
          | sed 's/^\[//;s/\]$//' \                                                         # remove square brackets
          | awk '{print "select ext::ai::search(Friend, <array<float32>>[" $0 "]);"}' \     # assemble the query
          | gel query --file -  # pass the query into Gel CLI



Use the built-in RAG
====================

One more feature |Gel| AI offers is built-in retrieval-augmented generation, also
known as RAG.

.. edb:split-section::

    |Gel| comes preconfigured to be able to process our text query, perform
    similarity search across the index we just created, pass the results to an LLM
    and return a response. We can access the built-in RAG using the ``/ai/rag``
    HTTP endpoint:


    .. code-block:: bash

        $ curl --user user:password --json '{
            "query": "Who helps Komi make friends?",
            "model": "gpt-4-turbo-preview",
            "context": {"query":"select Friend"}
          }' http://localhost:<port>/branch/main/ai/rag


.. edb:split-section::

    We can also stream the response like this:


    .. code-block:: bash-diff

          $ curl --user user:password --json '{
              "query": "Who helps Komi make friends?",
              "model": "gpt-4-turbo-preview",
              "context": {"query":"select Friend"},
        +     "stream": true,
            }' http://localhost:<port>/branch/main/ai/rag


Keep going!
===========

You are now sufficiently equipped to use |Gel| AI in your applications.

If you'd like to build something on your own, make sure to check out the
:ref:`Reference manual <ref_ai_extai_reference>` in order to learn the details
about using different APIs and models, configuring prompts or using the UI.
Make sure to also check out the |Gel| AI bindings in :ref:`Python
<ref_ai_python_reference>` and :ref:`JavaScript <ref_ai_javascript_reference>`
if those languages are relevant to you.

And if you would like more guidance for how |Gel| AI can be fit into an
application, take a look at the :ref:`FastAPI Gel AI Tutorial
<ref_guide_fastapi_gelai_searchbot>`, where we're building a search bot using
features you learned about above.

