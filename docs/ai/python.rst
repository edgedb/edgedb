.. _ref_ai_python:

======
Python
======

:edb-alt-title: EdgeDB AI's Python package

The ``edgedb.ai`` package is an optional binding of the AI extension in EdgeDB.
To use the AI binding, you need to install ``edgedb-python`` with the ``ai``
extra dependencies:

.. code-block:: bash

  $ pip install 'edgedb[ai]'


Usage
=====

Start by importing ``edgedb`` and ``edgedb.ai``:

.. code-block:: python

    import edgedb
    import edgedb.ai


Blocking
--------

The AI binding is built on top of the regular EdgeDB client objects, providing
both blocking and asynchronous versions of its API. For example, a blocking AI
client is initialized like this:

.. code-block:: python

    client = edgedb.create_client()
    gpt4ai = edgedb.ai.create_ai(
        client,
        model="gpt-4-turbo-preview"
    )

Add your query as context:

.. code-block:: python

    astronomy_ai = gpt4ai.with_context(
        query="Astronomy"
    )

The default text generation prompt will ask your selected provider to limit
answer to information provided in the context and will pass the queried
objects' AI index as context along with that prompt.

Call your AI client's ``query_rag`` method, passing in a text query.

.. code-block:: python

    print(
        astronomy_ai.query_rag("What color is the sky on Mars?")
    );

or stream back the results by using ``stream_rag`` instead:

.. code-block:: python

    for data in astronomy_ai.stream_rag("What color is the sky on Mars?"):
        print(data)


Async
-----

To use an async client instead, do this:

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


API reference
=============

.. py:function:: create_ai(client, **kwargs) -> EdgeDBAI

   Creates an instance of ``EdgeDBAI`` with the specified client and options.

   This function ensures that the client is connected before initializing the
   AI with the specified options.

   :param client:
       An EdgeDB client instance.

   :param kwargs:
       Keyword arguments that are passed to the ``AIOptions`` data class to
       configure AI-specific options. These options are:

       * ``model``: The name of the model to be used. (required)
       * ``prompt``: An optional prompt to guide the model's behavior.
         ``None`` will result in the client using the default prompt.
         (default: ``None``)

.. py:function:: create_async_ai(client, **kwargs) -> AsyncEdgeDBAI

   Creates an instance of ``AsyncEdgeDBAI`` w/ the specified client & options.

   This function ensures that the client is connected asynchronously before
   initializing the AI with the specified options.

   :param client:
       An asynchronous EdgeDB client instance.

   :param kwargs:
       Keyword arguments that are passed to the ``AIOptions`` data class to
       configure AI-specific options. These options are:

       * ``model``: The name of the model to be used. (required)
       * ``prompt``: An optional prompt to guide the model's behavior. (default: None)


AI client classes
-----------------


BaseEdgeDBAI
^^^^^^^^^^^^

.. py:class:: BaseEdgeDBAI

   The base class for EdgeDB AI clients.

   This class handles the initialization and configuration of AI clients and
   provides methods to modify their configuration and context dynamically.

   Both the blocking and async AI client classes inherit from this one, so
   these methods are available on an AI client of either type.

   :ivar options:
       An instance of :py:class:`AIOptions`, storing the AI options.

   :ivar context:
       An instance of :py:class:`QueryContext`, storing the context for AI
       queries.

   :ivar client_cls:
       A placeholder for the client class, should be implemented by subclasses.

   :param client:
       An instance of EdgeDB client, which could be either a synchronous or
       asynchronous client.

   :param options:
       AI options to be used with the client.

   :param kwargs:
       Keyword arguments to initialize the query context.

.. py:method:: with_config(**kwargs)

   Creates a new instance of the same class with modified configuration
   options. This method uses the current instance's configuration as a base and
   applies the changes specified in ``kwargs``.

   :param kwargs:
       Keyword arguments that specify the changes to the AI configuration.
       These changes are passed to the ``derive`` method of the current
       configuration options object. Possible keywords include:

       * ``model``: Specifies the AI model to be used. This must be a string.
       * ``prompt``: An optional prompt to guide the model's behavior. This is
         optional and defaults to None.

.. py:method:: with_context(**kwargs)

   Creates a new instance of the same class with a modified context. This
   method preserves the current AI options and client settings, but uses the
   modified context specified by ``kwargs``.

   :param kwargs:
       Keyword arguments that specify the changes to the context. These changes
       are passed to the ``derive`` method of the current context object.
       Possible keywords include:

       * ``query``: The database query string.
       * ``variables``: A dictionary of variables used in the query.
       * ``globals``: A dictionary of global settings affecting the query.
       * ``max_object_count``: An optional integer to limit the number of
         objects returned by the query.


EdgeDBAI
^^^^^^^^

.. py:class:: EdgeDBAI

   A synchronous class for creating EdgeDB AI clients.

   This class provides methods to send queries and receive responses using both
   blocking and streaming communication modes synchronously.

   :ivar client:
       An instance of ``httpx.AsyncClient`` used for making HTTP requests
       asynchronously.

.. py:method:: query_rag(message, context=None) -> str

   Sends a request to the AI provider and returns the response as a string.

   This method uses a blocking HTTP POST request. It raises an HTTP exception
   if the request fails.

   :param message:
       The query string to be sent to the AI model.
   :param context:
       An optional ``QueryContext`` object to provide additional context for
       the query. If not provided, uses the default context of this AI client
       instance.

.. py:method:: stream_rag(message, context=None)

   Opens a connection to the AI provider to stream query responses.

   This method yields data as it is received, utilizing Server-Sent Events
   (SSE) to handle streaming data. It raises an HTTP exception if the request
   fails.

   :param message:
       The query string to be sent to the AI model.
   :param context:
       An optional ``QueryContext`` object to provide additional context for
       the query. If not provided, uses the default context of this AI client
       instance.


AsyncEdgeDBAI
^^^^^^^^^^^^^

.. py:class:: AsyncEdgeDBAI

   An asynchronous class for creating EdgeDB AI clients.

   This class provides methods to send queries and receive responses using both
   blocking and streaming communication modes asynchronously.

   :ivar client:
       An instance of ``httpx.AsyncClient`` used for making HTTP requests
       asynchronously.

.. py:method:: query_rag(message, context=None) -> str
   :noindex:

   Sends an async request to the AI provider, returns the response as a string.

   This method is asynchronous and should be awaited. It raises an HTTP
   exception if the request fails.

   :param message:
       The query string to be sent to the AI model.

   :param context:
       An optional ``QueryContext`` object to provide additional context for
       the query. If not provided, uses the default context of this AI client
       instance.

.. py:method:: stream_rag(message, context=None)
   :noindex:

   Opens an async connection to the AI provider to stream query responses.

   This method yields data as it is received, using asynchronous Server-Sent
   Events (SSE) to handle streaming data. This is an asynchronous generator
   method and should be used in an async for loop. It raises an HTTP exception
   if the connection fails.

   :param message:
       The query string to be sent to the AI model.
   :param context:
       An optional ``QueryContext`` object to provide additional context for
       the query. If not provided, uses the default context of this AI client
       instance.


Other classes
-------------

.. py:class:: ChatParticipantRole

   An enumeration of roles used when defining a custom text generation prompt.

   :cvar SYSTEM:
       Represents a system-level entity or process.
   :cvar USER:
       Represents a human user participating in the chat.
   :cvar ASSISTANT:
       Represents an AI assistant.
   :cvar TOOL:
       Represents a tool or utility used within the chat context.


.. py:class:: Custom

   A single message in a custom text generation prompt.

   :ivar role:
       The role of the chat participant. Must be an instance of
       :py:class:`ChatParticipantRole`.
   :ivar content:
       The content associated with the role, expressed as a string.


.. py:class:: Prompt

   The metadata and content of a text generation prompt.

   :ivar name:
       An optional name identifying the prompt.
   :ivar id:
       An optional unique identifier for the prompt.
   :ivar custom:
       An optional list of :py:class:`Custom` objects, each providing
       role-specific content within the prompt.


.. py:class:: AIOptions

   A data class for AI options, specifying model and prompt settings.

   :ivar model:
       The name of the AI model.
   :ivar prompt:
       An optional :py:class:`Prompt` providing additional guiding information for
       the model.

   :method derive(kwargs):
       Creates a new instance of :py:class:`AIOptions` by merging existing options
       with provided keyword arguments. Returns a new :py:class:`AIOptions`
       instance with updated attributes.

       :param kwargs:
           Keyword arguments to update the current AI options. Possible
           keywords include:

           * ``model`` (str): Update the model name.
           * ``prompt`` (:py:class:`Prompt`): Update or set a new prompt object.


.. py:class:: QueryContext

   A data class defining the context for a query to an AI model.

   :ivar query:
       The base query string.
   :ivar variables:
       An optional dictionary of variables used in the query.
   :ivar globals:
       An optional dictionary of global settings affecting the query.
   :ivar max_object_count:
       An optional integer specifying the maximum number of objects the query
       should return.

   :method derive(kwargs):
       Creates a new instance of :py:class:`QueryContext` by merging existing
       context with provided keyword arguments. Returns a new
       :py:class:`QueryContext` instance with updated attributes.

       :param kwargs:
           Keyword arguments to update the current query context. Possible
           keywords include:

           * ``query`` (str): Update the query string.
           * ``variables`` (dict): Update or set new variables for the query.
           * ``globals`` (dict): Update or set new global settings for the query.
           * ``max_object_count`` (int): Update the limit on the number of objects returned by the query.


.. py:class:: RAGRequest

   A data class defining a request to a text generation model.

   :ivar model:
       The name of the AI model to query.
   :ivar prompt:
       An optional :py:class:`Prompt` associated with the request.
   :ivar context:
       The :py:class:`QueryContext` defining the query context.
   :ivar query:
       The specific query string to be sent to the model.
   :ivar stream:
       A boolean indicating whether the response should be streamed (True) or
       returned in a single response (False).

   :method to_httpx_request():
       Converts the RAGRequest into a dictionary suitable for making an HTTP
       request using the httpx library.
