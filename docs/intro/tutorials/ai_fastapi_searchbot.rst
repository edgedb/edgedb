.. _ref_guide_fastapi_gelai_searchbot:

===============================
Build a Search Bot with FastAPI
===============================

:edb-alt-title: Building a search bot with memory using FastAPI and Gel AI

In this tutorial we're going to walk you through building a chat bot with search
capabilities using Gel and `FastAPI <https://fastapi.tiangolo.com/>`_.

FastAPI is a framework designed to help you build web apps *fast*. Gel is a
data layer designed to help you figure out storage in your application - also
*fast*. By the end of this tutorial, you will have tried out different aspects
of using those two together.

We will start by creating an app with FastAPI, adding web search capabilities,
and then putting search results through a language model to get a
human-friendly answer. After that, we'll use Gel to implement chat history so
that the bot remembers previous interactions with the user. We'll finish it off
with semantic search-based cross-chat memory.


1. Initialize the project
=========================

.. edb:split-section::

  We're going to start by installing `uv <https://docs.astral.sh/uv/>`_ - a Python
  package manager that's going to simplify environment management for us. You can
  follow their `installation instructions
  <https://docs.astral.sh/uv/getting-started/installation/>`_ or simply run:

  .. code-block:: bash

      $ curl -LsSf https://astral.sh/uv/install.sh | sh

.. edb:split-section::

  Once that is done, we can use uv to create scaffolding for our project following
  the `documentation <https://docs.astral.sh/uv/guides/projects/>`_:

  .. code-block:: bash

      $ uv init searchbot \
        && cd searchbot

.. edb:split-section::

  For now, we know we're going to need Gel and FastAPI, so let's add those
  following uv's instructions on `managing dependencies
  <https://docs.astral.sh/uv/concepts/projects/dependencies/#optional-dependencies>`_,
  as well as FastAPI's `installation docs
  <https://fastapi.tiangolo.com/#installation>`_. Running ``uv sync`` after
  that will create our virtual environment in a ``.venv`` directory and ensure
  it's ready. As the last step, we'll activate the environment and get started.

  .. note::

      Every time you open a new terminal session, you should source the
      environment before running ``python``, ``gel`` or ``fastapi`` commands.

  .. code-block:: bash

      $ uv add "fastapi[standard]" \
        && uv add gel \
        && uv sync \
        && source .venv/bin/activate


2. Get started with FastAPI
===========================

.. edb:split-section::

  At this stage we need to follow FastAPI's `tutorial
  <https://fastapi.tiangolo.com/tutorial/>`_ to create the foundation of our app.

  We're going to make a minimal web API with one endpoint that takes in a user
  query as an input and echoes it as an output. First, let's make a directory
  called ``app`` in our project root, and put an empty ``__init__.py`` there.

  .. code-block:: bash

     $ mkdir app && touch app/__init__.py

.. edb:split-section::

  Now let's create a file called ``main.py`` inside the ``app`` directory and put
  the "Hello World" example in it:

  .. code-block:: python
      :caption: app/main.py

      from fastapi import FastAPI

      app = FastAPI()


      @app.get("/")
      async def root():
          return {"message": "Hello World"}


.. edb:split-section::

  To start the server, we'll run:

  .. code-block:: bash

      $ fastapi dev app/main.py


.. edb:split-section::

  Once the server gets up and running, we can make sure it works using FastAPI's
  built-in UI at <http://127.0.0.1:8000/docs>_, or manually with ``curl``:

  .. code-block:: bash

      $ curl -X 'GET' \
        'http://127.0.0.1:8000/' \
        -H 'accept: application/json'

      {"message":"Hello World"}


.. edb:split-section::

  Now, to create the search endpoint we mentioned earlier, we need to pass our
  query as a parameter to it. We'd prefer to have it in the request's body
  since user messages can be long.

  In FastAPI land, this is done by creating a Pydantic schema and making it the
  type of the input parameter. `Pydantic <https://docs.pydantic.dev/latest/>`_ is
  a data validation library for Python. It has many features, but we don't
  actually need to know about them for now. All we need to know is that FastAPI
  uses Pydantic types to automatically figure out schemas for `input
  <https://fastapi.tiangolo.com/tutorial/body/>`_, as well as `output
  <https://fastapi.tiangolo.com/tutorial/response-model/>`_.

  Let's add the following to our ``main.py``:

  .. code-block:: python
      :caption: app/main.py

      from pydantic import BaseModel


      class SearchTerms(BaseModel):
          query: str

      class SearchResult(BaseModel):
          response: str | None = None


.. edb:split-section::

  Now, we can define our endpoint. We'll set the two classes we just created as
  the new endpoint's argument and return type.

  .. code-block:: python
      :caption: app/main.py

      @app.post("/search")
      async def search(search_terms: SearchTerms) -> SearchResult:
          return SearchResult(response=search_terms.query)


.. edb:split-section::

  Same as before, we can test the endpoint using the UI, or by sending a request
  with ``curl``:

  .. code-block:: bash

     $ curl -X 'POST' \
        'http://127.0.0.1:8000/search' \
        -H 'accept: application/json' \
        -H 'Content-Type: application/json' \
        -d '{ "query": "string" }'

      {
        "response": "string",
      }

3. Implement web search
=======================

Now that we have our web app infrastructure in place, let's add some substance
to it by implementing web search capabilities.

.. edb:split-section::

  There're many powerful feature-rich products for LLM-driven web search. But
  in this tutorial we're going to use a much more reliable source of real-world
  information that is comment threads on `Hacker News
  <https://news.ycombinator.com/>`_. Their `web API
  <https://hn.algolia.com/api>`_ is free of charge and doesn't require an
  account. Below is a simple function that requests a full-text search for a
  string query and extracts a nice sampling of comment threads from each of the
  stories that came up in the result.

  We are not going to cover this code sample in too much depth. Feel free to grab
  it save it to ``app/web.py``, or make your own.

  Notice that we've created another Pydantic type called ``WebSource`` to store
  our web search results. There's no framework-related reason for that, it's just
  nicer than passing dictionaries around.

  .. code-block:: python
      :caption: app/web.py
      :class: collapsible

      import requests
      from pydantic import BaseModel
      from datetime import datetime
      import html


      class WebSource(BaseModel):
          """Type that stores search results."""

          url: str | None = None
          title: str | None = None
          text: str | None = None


      def extract_comment_thread(
          comment: dict,
          max_depth: int = 3,
          current_depth: int = 0,
          max_children=3,
      ) -> list[str]:
          """
          Recursively extract comments from a thread up to max_depth.
          Returns a list of formatted comment strings.
          """
          if not comment or current_depth > max_depth:
              return []

          results = []

          # Get timestamp, author and the body of the comment,
          # then pad it with spaces so that it's offset appropriately for its depth

          if comment["text"]:
              timestamp = datetime.fromisoformat(comment["created_at"].replace("Z", "+00:00"))
              author = comment["author"]
              text = html.unescape(comment["text"])
              formatted_comment = f"[{timestamp.strftime('%Y-%m-%d %H:%M')}] {author}: {text}"
              results.append(("  " * current_depth) + formatted_comment)

          # If there're children comments, we are going to extract them too,
          # and add them to the list.

          if comment.get("children"):
              for child in comment["children"][:max_children]:
                  child_comments = extract_comment_thread(child, max_depth, current_depth + 1)
                  results.extend(child_comments)

          return results


      def fetch_web_sources(query: str, limit: int = 5) -> list[WebSource]:
          """
          For a given query perform a full-text search for stories on Hacker News.
          From each of the matched stories extract the comment thread and format it into a single string.
          For each story return its title, url and comment thread.
          """
          search_url = "http://hn.algolia.com/api/v1/search_by_date?numericFilters=num_comments>0"

          # Search for stories
          response = requests.get(
              search_url,
              params={
                  "query": query,
                  "tags": "story",
                  "hitsPerPage": limit,
                  "page": 0,
              },
          )

          response.raise_for_status()
          search_result = response.json()

          # For each search hit fetch and process the story
          web_sources = []
          for hit in search_result.get("hits", []):
              item_url = f"https://hn.algolia.com/api/v1/items/{hit['story_id']}"
              response = requests.get(item_url)
              response.raise_for_status()
              item_result = response.json()

              site_url = f"https://news.ycombinator.com/item?id={hit['story_id']}"
              title = hit["title"]
              comments = extract_comment_thread(item_result)
              text = "\n".join(comments) if len(comments) > 0 else None
              web_sources.append(
                  WebSource(url=site_url, title=title, text=text)
              )

          return web_sources


      if __name__ == "__main__":
          web_sources = fetch_web_sources("edgedb", limit=5)

          for source in web_sources:
              print(source.url)
              print(source.title)
              print(source.text)


.. edb:split-section::

  One more note: this snippet comes with an extra dependency called ``requests``,
  which is a library for making HTTP requests. Let's add it by running:

  .. code-block:: bash

      $ uv add requests


.. edb:split-section::

  Now, we can test our web search on its own by running it like this:

  .. code-block:: bash

      $ python3 app/web.py


.. edb:split-section::

  It's time to reflect the new capabilities in our web app.

  .. code-block:: python
       :caption: app/main.py

       from .web import fetch_web_sources, WebSource

       async def search_web(query: str) -> list[WebSource]:
           raw_sources = fetch_web_sources(query, limit=5)
           return [s for s in raw_sources if s.text is not None]


.. edb:split-section::

  Now we can update the ``/search`` endpoint as follows:

  .. code-block:: python-diff
      :caption: app/main.py

        class SearchResult(BaseModel):
            response: str | None = None
      +     sources: list[WebSource] | None = None


        @app.post("/search")
        async def search(search_terms: SearchTerms) -> SearchResult:
      +     web_sources = await search_web(search_terms.query)
      -     return SearchResult(response=search_terms.query)
      +     return SearchResult(
      +         response=search_terms.query, sources=web_sources
      +     )


4. Connect to the LLM
=====================

Now that we're capable of scraping text from search results, we can forward
those results to the LLM to get a nice-looking summary.

.. edb:split-section::

  There's a million different LLMs accessible via a web API (`one
  <https://docs.anthropic.com/en/api/getting-started>`_, `two
  <https://ai.google.dev/gemini-api/docs>`_, `three
  <https://ollama.com/search>`_, `four <https://docs.mistral.ai/api/>`_ to name
  a few), feel free to choose whichever you prefer. In this tutorial we will
  roll with OpenAI, primarily for how ubiquitous it is. To keep things somewhat
  provider-agnostic, we're going to get completions via raw HTTP requests.
  Let's grab API descriptions from OpenAI's `API documentation
  <https://platform.openai.com/docs/api-reference/chat/create>`_, and set up
  LLM generation like this:

  .. code-block:: python
      :caption: app/main.py

      import requests
      from dotenv import load_dotenv

      _ = load_dotenv()


      def get_llm_completion(system_prompt: str, messages: list[dict[str, str]]) -> str:
          api_key = os.getenv("OPENAI_API_KEY")
          url = "https://api.openai.com/v1/chat/completions"
          headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}

          response = requests.post(
              url,
              headers=headers,
              json={
                  "model": "gpt-4o-mini",
                  "messages": [
                      {"role": "developer", "content": system_prompt},
                      *messages,
                  ],
              },
          )
          response.raise_for_status()
          result = response.json()
          return result["choices"][0]["message"]["content"]


.. edb:split-section::

  Note that this cloud LLM API (and many others) requires a secret key to be
  set as an environment variable. A common way to manage those is to use the
  ``python-dotenv`` library in combinations with a ``.env`` file. Feel free to
  browse `the readme
  <https://github.com/theskumar/python-dotenv?tab=readme-ov-file#getting-started>`_,
  to learn more. Create a file called ``.env`` in the root directory and put
  your api key in there:

  .. code-block:: .env
      :caption: .env

      OPENAI_API_KEY="sk-..."


.. edb:split-section::

  Don't forget to add the new dependency to the environment:

  .. code-block:: bash

      uv add python-dotenv


.. edb:split-section::

  And now we can integrate this LLM-related code with the rest of the app. First,
  let's set up a function that prepares LLM inputs:


  .. code-block:: python
      :caption: app/main.py

      async def generate_answer(
          query: str,
          web_sources: list[WebSource],
      ) -> SearchResult:
          system_prompt = (
              "You are a helpful assistant that answers user's questions"
              + " by finding relevant information in Hacker News threads."
              + " When answering the question, describe conversations that people have around the subject,"
              + " provided to you as a context, or say i don't know if they are completely irrelevant."
          )

          prompt = f"User search query: {query}\n\nWeb search results:\n"

          for i, source in enumerate(web_sources):
              prompt += f"Result {i} (URL: {source.url}):\n"
              prompt += f"{source.text}\n\n"

          messages = [{"role": "user", "content": prompt}]

          llm_response = get_llm_completion(
              system_prompt=system_prompt,
              messages=messages,
          )

          search_result = SearchResult(
              response=llm_response,
              sources=web_sources,
          )

          return search_result


.. edb:split-section::

  Then we can plug that function into the ``/search`` endpoint:

  .. code-block:: python-diff
      :caption: app/main.py

        @app.post("/search")
        async def search(search_terms: SearchTerms) -> SearchResult:
            web_sources = await search_web(search_terms.query)
      +     search_result = await generate_answer(search_terms.query, web_sources)
      +     return search_result
      -     return SearchResult(
      -         response=search_terms.query, sources=web_sources
      -     )


.. edb:split-section::

  And now we can test the result as usual.

  .. code-block:: bash

      $ curl -X 'POST' \
          'http://127.0.0.1:8000/search' \
          -H 'accept: application/json' \
          -H 'Content-Type: application/json' \
          -d '{ "query": "gel" }'


5. Use Gel to implement chat history
====================================

So far we've built an application that can take in a query, fetch some Hacker
News threads for it, sift through them using an LLM, and generate a nice
summary.

However, right now it's hardly user-friendly since you have to speak in
keywords and basically start over every time you want to refine the query. To
enable a more organic multi-turn interaction, we need to add chat history and
infer the query from the context of the entire conversation.

Now's a good time to introduce Gel.

.. edb:split-section::

  In case you need installation instructions, take a look at the :ref:`Quickstart
  <ref_quickstart>`. Once Gel CLI is present in your system, initialize the
  project like this:

  .. code-block:: bash

      $ gel project init --non-interactive


This command is going to put some project scaffolding inside our app, spin up a
local instace of Gel, and then link the two together. From now on, all
Gel-related things that happen inside our project directory are going to be
automatically run on the correct database instance, no need to worry about
connection incantations.


Defining the schema
-------------------

The database :ref:`schema <ref_datamodel_index>` in Gel is defined
declaratively. The :gelcmd:`project init` command has created a file called
:dotgel:`dbschema/default`, which we're going to use to define our types.

.. edb:split-section::

  We obviously want to keep track of the messages, so we need to represent
  those in the schema. By convention established in the LLM space, each message
  is going to have a role in addition to the message content itself. We can
  also get Gel to automatically keep track of message's creation time by adding
  a property callled ``timestamp`` and setting its :ref:`default value
  <ref_datamodel_props>` to the output of the :ref:`datetime_current()
  <ref_std_datetime>` function. Finally, LLM messages in our search bot have
  source URLs associated with them. Let's keep track of those too, by adding a
  :ref:`multi-property <ref_datamodel_props>`.

  .. code-block:: sdl
      :caption: dbschema/default.esdl

      type Message {
          role: str;
          body: str;
          timestamp: datetime {
              default := datetime_current();
          }
          multi sources: str;
      }


.. edb:split-section::

  Messages are grouped together into a chat, so let's add that entity to our
  schema too.

  .. code-block:: sdl
      :caption: dbschema/default.esdl

      type Chat {
          multi messages: Message;
      }


.. edb:split-section::

  And chats all belong to a certain user, making up their chat history. One other
  thing we'd like to keep track of about our users is their username, and it would
  make sense for us to make sure that it's unique by using an ``excusive``
  :ref:`constraint <ref_datamodel_constraints>`.

  .. code-block:: sdl
      :caption: dbschema/default.esdl

      type User {
          name: str {
              constraint exclusive;
          }
          multi chats: Chat;
      }


.. edb:split-section::

  We're going to keep our schema super simple. One cool thing about Gel is that
  it will enable us to easily implement advanced features such as authentication
  or AI down the road, but we're gonna come back to that later.

  For now, this is the entire schema we came up with:

  .. code-block:: sdl
      :caption: dbschema/default.esdl

      module default {
          type Message {
              role: str;
              body: str;
              timestamp: datetime {
                  default := datetime_current();
              }
              multi sources: str;
          }

          type Chat {
              multi messages: Message;
          }

          type User {
              name: str {
                  constraint exclusive;
              }
              multi chats: Chat;
          }
      }


.. edb:split-section::

  Let's use the :gelcmd:`migration create` CLI command, followed by :gelcmd:`migrate` in
  order to migrate to our new schema and proceed to writing some queries.

  .. code-block:: bash

      $ gel migration create
      $ gel migrate


.. edb:split-section::

  Now that our schema is applied, let's quickly populate the database with some
  fake data in order to be able to test the queries. We're going to explore
  writing queries in a bit, but for now you can just run the following command in
  the shell:

  .. code-block:: bash
      :class: collapsible

      $ mkdir app/sample_data && cat << 'EOF' > app/sample_data/inserts.edgeql
      # Create users first
      insert User {
          name := 'alice',
      };
      insert User {
          name := 'bob',
      };
      # Insert chat histories for Alice
      update User
      filter .name = 'alice'
      set {
          chats := {
              (insert Chat {
                  messages := {
                      (insert Message {
                          role := 'user',
                          body := 'What are the main differences between GPT-3 and GPT-4?',
                          timestamp := <datetime>'2024-01-07T10:00:00Z',
                          sources := {'arxiv:2303.08774', 'openai.com/research/gpt-4'}
                      }),
                      (insert Message {
                          role := 'assistant',
                          body := 'The key differences include improved reasoning capabilities, better context understanding, and enhanced safety features...',
                          timestamp := <datetime>'2024-01-07T10:00:05Z',
                          sources := {'openai.com/blog/gpt-4-details', 'arxiv:2303.08774'}
                      })
                  }
              }),
              (insert Chat {
                  messages := {
                      (insert Message {
                          role := 'user',
                          body := 'Can you explain what policy gradient methods are in RL?',
                          timestamp := <datetime>'2024-01-08T14:30:00Z',
                          sources := {'Sutton-Barto-RL-Book-Ch13', 'arxiv:1904.12901'}
                      }),
                      (insert Message {
                          role := 'assistant',
                          body := 'Policy gradient methods are a class of reinforcement learning algorithms that directly optimize the policy...',
                          timestamp := <datetime>'2024-01-08T14:30:10Z',
                          sources := {'Sutton-Barto-RL-Book-Ch13', 'spinning-up.openai.com'}
                      })
                  }
              })
          }
      };
      # Insert chat histories for Bob
      update User
      filter .name = 'bob'
      set {
          chats := {
              (insert Chat {
                  messages := {
                      (insert Message {
                          role := 'user',
                          body := 'What are the pros and cons of different sharding strategies?',
                          timestamp := <datetime>'2024-01-05T16:15:00Z',
                          sources := {'martin-kleppmann-ddia-ch6', 'aws.amazon.com/sharding-patterns'}
                      }),
                      (insert Message {
                          role := 'assistant',
                          body := 'The main sharding strategies include range-based, hash-based, and directory-based sharding...',
                          timestamp := <datetime>'2024-01-05T16:15:08Z',
                          sources := {'martin-kleppmann-ddia-ch6', 'mongodb.com/docs/sharding'}
                      }),
                      (insert Message {
                          role := 'user',
                          body := 'Could you elaborate on hash-based sharding?',
                          timestamp := <datetime>'2024-01-05T16:16:00Z',
                          sources := {'mongodb.com/docs/sharding'}
                      })
                  }
              })
          }
      };
      EOF


.. edb:split-section::

  This created the ``app/sample_data/inserts.edgeql`` file, which we can now execute
  using the CLI like this:

  .. code-block:: bash

      $ gel query -f app/sample_data/inserts.edgeql

      {"id": "862de904-de39-11ef-9713-4fab09220c4a"}
      {"id": "862e400c-de39-11ef-9713-2f81f2b67013"}
      {"id": "862de904-de39-11ef-9713-4fab09220c4a"}
      {"id": "862e400c-de39-11ef-9713-2f81f2b67013"}


.. edb:split-section::

  The :gelcmd:`query` command is one of many ways we can execute a query in Gel. Now
  that we've done it, there's stuff in the database.

  Let's verify it by running:

  .. code-block:: bash

      $ gel query "select User { name };"

      {"name": "alice"}
      {"name": "bob"}


Writing queries
---------------

With schema in place, it's time to focus on getting the data in and out of the
database.

In this tutorial we're going to write queries using :ref:`EdgeQL
<ref_intro_edgeql>` and then use :ref:`codegen <gel-python-codegen>` to
generate typesafe function that we can plug directly into out Python code. If
you are completely unfamiliar with EdgeQL, now is a good time to check out the
basics before proceeding.


.. edb:split-section::

  Let's move on. First, we'll create a directory inside ``app`` called
  ``queries``. This is where we're going to put all of the EdgeQL-related stuff.

  We're going to start by writing a query that fetches all of the users. In
  ``queries`` create a file named ``get_users.edgeql`` and put the following query
  in there:

  .. code-block:: edgeql
      :caption: app/queries/get_users.edgeql

      select User { name };


.. edb:split-section::

  Now run the code generator from the shell:

  .. code-block:: bash

      $ gel-py


.. edb:split-section::

  It's going to automatically locate the ``.edgeql`` file and generate types for
  it. We can inspect generated code in ``app.queries/get_users_async_edgeql.py``.
  Once that is done, let's use those types to create the endpoint in ``main.py``:

  .. code-block:: python
      :caption: app/main.py

      from edgedb import create_async_client
      from .queries.get_users_async_edgeql import get_users as get_users_query, GetUsersResult


      gel_client = create_async_client()

      @app.get("/users")
      async def get_users() -> list[GetUsersResult]:
          return await get_users_query(gel_client)


.. edb:split-section::

  Let's verify it that works as expected:

  .. code-block:: bash

      $ curl -X 'GET' \
      'http://127.0.0.1:8000/users' \
      -H 'accept: application/json'

      [
        {
          "id": "862de904-de39-11ef-9713-4fab09220c4a",
          "name": "alice"
        },
        {
          "id": "862e400c-de39-11ef-9713-2f81f2b67013",
          "name": "bob"
        }
      ]


.. edb:split-section::

  While we're at it, let's also implement the option to fetch a user by their
  username. In order to do that, we need to write a new query in a separate file
  ``app/queries/get_user_by_name.edgeql``:

  .. code-block:: edgeql
      :caption: app/queries/get_user_by_name.edgeql

      select User { name }
      filter .name = <str>$name;


.. edb:split-section::

  After that, we will run the code generator again by calling ``gel-py``. In the
  app, we are going to reuse the same endpoint that fetches the list of all users.
  From now on, if the user calls it without any arguments (e.g.
  ``http://127.0.0.1/users``), they are going to receive the list of all users,
  same as before. But if they pass a username as a query argument like this:
  ``http://127.0.0.1/users?username=bob``, the system will attempt to fetch a user
  named ``bob``.

  In order to achieve this, we're going to need to add a ``Query``-type argument
  to our endpoint function. You can learn more about how to configure this type of
  arguments in `FastAPI's docs
  <https://fastapi.tiangolo.com/tutorial/query-params/>`_. It's default value is
  going to be ``None``, which will enable us to implement our conditional logic:

  .. code-block:: python
      :caption: app/main.py

      from fastapi import Query, HTTPException
      from http import HTTPStatus
      from .queries.get_user_by_name_async_edgeql import (
          get_user_by_name as get_user_by_name_query,
          GetUserByNameResult,
      )


      @app.get("/users")
      async def get_users(
          username: str = Query(None),
      ) -> list[GetUsersResult] | GetUserByNameResult:
          """List all users or get a user by their username"""
          if username:
              user = await get_user_by_name_query(gel_client, name=username)
              if not user:
                  raise HTTPException(
                      HTTPStatus.NOT_FOUND,
                      detail={"error": f"Error: user {username} does not exist."},
                  )
              return user
          else:
              return await get_users_query(gel_client)


.. edb:split-section::

  And once again, let's verify that everything works:

  .. code-block:: bash

      $ curl -X 'GET' \
        'http://127.0.0.1:8000/users?username=alice' \
        -H 'accept: application/json'

      {
        "id": "862de904-de39-11ef-9713-4fab09220c4a",
        "name": "alice"
      }


.. edb:split-section::

  Finally, let's also implement the option to add a new user. For this, just as
  before, we'll create a new file ``app/queries/create_user.edgeql``, add a query
  to it and run code generation.

  Note that in this query we've wrapped the ``insert`` in a ``select`` statement.
  This is a common pattern in EdgeQL, that can be used whenever you would like to
  get something other than object ID when you just inserted it.

  .. code-block:: edgeql
      :caption: app/queries/create_user.edgeql

      select(
          insert User {
              name := <str>$username
          }
      ) {
          name
      }



.. edb:split-section::

  In order to integrate this query into our app, we're going to add a new
  endpoint. Note that this one has the same name ``/users``, but is for the POST
  HTTP method.

  .. code-block:: python
      :caption: app/main.py

      from gel import ConstraintViolationError
      from .queries.create_user_async_edgeql import (
          create_user as create_user_query,
          CreateUserResult,
      )

      @app.post("/users", status_code=HTTPStatus.CREATED)
      async def post_user(username: str = Query()) -> CreateUserResult:
          try:
              return await create_user_query(gel_client, username=username)
          except ConstraintViolationError:
              raise HTTPException(
                  status_code=HTTPStatus.BAD_REQUEST,
                  detail={"error": f"Username '{username}' already exists."},
              )


.. edb:split-section::

  Once more, let's verify that the new endpoint works as expected:

  .. code-block:: bash

      $ curl -X 'POST' \
        'http://127.0.0.1:8000/users?username=charlie' \
        -H 'accept: application/json' \
        -d ''

      {
        "id": "20372a1a-ded5-11ef-9a08-b329b578c45c",
        "name": "charlie"
      }


.. edb:split-section::

  This wraps things up for our user-related functionality. Of course, we now need
  to deal with Chats and Messages, too. We're not going to go in depth for those,
  since the process would be quite similar to what we've just done. Instead, feel
  free to implement those endpoints yourself as an exercise, or copy the code
  below if you are in rush.

  .. code-block:: bash
      :class: collapsible

      $ echo 'select Chat {
          messages: { role, body, sources },
          user := .<chats[is User],
      } filter .user.name = <str>$username;' > app/queries/get_chats.edgeql && echo 'select Chat {
          messages: { role, body, sources },
          user := .<chats[is User],
      } filter .user.name = <str>$username and .id = <uuid>$chat_id;' > app/queries/get_chat_by_id.edgeql && echo 'with new_chat := (insert Chat)
      select (
          update User filter .name = <str>$username
          set {
              chats := assert_distinct(.chats union new_chat)
          }
      ) {
          new_chat_id := new_chat.id
      }' > app/queries/create_chat.edgeql && echo 'with
          user := (select User filter .name = <str>$username),
          chat := (
              select Chat filter .<chats[is User] = user and .id = <uuid>$chat_id
          )
      select Message {
          role,
          body,
          sources,
          chat := .<messages[is Chat]
      } filter .chat = chat;' > app/queries/get_messages.edgeql && echo 'with
          user := (select User filter .name = <str>$username),
      update Chat
      filter .id = <uuid>$chat_id and .<chats[is User] = user
      set {
          messages := assert_distinct(.messages union (
              insert Message {
                  role := <str>$message_role,
                  body := <str>$message_body,
                  sources := array_unpack(<array<str>>$sources)
              }
          ))
      }' > app/queries/add_message.edgeql


.. edb:split-section::

  And these are the endpoint definitions, provided in bulk.

  .. code-block:: python
      :caption: app/main.py
      :class: collapsible

      from .queries.get_chats_async_edgeql import get_chats as get_chats_query, GetChatsResult
      from .queries.get_chat_by_id_async_edgeql import (
          get_chat_by_id as get_chat_by_id_query,
          GetChatByIdResult,
      )
      from .queries.get_messages_async_edgeql import (
          get_messages as get_messages_query,
          GetMessagesResult,
      )
      from .queries.create_chat_async_edgeql import (
          create_chat as create_chat_query,
          CreateChatResult,
      )
      from .queries.add_message_async_edgeql import (
          add_message as add_message_query,
      )


      @app.get("/chats")
      async def get_chats(
          username: str = Query(), chat_id: str = Query(None)
      ) -> list[GetChatsResult] | GetChatByIdResult:
          """List user's chats or get a chat by username and id"""
          if chat_id:
              chat = await get_chat_by_id_query(
                  gel_client, username=username, chat_id=chat_id
              )
              if not chat:
                  raise HTTPException(
                      HTTPStatus.NOT_FOUND,
                      detail={"error": f"Chat {chat_id} for user {username} does not exist."},
                  )
              return chat
          else:
              return await get_chats_query(gel_client, username=username)


      @app.post("/chats", status_code=HTTPStatus.CREATED)
      async def post_chat(username: str) -> CreateChatResult:
          return await create_chat_query(gel_client, username=username)


      @app.get("/messages")
      async def get_messages(
          username: str = Query(), chat_id: str = Query()
      ) -> list[GetMessagesResult]:
          """Fetch all messages from a chat"""
          return await get_messages_query(gel_client, username=username, chat_id=chat_id)


.. edb:split-section::

  For the ``post_messages`` function we're going to do something a little bit
  different though. Since this is now the primary way for the user to add their
  queries to the system, it functionally superceeds the ``/search`` endpoint we
  made before. To this end, this function is where we're going to handle saving
  messages, retrieving chat history, invoking web search and generating the
  answer.

  .. code-block:: python-diff
      :caption: app/main.py

      - @app.post("/search")
      - async def search(search_terms: SearchTerms) -> SearchResult:
      -     web_sources = await search_web(search_terms.query)
      -     search_result = await generate_answer(search_terms.query, web_sources)
      -     return search_result

      + @app.post("/messages", status_code=HTTPStatus.CREATED)
      + async def post_messages(
      +     search_terms: SearchTerms,
      +     username: str = Query(),
      +     chat_id: str = Query(),
      + ) -> SearchResult:
      +     chat_history = await get_messages_query(
      +         gel_client, username=username, chat_id=chat_id
      +     )

      +     _ = await add_message_query(
      +         gel_client,
      +         username=username,
      +         message_role="user",
      +         message_body=search_terms.query,
      +         sources=[],
      +         chat_id=chat_id,
      +     )

      +     search_query = search_terms.query
      +     web_sources = await search_web(search_query)

      +     search_result = await generate_answer(
      +         search_terms.query, chat_history, web_sources
      +     )

      +     _ = await add_message_query(
      +         gel_client,
      +         username=username,
      +         message_role="assistant",
      +         message_body=search_result.response,
      +         sources=search_result.sources,
      +         chat_id=chat_id,
      +     )

      +     return search_result


.. edb:split-section::

  Let's not forget to modify the ``generate_answer`` function, so it can also be
  history-aware.

  .. code-block:: python-diff
      :caption: app/main.py

        async def generate_answer(
            query: str,
      +     chat_history: list[GetMessagesResult],
            web_sources: list[WebSource],
        ) -> SearchResult:
            system_prompt = (
                "You are a helpful assistant that answers user's questions"
                + " by finding relevant information in HackerNews threads."
                + " When answering the question, describe conversations that people have around the subject,"
                + " provided to you as a context, or say i don't know if they are completely irrelevant."
            )

            prompt = f"User search query: {query}\n\nWeb search results:\n"

            for i, source in enumerate(web_sources):
                prompt += f"Result {i} (URL: {source.url}):\n"
                prompt += f"{source.text}\n\n"

      -     messages = [{"role": "user", "content": prompt}]
      +     messages = [
      +         {"role": message.role, "content": message.body} for message in chat_history
      +     ]
      +     messages.append({"role": "user", "content": prompt})

            llm_response = get_llm_completion(
                system_prompt=system_prompt,
                messages=messages,
            )

            search_result = SearchResult(
                response=llm_response,
                sources=web_sources,
            )

            return search_result


.. edb:split-section::

  Ok, this should be it for setting up the chat history. Let's test it. First, we
  are going to start a new chat for our user:

  .. code-block:: bash

      $ curl -X 'POST' \
        'http://127.0.0.1:8000/chats?username=charlie' \
        -H 'accept: application/json' \
        -d ''

      {
        "id": "20372a1a-ded5-11ef-9a08-b329b578c45c",
        "new_chat_id": "544ef3f2-ded8-11ef-ba16-f7f254b95e36"
      }


.. edb:split-section::

  Next, let's add a couple messages and wait for the bot to respond:

  .. code-block:: bash

      $ curl -X 'POST' \
        'http://127.0.0.1:8000/messages?username=charlie&chat_id=544ef3f2-ded8-11ef-ba16-f7f254b95e36' \
        -H 'accept: application/json' \
        -H 'Content-Type: application/json' \
        -d '{
        "query": "best database in existence"
      }'

      $ curl -X 'POST' \
        'http://127.0.0.1:8000/messages?username=charlie&chat_id=544ef3f2-ded8-11ef-ba16-f7f254b95e36' \
        -H 'accept: application/json' \
        -H 'Content-Type: application/json' \
        -d '{
        "query": "gel"
      }'


.. edb:split-section::

  Finally, let's check that the messages we saw are in fact stored in the chat
  history:

  .. code-block:: bash

      $ curl -X 'GET' \
        'http://127.0.0.1:8000/messages?username=charlie&chat_id=544ef3f2-ded8-11ef-ba16-f7f254b95e36' \
        -H 'accept: application/json'


In reality this workflow would've been handled by the frontend, providing the
user with a nice inteface to interact with. But even without one our chatbot is
almost functional by now.

Generating a Google search query
--------------------------------

Congratulations! We just got done implementing multi-turn conversations for our
search bot.

However, there's still one crucial piece missing. Right now we're simply
forwarding the users message straight to the full-text search. But what happens
if their message is a followup that cannot be used as a standalone search
query?

Ideally what we should do is we should infer the search query from the entire
conversation, and use that to perform the search.

Let's implement an extra step in which the LLM is going to produce a query for
us based on the entire chat history. That way we can be sure we're progressively
working on our query rather than rewriting it from scratch every time.


.. edb:split-section::

  This is what we need to do: every time the user submits a message, we need to
  fetch the chat history, extract a search query from it using the LLM, and the
  other steps are going to the the same as before. Let's make the follwing
  modifications to the ``main.py``: first we need to create a function that
  prepares LLM inputs for the search query inference.


  .. code-block:: python
      :caption: app/main.py

      async def generate_search_query(
          query: str, message_history: list[GetMessagesResult]
      ) -> str:
          system_prompt = (
              "You are a helpful assistant."
              + " Your job is to extract a keyword search query"
              + " from a chat between an AI and a human."
              + " Make sure it's a single most relevant keyword to maximize matching."
              + " Only provide the query itself as your response."
          )

          formatted_history = "\n---\n".join(
              [
                  f"{message.role}: {message.body} (sources: {message.sources})"
                  for message in message_history
              ]
          )
          prompt = f"Chat history: {formatted_history}\n\nUser message: {query} \n\n"

          llm_response = get_llm_completion(
              system_prompt=system_prompt, messages=[{"role": "user", "content": prompt}]
          )

          return llm_response


.. edb:split-section::

  And now we can use this function in ``post_messages`` in order to get our
  search query:


  .. code-block:: python-diff
      :caption: app/main.py

        class SearchResult(BaseModel):
            response: str | None = None
      +     search_query: str | None = None
            sources: list[WebSource] | None = None


        @app.post("/messages", status_code=HTTPStatus.CREATED)
        async def post_messages(
            search_terms: SearchTerms,
            username: str = Query(),
            chat_id: str = Query(),
        ) -> SearchResult:
            # 1. Fetch chat history
            chat_history = await get_messages_query(
                gel_client, username=username, chat_id=chat_id
            )

            # 2. Add incoming message to Gel
            _ = await add_message_query(
                gel_client,
                username=username,
                message_role="user",
                message_body=search_terms.query,
                sources=[],
                chat_id=chat_id,
            )

            # 3. Generate a query and perform googling
      -     search_query = search_terms.query
      +     search_query = await generate_search_query(search_terms.query, chat_history)
      +     web_sources = await search_web(search_query)


            # 5. Generate answer
            search_result = await generate_answer(
                search_terms.query,
                chat_history,
                web_sources,
            )
      +     search_result.search_query = search_query  # add search query to the output
      +                                                # to see what the bot is searching for
            # 6. Add LLM response to Gel
            _ = await add_message_query(
                gel_client,
                username=username,
                message_role="assistant",
                message_body=search_result.response,
                sources=[s.url for s in search_result.sources],
                chat_id=chat_id,
            )

            # 7. Send result back to the client
            return search_result


.. edb:split-section::

  Done! We've now fully integrated the chat history into out app and enabled
  natural language conversations. As before, let's quickly test out the
  improvements before moving on:


  .. code-block:: bash

      $ curl -X 'POST' \
          'http://localhost:8000/messages?username=alice&chat_id=d4eed420-e903-11ef-b8a7-8718abdafbe1' \
          -H 'accept: application/json' \
          -H 'Content-Type: application/json' \
          -d '{
          "query": "what are people saying about gel"
        }'

      $ curl -X 'POST' \
          'http://localhost:8000/messages?username=alice&chat_id=d4eed420-e903-11ef-b8a7-8718abdafbe1' \
          -H 'accept: application/json' \
          -H 'Content-Type: application/json' \
          -d '{
          "query": "do they like it or not"
        }'


6. Use Gel's advanced features to create a RAG
==============================================

At this point we have a decent search bot that can refine a search query over
multiple turns of a conversation.

It's time to add the final touch: we can make the bot remember previous similar
interactions with the user using retrieval-augmented generation (RAG).

To achieve this we need to implement similarity search across message history:
we're going to create a vector embedding for every message in the database using
a neural network. Every time we generate a Google search query, we're also going
to use it to search for similar messages in user's message history, and inject
the corresponding chat into the prompt. That way the search bot will be able to
quickly "remember" similar interactions with the user and use them to understand
what they are looking for.

Gel enables us to implement such a system with only minor modifications to the
schema.


.. edb:split-section::

  We begin by enabling the ``ai`` extension by adding the following like on top of
  the :dotgel:`dbschema/default`:

  .. code-block:: sdl-diff
      :caption: dbschema/default.esdl

      + using extension ai;


.. edb:split-section::

  ... and do the migration:


  .. code-block:: bash

      $ gel migration create
      $ gel migrate


.. edb:split-section::

  Next, we need to configure the API key in Gel for whatever embedding provider
  we're going to be using. As per documentation, let's open up the CLI by typing
  ``gel`` and run the following command (assuming we're using OpenAI):

  .. code-block:: edgeql-repl

      searchbot:main> configure current database
      insert ext::ai::OpenAIProviderConfig {
        secret := 'sk-....',
      };

      OK: CONFIGURE DATABASE


.. edb:split-section::

  In order to get Gel to automatically keep track of creating and updating
  message embeddings, all we need to do is create a deferred index like this.
  Don't forget to run a migration one more time!

  .. code-block:: sdl-diff

        type Message {
            role: str;
            body: str;
            timestamp: datetime {
                default := datetime_current();
            }
            multi sources: str;

      +     deferred index ext::ai::index(embedding_model := 'text-embedding-3-small')
      +         on (.body);
        }


.. edb:split-section::

  And we're done! Gel is going to cook in the background for a while and generate
  embedding vectors for our queries. To make sure nothing broke we can follow
  Gel's AI documentation and take a look at instance logs:

  .. code-block:: bash

      $ gel instance logs -I searchbot | grep api.openai.com

      INFO 50121 searchbot 2025-01-30T14:39:53.364 httpx: HTTP Request: POST https://api.openai.com/v1/embeddings "HTTP/1.1 200 OK"


.. edb:split-section::

  It's time to create the second half of the similarity search - the search query.
  The query needs to fetch ``k`` chats in which there're messages that are most
  similar to our current message. This can be a little difficult to visualize in
  your head, so here's the query itself:

  .. code-block:: edgeql
      :caption: app/queries/search_chats.edgeql

      with
          user := (select User filter .name = <str>$username),
              chats := (
                  select Chat
                  filter .<chats[is User] = user
                         and .id != <uuid>$current_chat_id
              )

      select chats {
          distance := min(
              ext::ai::search(
                  .messages,
                  <array<float32>>$embedding,
              ).distance,
          ),
          messages: {
              role, body, sources
          }
      }

      order by .distance
      limit <int64>$limit;


.. edb:split-section::

  .. note::

     Before we can integrate this query into our Python app, we also need to add a
     new dependency for the Python binding: ``httpx-sse``. It's enables streaming
     outputs, which we're not going to use right now, but we won't be able to
     create the AI client without it.

  Let's place in in ``app/queries/search_chats.edgeql``, run the codegen and modify
  our ``post_messages`` endpoint to keep track of those similar chats.

  .. code-block:: python-diff
      :caption: app.main.py

      + from edgedb.ai import create_async_ai, AsyncEdgeDBAI
      + from .queries.search_chats_async_edgeql import (
      +     search_chats as search_chats_query,
      + )

        class SearchResult(BaseModel):
            response: str | None = None
            search_query: str | None = None
            sources: list[WebSource] | None = None
      +     similar_chats: list[str] | None = None


        @app.post("/messages", status_code=HTTPStatus.CREATED)
        async def post_messages(
            search_terms: SearchTerms,
            username: str = Query(),
            chat_id: str = Query(),
        ) -> SearchResult:
            # 1. Fetch chat history
            chat_history = await get_messages_query(
                gel_client, username=username, chat_id=chat_id
            )

            # 2. Add incoming message to Gel
            _ = await add_message_query(
                gel_client,
                username=username,
                message_role="user",
                message_body=search_terms.query,
                sources=[],
                chat_id=chat_id,
            )

            # 3. Generate a query and perform googling
            search_query = await generate_search_query(search_terms.query, chat_history)
            web_sources = await search_web(search_query)

      +     # 4. Fetch similar chats
      +     db_ai: AsyncEdgeDBAI = await create_async_ai(gel_client, model="gpt-4o-mini")
      +     embedding = await db_ai.generate_embeddings(
      +         search_query, model="text-embedding-3-small"
      +     )
      +     similar_chats = await search_chats_query(
      +         gel_client,
      +         username=username,
      +         current_chat_id=chat_id,
      +         embedding=embedding,
      +         limit=1,
      +     )

            # 5. Generate answer
            search_result = await generate_answer(
                search_terms.query,
                chat_history,
                web_sources,
      +         similar_chats,
            )
            search_result.search_query = search_query  # add search query to the output
                                                       # to see what the bot is searching for
            # 6. Add LLM response to Gel
            _ = await add_message_query(
                gel_client,
                username=username,
                message_role="assistant",
                message_body=search_result.response,
                sources=[s.url for s in search_result.sources],
                chat_id=chat_id,
            )

            # 7. Send result back to the client
            return search_result


.. edb:split-section::

  Finally, the answer generator needs to get updated one more time, since we need
  to inject the additional messages into the prompt.

  .. code-block:: python-diff
      :caption: app/main.py

        async def generate_answer(
            query: str,
            chat_history: list[GetMessagesResult],
            web_sources: list[WebSource],
      +     similar_chats: list[list[GetMessagesResult]],
        ) -> SearchResult:
            system_prompt = (
                "You are a helpful assistant that answers user's questions"
                + " by finding relevant information in HackerNews threads."
                + " When answering the question, describe conversations that people have around the subject, provided to you as a context, or say i don't know if they are completely irrelevant."
      +         + " You can reference previous conversation with the user that"
      +         + " are provided to you, if they are relevant, by explicitly referring"
      +         + " to them by saying as we discussed in the past."
            )

            prompt = f"User search query: {query}\n\nWeb search results:\n"

            for i, source in enumerate(web_sources):
                prompt += f"Result {i} (URL: {source.url}):\n"
                prompt += f"{source.text}\n\n"

      +     prompt += "Similar chats with the same user:\n"

      +     formatted_chats = []
      +     for i, chat in enumerate(similar_chats):
      +         formatted_chat = f"Chat {i}: \n"
      +         for message in chat.messages:
      +             formatted_chat += f"{message.role}: {message.body}\n"
      +         formatted_chats.append(formatted_chat)

      +     prompt += "\n".join(formatted_chats)

            messages = [
                {"role": message.role, "content": message.body} for message in chat_history
            ]
            messages.append({"role": "user", "content": prompt})

            llm_response = get_llm_completion(
                system_prompt=system_prompt,
                messages=messages,
            )

            search_result = SearchResult(
                response=llm_response,
                sources=web_sources,
      +         similar_chats=formatted_chats,
            )

            return search_result


.. edb:split-section::

  And one last time, let's check to make sure everything works:

  .. code-block:: bash

      $ curl -X 'POST' \
          'http://localhost:8000/messages?username=alice&chat_id=d4eed420-e903-11ef-b8a7-8718abdafbe1' \
          -H 'accept: application/json' \
          -H 'Content-Type: application/json' \
          -d '{
                "query": "remember that cool db i was talking to you about?"
              }'


Keep going!
===========

This tutorial is over, but this app surely could use way more features!

Basic functionality like deleting messages, a user interface or real web
search, sure. But also authentication or access policies -- Gel will let you
set those up in minutes.

Thanks!







