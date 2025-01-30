.. _ref_guide_fastapi_gelai_searchbot:

=======
FastAPI (Searchbot)
=======

:edb-alt-title: Building a searchbot with memory using FastAPI and Gel AI

In this tutorial we're going to walk you through building a chat bot with search
capabilities using Gel and `FastAPI <https://fastapi.tiangolo.com/>`_.

FastAPI is a framework designed to help you build a web app, well, fast. And Gel
is a data layer designed to help you figure out storage  in your application,
and also do it fast. By the end of this tutorial you will have tried out
different aspects of using those two together, and hopefully come out with
something useful on the other end.

We're going to start by creating an app with FastAPI, then add web search
capabilities to it, and then put search results through a language model to get
a human-friendly answer. After that we'll use Gel to add chat history, and
finish it off with semantic search, so that the bot can remember previous
interactions with the user.

The end result is going to look something like this:

.. image::
    /docs/tutorials/placeholder.png
    :alt: Placeholder
    :width: 100%

Step 1. Initialize the project
==============================

We're going to start by installing `uv <https://docs.astral.sh/uv/>`_ - a Python
package manager that's going to simplify environment management for us. You can
follow their `installation instructions
<https://docs.astral.sh/uv/getting-started/installation/>`_ or simply run:

.. code-block:: bash
    $ curl -LsSf https://astral.sh/uv/install.sh | sh

Once that is done, we can use uv to create scaffolding for our project following
the `documentation <https://docs.astral.sh/uv/guides/projects/>`_:

.. code-block:: bash
    $ uv init searchbot \
      && cd searchbot

For now, we know we're going to need are Gel and FastAPI, so let's add those
following uv's instructions on `managing dependencies
<https://docs.astral.sh/uv/concepts/projects/dependencies/#optional-dependencies>`_,
as well as FastAPI's `installation docs
<https://fastapi.tiangolo.com/#installation>`_. Running ``uv sync`` after that
will create our virtual environment in a ``.venv`` directory and ensure it's
ready. Finally, we'll activate the environment and get started.

.. code-block:: bash
    $ uv add "fastapi[standard]" \
      && uv add gel \
      && uv sync \
      && source .venv/bin/activate

.. note::
   Source the env every time you open a new terminal session.


Step 2. Get started with FastAPI
================================

At this stage we need to follow FastAPI's `tutorial
<https://fastapi.tiangolo.com/tutorial/>`_.

We're going to make a super simple app with one endpoint that takes in a user
query as input and echoes it as an output. First, let's create a file called
`main.py` inside out `app` directory and put the "Hello World" example in it:

.. note::
   make a directory called app first and put init.py there


.. code-block:: python
    :caption: app/main.py

    from fastapi import FastAPI

    app = FastAPI()


    @app.get("/")
    async def root():
        return {"message": "Hello World"}

To start the server, we need to run:

.. code-block:: bash
    $ fastapi dev app/main.py

Once the server gets up and running, we can make sure it works using FastAPI's
built-in UI at <http://127.0.0.1:8000/docs>_, or simply using `curl`:

.. code-block:: bash
    $ curl -X 'GET' \
      'http://127.0.0.1:8000/' \
      -H 'accept: application/json'

    {"message":"Hello World"}


Now, in order to create the endpoint we set out to create, we need to pass our
query as a parameter to it. We'd prefer to have it in the body of the request
since user messages can get pretty long.

In FastAPI land this is done by creating a Pydantic schema and making it the
type of the input parameter. `Pydantic <https://docs.pydantic.dev/latest/>`_ is
a data validation library for Python that's similar to standard dataclasses. It
has many features, but we don't actually need to know about them for now. All we
need to know is that FastAPI uses Pydantic types to automatically figure out
schemae for `input <https://fastapi.tiangolo.com/tutorial/body/>`_, as well as
`output <https://fastapi.tiangolo.com/tutorial/response-model/>`_.

Let's add the following to our `main.py`:

.. code-block:: python
    :caption: app/main.py
    from pydantic import BaseModel


    class SearchTerms(BaseModel):
        query: str

    class SearchResult(BaseModel):
        response: str | None = None
        sources: list[str] | None = None

Now we can define our endpoint and set the two classes we just added as its
argument and return type.

.. code-block:: python
    @app.post("/search")
    async def search(search_terms: SearchTerms) -> SearchResult:
        return SearchResult(response=search_terms.query)

Same as before, we can test the endpoint using the UI, or by sending a request
with `curl`:

.. code-block:: bash
   $ curl -X 'POST' \
      'http://127.0.0.1:8000/search' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "query": "string"
    }'

    {
      "response": "string",
      "sources": null
    }

Step 3. Implement web search
============================

Now that we have our web app infrastructure in place, let's add some substance
to it by implementing web search capabilities.

There're many powerful feature-rich products for LLM-driven web search (such as
Brave for example). But for our purely educational purposes we will set our
sails on the high seas 🏴‍☠️and scrape Google search results. Google tends to
actively resist such behavior, so the most reliable way for us to get our links
is to employ the `googlesearch-python` library:

.. code-block:: bash
    $ uv add googlesearch-python

Having dealt with acquiring the links, we need to parse HTML in order to extract
text. Rather than getting into the weeds, we can generate a reasonable solution
using an LLM. After some cleanup, the end result should look similar to this:

.. note::
   create a new file called web.py

.. code-block:: python
    :caption: app/web.py

    import requests
    from bs4 import BeautifulSoup
    import time
    import re

    from googlesearch import search

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }


    def extract_text_from_url(url: str) -> str:
        """
        Extract main text content from a webpage.
        """
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Remove script and style elements
            for element in soup(["script", "style", "header", "footer", "nav"]):
                element.decompose()

            # Get text and clean it up
            text = soup.get_text(separator=" ")
            # Remove extra whitespace
            text = re.sub(r"\s+", " ", text).strip()

            return text

        except Exception as e:
            print(f"Error extracting text from {url}: {e}")
            return ""


    def fetch_web_sources(query: str, limit: int = 5) -> list[tuple[str, str]]:
        """
        Perform search and extract text from results.
        Returns list of (url, text_content) tuples.
        """
        results = []
        urls = search(query, num_results=limit)

        for url in urls:
            text = extract_text_from_url(url)
            if text:  # Only include if we got some text
                results.append((url, text))
            # Be nice to servers
            time.sleep(1)

        return results

    if __name__ == "__main__":
        print(fetch_web_sources("gel database", limit=1)[0][0])


Good enough for now! We need to add two extra dependencies: requests and
Beautiful Soup, which is a commonly used HTML parsing library. Let's add it by
running:

.. code-block:: bash
    $ uv add beautifulsoup4 requests

... and test out LLM-generated solution to see if it works:

.. code-block:: bash
    $ python3 app/web.py

    https://www.geldata.com

Now it's time to reflect the new capabilities in our web app. Let's update our
search function like this:

.. code-block:: python
    :caption: app/main.py

    from .web import fetch_web_sources

    class WebSource(BaseModel):
        url: str | None = None
        text: str | None = None

    @app.post("/search")
    async def search(search_terms: SearchTerms) -> SearchResult:
        web_sources = await search_web(search_terms.query)
        return SearchResult(
            response=search_terms.query, sources=[source.url for source in web_sources]
        )


    async def search_web(query: str) -> list[WebSource]:
        web_sources = [
            WebSource(url=url, text=text) for url, text in fetch_web_sources(query, limit=1)
        ]
        return web_sources

Testing it using the web UI, and sure enough, we get our sources in the
response!


Step 4. Connect to the LLM
==========================

.. note::
   add links to documentation

Now that we're capable of scraping text from search results, we can forward
those results to the LLM to get a nice-looking summary.

The most straightforward way to do that is to set up some OpenAI chat
completions. To avoid delicate fiddling with HTML requests, let's add their
library as another dependency:

.. code-block:: bash
    $ uv add openai

Then we can grab some code straight from their documentation, and set up LLM
generation like this:

.. note::
    describe env management

.. code-block:: python
    from openai import OpenAI
    from dotenv import load_dotenv()

    _ = load_dotenv()

    llm_client = OpenAI()

    async def generate_answer(
        query: str,
        web_sources: list[WebSource],
    ) -> str:
        system_prompt = (
            "You are a helpful assistant that answers user's questions"
            + " by finding relevant information in web search results."
        )

        prompt = f"User search query: {query}\n\nWeb search results:\n"

        for i, source in enumerate(web_sources):
            prompt += f"Result {i} (URL: {source.url}):\n"
            prompt += f"{source.text}\n\n"

        completion = llm_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
        )

        llm_response = completion.choices[0].message.content
        return llm_response

And as usual, let's reflect the new capabilities in the app and test it:

.. code-block:: python

    @app.post("/search")
    async def search(search_terms: SearchTerms) -> SearchResult:
        web_sources = await search_web(search_terms.query)
        response = await generate_answer(search_terms.query, web_sources)
        return SearchResult(
            response=response, sources=[source.url for source in web_sources]
        )

.. code-block:: bash
   curl -X 'POST' \
      'http://127.0.0.1:8000/search' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "query": "what is gel"
    }'

    {
      "response": "Gel is a next-generation database ... "
      "sources": [
        "https://www.geldata.com/"
      ]
    }

Step 5. Use Gel to implement chat history
=========================================

So far we've built an application that can take in a query, fetch top 5 Google
search results for it, sift through them using an LLM, and generate a nice
answer.

However, right now it's hardly better than google, since you have to basically
start over every time you want to refine the query. To enable more organic
multi-turn interaction we need to add chat history, and in order to enable
gradual query refinement, we need to infer query from that history. Let's do
both using Gel.

To start using Gel, first we need to initialize the project using the command
line interface.

.. code-block:: bash
    $ gel project init

.. note::
   accept all defaults


Defining the schema
-------------------

.. note::
   add links to documentation

The database schema in Gel is defined declaratively. The init command actually
created a stub for it in `dbchema/default.esdl`, that we're going to extend now
with our types.

We obviously want to keep track of messages, so that should be there. By
convention established in the LLM space, each message is going to have a role.

.. code-block:: sdl
    type Message {
        role: str;
        body: str;
        timestamp: datetime {
            default := datetime_current();
        }
        multi sources: str;
    }

Messages are grouped together into a chat, so let's add that, too.

.. code-block:: sdl
    type Chat {
        multi messages: Message;
    }

And chats all belong to a certain user, making up their chat history:

.. code-block:: sdl
    type User {
        name: str {
            constraint exclusive;
        }
        multi chats: Chat;
    }

We're going to keep our schema super simple for now. Some time down the road,
you might wanna leverage Gel's powerful capabilities in order to add auth or AI
features. But we're gonna come back to that.

This is the entire schema we came up with:

.. code-block:: sdl
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

For now, let's migrate to our new schema and proceed to writing some queries.

.. note::
   add links to documentation

.. code-block:: sdl
    $ gel migration create

.. code-block:: sdl
    $ gel migrate

Now that our schema is applied, let's quickly populate the database with some
fake data in order to be able to test the queries.

.. code-block:: bash
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

Make sure that the `app/sample_data/inserts.edgeql` popped up in your file
system, then run:

.. code-block:: bash
    $ gel query -f app/sample_data/inserts.edgeql

    {"id": "862de904-de39-11ef-9713-4fab09220c4a"}
    {"id": "862e400c-de39-11ef-9713-2f81f2b67013"}
    {"id": "862de904-de39-11ef-9713-4fab09220c4a"}
    {"id": "862e400c-de39-11ef-9713-2f81f2b67013"}

That's it! Now there's stuff in the database. Let's verify it by running:

.. code-block:: bash
    $ gel query "select User { name };"

    {"name": "alice"}
    {"name": "bob"}

Writing queries
---------------

.. note::
   add links to documentation

.. note::
   we're assuming knowledge of EdgeQL here. If a refresher is needed, add link


First, let's create a directory inside `app` called `queries` where we're going
to put all of the EdgeQL-related stuff.

Let's start simple. We're going to write a query that fetches all of the users.
In `queries` create a file named `get_users.edgeql` and put the following query
in there:

.. code-block:: edgeql
    :caption: app/queries/get_users.edgeql

    select User { name };


Now run the code generator from the shell:

.. code-block:: bash
    $ gel-py

It's going to automatically locate the `.edgeql` file and generate types for it.
Once that is done, let's use those types to create the endpoint in ``main.py``:

.. code-block:: python
    from edgedb import create_async_client
    from .queries.get_users_async_edgeql import get_users as get_users_query, GetUsersResult
    gel_client = create_async_client()

    @app.get("/users")
    async def get_users() -> list[GetUsersResult]:
        return await get_users_query(gel_client)

With that, we've added our first CRUD endpoint! Let's verify it works as
expected:

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


While we're at it, let's also implement the option to fetch a user by their
username. In order to do that, we need to write a new query in a separate file
`app/queries/get_user_by_name.edgeql`:

.. code-block:: edgeql
    :caption: app/queries/get_users.edgeql

    select User { name }
    filter .name = <str>$name;

After that, we will run the code generator again by calling `gel-py`.
In the app, we are going to reuse the same endpoint that fetches the list of all
users. From now on, if the user calls it without any arguments (e.g.
`http://127.0.0.1/users`), they are going to receive the list of all users, same
as before. But if they pass a username as a query argument like this:
`http://127.0.0.1/users?username=bob`, the system will attempt to fetch a user
named `bob`.

In order to achieve this, we're going to need to add a `Query`-type argument to
our endpoint function. It's default value is going to be `None`, which will
enable us to implement out conditional logic:

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


And once again, let's verify that everything works:

.. code-block:: bash
    $ curl -X 'GET' \
      'http://127.0.0.1:8000/users?username=alice' \
      -H 'accept: application/json'

    {
      "id": "862de904-de39-11ef-9713-4fab09220c4a",
      "name": "alice"
    }


Finally, let's also implement the option to add a new user. For this, just as
before, we'll create a new file `app/queries/create_user.edgeql`, add a query to
it and run code generation.

.. code-block:: edgeql
    select(
        insert User {
            name := <str>$username
        }
    ) {
        name
    }

.. note::
   trickery with the insert wrapped in select

For this, we're going to add a new endpoint. Note that this one has the same
name `/users`, but is for the POST HTTP method.

.. code-block:: python
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

This wraps things up for our user-related functionality. Of course, we now need
to deal with Chats and Messages, too. We're not going to go in depth for those,
since the process would be quite similar to what we just done. Instead, feel
free to implement those endpoints yourself as an exercise, or copy the code
below if you are in rush.

.. code-block:: bash

    $ echo 'select Chat {
        messages,
        user := .<chats[is User],
    } filter .user.name = <str>$username;' > app/queries/get_chats.edgeql && echo 'select Chat {
        messages,
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

.. code-block:: python
    :caption: app/main.py
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


For the `post_messages` function we're going to do something a little bit
different though. Since this is now the primary way for the user to add their
queries to the system, it functionally superceeds the `/search` endpoint we made
before. To this end, this function is where we're going to handle saving
messages, retrieving chat history, invoking web search and generating the
answer.

.. code-block:: python
    @app.post("/messages", status_code=HTTPStatus.CREATED)
    async def post_messages(
        search_terms: SearchTerms,
        username: str = Query(),
        chat_id: str = Query(),
    ) -> SearchResult:
        chat_history = await get_messages_query(
            gel_client, username=username, chat_id=chat_id
        )

        _ = await add_message_query(
            gel_client,
            username=username,
            message_role="user",
            message_body=search_terms.query,
            sources=[],
            chat_id=chat_id,
        )

        search_query = search_terms.query
        web_sources = await search_web(search_query)

        search_result = await generate_answer(
            search_terms.query, chat_history, web_sources
        )

        _ = await add_message_query(
            gel_client,
            username=username,
            message_role="assistant",
            message_body=search_result.response,
            sources=search_result.sources,
            chat_id=chat_id,
        )

        return search_result


Let's not forget to modify the `generate_answer` function, so it can also be
history-aware.

.. code-block:: python
    async def generate_answer(
        query: str,
        chat_history: list[GetMessagesResult],
        web_sources: list[WebSource],
    ) -> SearchResult:
        system_prompt = (
            "You are a helpful assistant that answers user's questions"
            + " by finding relevant information in web search results."
        )

        prompt = f"User search query: {query}\n\nWeb search results:\n"

        for i, source in enumerate(web_sources):
            prompt += f"Result {i} (URL: {source.url}):\n"
            prompt += f"{source.text}\n\n"

        completion = llm_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
        )

        llm_response = completion.choices[0].message.content
        search_result = SearchResult(
            response=llm_response, sources=[source.url for source in web_sources]
        )

        return search_result


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


Next, let's add a couple messages and wait for the bot to respond:

.. code-block:: bash
    $ curl -X 'POST' \
      'http://127.0.0.1:8000/messages?username=charlie&chat_id=544ef3f2-ded8-11ef-ba16-f7f254b95e36' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "query": "tell me about the best database in existence"
    }'

    {
      "response": "Let me tell you about MS SQL Server...",
      "sources": [
        "https://www.itta.net/en/blog/top-10-best-databases-to-use-in-2024/"
      ]
    }

    $ curl -X 'POST' \
      'http://127.0.0.1:8000/messages?username=charlie&chat_id=544ef3f2-ded8-11ef-ba16-f7f254b95e36' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "query": "no i was talking about gel"
    }'

    {
      "response": "Gel is an innovative open-source database ... "
      "sources": [
        "https://divan.dev/posts/edgedb/"
      ]
    }

Finally, let's check that the messages we saw are in fact stored in the chat
history:

.. code-block:: bash
    $ curl -X 'GET' \
      'http://127.0.0.1:8000/messages?username=charlie&chat_id=544ef3f2-ded8-11ef-ba16-f7f254b95e36' \
      -H 'accept: application/json'

    [
      {
        "id": "7e0a0f1a-ded8-11ef-ba16-2344d9519bcf",
        "role": "user",
        "body": "tell me about the best database in existence",
        "sources": [],
        "chat": [
          {
            "id": "544ef3f2-ded8-11ef-ba16-f7f254b95e36"
          }
        ]
      },
      {
        "id": "8980413e-ded8-11ef-a67b-0bb26b4bb123",
        "role": "assistant",
        "body": "Let me tell you about MS SQL Server...",
        "sources": [
          "https://www.itta.net/en/blog/top-10-best-databases-to-use-in-2024/"
        ],
        "chat": [
          {
            "id": "544ef3f2-ded8-11ef-ba16-f7f254b95e36"
          }
        ]
      },
      {
        "id": "a7fa9f4c-ded8-11ef-a67b-8394596c51b4",
        "role": "user",
        "body": "no i was talking about edgedb",
        "sources": [],
        "chat": [
          {
            "id": "544ef3f2-ded8-11ef-ba16-f7f254b95e36"
          }
        ]
      },
      {
        "id": "ad60c43e-ded8-11ef-a67b-1fd15164d162",
        "role": "assistant",
        "body": "EdgeDB is an innovative open-source database ... "
        "sources": [
          "https://divan.dev/posts/edgedb/"
        ],
        "chat": [
          {
            "id": "544ef3f2-ded8-11ef-ba16-f7f254b95e36"
          }
        ]
      }
    ]


In reality this workflow would've been handled by the frontend, providing the
user with a nice inteface to interact with. But even without one we're built a
fully functional chatbot already!

.. note::
   Describe how the post message kind of inherits the search functionality

.. note::
   Modify the generate too so it's history aware.

.. note::
   Add a fold of some kind to streamline the text

.. note::
   Explain that for each query we want to create a separate file inside the
   folder, and that we're doing it to use codegen. Explain what problem codegen
   is supposed to solve for us.


Generating a Google search query
--------------------------------

Congratulations! We just got done implementing multi-turn conversations for our
search bot.

However, there's still one crucial piece missing. Right now we're
simply forwarding the users message straight to Google search. But what happens
if their message is a followup that cannot be used as a standalone search query?

Ideally what we should do is we should infer the search query from the entire
conversation, and use that to perform the search.

Let's implement an extra step in which the LLM is going to produce a query for
us based on the entire chat history. That way we can be sure we're progressively
working on our query rather than rewriting it from scratch every time.

This is what we need to do: every time the user submits a message, we need to
fetch the chat history, extract a search query from it using the LLM, and the
other steps are going to the the same as before. Let's make the follwing
modifications to the `main.py`:

.. code-block:: python
    :caption: app/main.py
    @app.post("/messages", status_code=HTTPStatus.CREATED)
    async def post_messages(
        search_terms: SearchTerms,
        username: str = Query(),
        chat_id: str = Query(),
    ) -> SearchResult:
        chat_history = await get_messages_query(
            gel_client, username=username, chat_id=chat_id
        )

        _ = await add_message_query(
            gel_client,
            username=username,
            message_role="user",
            message_body=search_terms.query,
            sources=[],
            chat_id=chat_id,
        )

        search_query = await generate_search_query(search_terms.query, chat_history)
        web_sources = await search_web(search_query)

        search_result = await generate_answer(
            search_terms.query, chat_history, web_sources
        )

        _ = await add_message_query(
            gel_client,
            username=username,
            message_role="assistant",
            message_body=search_result.response,
            sources=search_result.sources,
            chat_id=chat_id,
        )

        return search_result

    async def generate_search_query(
        query: str, message_history: list[GetMessagesResult]
    ) -> str:
        system_prompt = (
            "You are a helpful assistant."
            + " Your job is to summarize chat history into a standalone google search query."
            + " Only provide the query itself as your response."
        )

        formatted_history = "\n---\n".join(
            [
                f"{message.role}: {message.body} (sources: {message.sources})"
                for message in message_history
            ]
        )
        prompt = f"Chat history: {formatted_history}\n\nUser message: {query} \n\n"

        completion = llm_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
        )

        llm_response = completion.choices[0].message.content
        return llm_response


Step 6. Use Gel's advanced features to create a RAG
====================================================

At this point we have a decent search bot that can refine a search query over
multiple turns of a conversation.

It's time to add a final touch: we can make the bot remember previous similar
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

We begin by enabling the `ai` extension by adding the following like on top of
the `dbschema/default.esdl`:

.. code-block:: sdl
    using extension ai;

    module default {
        # type definitions
    }

Next, we need to configure the API key in Gel for whatever embedding provider
we're going to be using. As per documentation, let's open up `gel cli` and run
the following command:

.. code-block:: edgeql
    configure current database
    insert ext::ai::OpenAIProviderConfig {
      secret := 'sk-....',
    };

In order to get Gel to automatically keep track of creating and updating message
embeddings, all we need to do is create a deferred index like this:

.. code-block:: sdl
    type Message {
        role: str;
        body: str;
        timestamp: datetime {
            default := datetime_current();
        }
        multi sources: str;

        deferred index ext::ai::index(embedding_model := 'text-embedding-3-small')
            on (.body);
    }

And we're done! Gel is going to cook in the background for a while and generate
embedding vectors for our queries. To make sure nothing broke we can follow
Gel's AI documentation and take a look at instance logs:

.. code-block:: bash
   $ gel instance logs -I searchbot

It's time to create the second half of the similarity search - the search query.
The query needs to fetch `k` chats in which there're messages that are most
similar to our current message. This can be a little difficult to visualize in
your head, so here's the query itself:

.. code-block:: edgeql
    # queries

As before, let's run the query generator by calling `gel-py` in the terminal.
Then we need to modify our `search` function to make sure we use the new
capabilities.




