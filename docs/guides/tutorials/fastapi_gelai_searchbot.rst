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
    $ uv add fastapi --optional standard \
      && uv add gel \
      && uv sync \
      && source .venv/bin/activate

Step 2. Get started with FastAPI
================================

At this stage we need to follow FastAPI's `tutorial
<https://fastapi.tiangolo.com/tutorial/>`_.

We're going to make a super simple app with one endpoint that takes in a user
query as input and echoes it as an output. First, let's create a file called
`main.py` inside out `app` directory and put the "Hello World" example in it:

.. code-block:: python
    :caption: app/main.py

    from fastapi import FastAPI

    app = FastAPI()


    @app.get("/")
    async def root():
        return {"message": "Hello World"}

To start the server, we need to run:

.. code-block:: bash
    $ fastapi dev main.py

Once the server gets up and running, we can make sure it works using FastAPI's
built-in UI at <http://127.0.0.1:8000/docs>_, or simply using `curl`:

.. code-block:: bash
    $ curl -X GET "http://localhost:8000/"

    {"message": "Hello World"}


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
    $ curl -X POST "http://localhost:8000/search" \
      -H "Content-Type: application/json" \
      -d '{"query": "test search"}'

    {"response":"test search","sources":null}

Step 3. Implement web search
============================

Now that we have our web app infrastructure in place, let's add some substance
to it by implementing web search capabilities.

There're many powerful feature-rich products for LLM-driven web search, but all
we need for now is to simply scrape the text from a few sources so we can feed
it to the model as context. Rather than getting into the weeds here, for the
sake of this tutorial we can delegate this task to an LLM. After some cleanup,
the end result should look similar to this:

.. code-block:: python
    :caption: app/web.py

    import requests
    from bs4 import BeautifulSoup
    from urllib.parse import quote_plus
    import time
    import re

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }


    def search_google(query: str, limit: int = 5) -> list[str]:
        """
        Perform a Google search and return top URLs.
        """

        encoded_query = quote_plus(query)  # encode the search query
        search_url = f"https://www.google.com/search?q={encoded_query}"

        try:
            response = requests.get(search_url, headers=HEADERS)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            result_block = soup.find_all("div", attrs={"class": "g"})

            search_results = []

            for result in result_block:
                link = result.find("a", href=True)
                title = result.find("h3")

                if link and title:
                    link = result.find("a", href=True)
                    if link["href"] not in search_results:
                        search_results.append(link["href"])

                        if len(search_results) >= limit:
                            break

            return search_results

        except requests.exceptions.RequestException as e:
            print(f"Error performing search: {e}")
            return []


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
        urls = search_google(query, limit)

        for url in urls:
            text = extract_text_from_url(url)
            if text:  # Only include if we got some text
                results.append((url, text))
            # Be nice to servers
            time.sleep(1)

        return results

    if __name__ == "__main__":
        print(fetch_web_sources("gel database"))

Good enough for now! We need to add one extra dependency: Beautiful Soup, which
is a commonly used HTML parsing library. Let's add it by running:

.. code-block:: bash
    $ uv add beautifulsoup4

... and test out LLM-generated solution to see if it works:

.. code-block:: bash
    $ python3 app/web.py

Now it's time to reflect the new capabilities in our web app:

.. code-block:: python
    # more code...

    @app.post("/search")
    async def search(search_terms: SearchTerms) -> SearchResult:
        search_result = await generate(search_terms.query)
        return search_result


    async def do_search(query):
        return [{"url": url, "text": text} for url, text in fetch_text_results(query)]

Step 4. Connect to the LLM
==========================

Now that we're capable of scraping text from search results, all that's left for
us is to get the LLM to summarize it for us.

The most straightforward way to do that is to set up some OpenAI chat
completions. To avoid delicate fiddling with HTML requests, let's add their
library as another dependency:

.. code-block:: bash
    $ uv add openai

Then we can grab some code straight from their documentation, and set up LLM
generation like this:

.. code-block:: python
    async def generate(query):
        web_results = await do_search(query)

        system_prompt = (
            "You are a helpful assistant that answers user's questions"
            + " by finding relevant information in web search results"
        )

        prompt = f"User search query: {query}\n\nWeb search results:\n"

        for i, result in enumerate(web_results):
            prompt += f"Result {i} (URL: {result['url']}):\n"
            prompt += f"{result['text']}\n\n"

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

        generated_query = completion.choices[0].message.content

        search_result = SearchResult(
            response=generated_query, sources=[result["url"] for result in web_results]
        )

        # search_result = SearchResult(sources=[result["url"] for result in web_results])
        return search_result

And as usual, let's reflect the new capabilities in the app and test it:

.. code-block:: python
    # beefed up endpoint

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

Defining the schema
-------------------

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

```esdl
type Chat {
	multi messages: Message;
}
```

And chats all belong to a certain user, making up their chat history:

.. code-block:: sdl
    type User {
        name: str {
            constraint exclusive;
        }
        multi chats: ChatHistory;
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
            multi chats: ChatHistory;
        }
    }

For now, let's migrate to our new schema and proceed to writing some queries.

.. code-block:: sdl
    $ gel migration create

.. code-block:: sdl
    $ gel migrate

Writing queries
---------------

First, let's create a directory called `queries` where we're going to put all of
the EdgeQL-related stuff.

Let's get the trivial stuff out of the way first. Here're queries that fetch all
the users, a single user, user's chats, and a particular chat.

.. note::
   Add a fold of some kind to streamline the text

.. code-block:: edgeql

For messages we're going to need something slightly more involved.

Finally, let's run the code generator and set up FastAPI endpoints.

This is great, we now have multi-turn conversations. However, right now we're
simply forwarding the users message straight to Google search. But what happens
if their message is a followup that cannot be used as a standalone search query?

To amend that, we're going to implement an extra step in which the LLM is going
to produce a query for us based on the entire chat history. That way we can be
sure we're progressively working on our query rather than rewriting it from
scratch every time.

Right now the user has to keep rewriting their own query, which is not very
different from the Google itself. Let's make the conversation seem more natural.

We've now successfully build a search bot that keeps track of the history. As a
final cool feature, let's implement a capability for the bot to remember
previous conversations with the user. That way, if you have to narrow down your
search over multiple messages, the bot will be able to recall that and cut
straight to the result next time.

Stage 5. Use Gel's advanced features to create a RAG
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




