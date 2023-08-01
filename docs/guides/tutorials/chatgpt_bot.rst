.. _ref_guide_chatgpt_bot:

=======
ChatGPT
=======

:edb-alt-title: Build your own docs chatbot with ChatGPT and EdgeDB

*For additional context, check out* `our blog post about why and how we use
ChatGPT via embeddings`_ *to create our “Ask AI” bot which answers questions
related to the EdgeDB docs.*

.. lint-off

.. _our blog post about why and how we use ChatGPT via embeddings:
  https://www.edgedb.com/blog/chit-chatting-with-edgedb-docs-via-chatgpt-and-pgvector

.. lint-on

In this tutorial we're going to build a documentation chatbot with
`Next.js <https://nextjs.org/>`_, `OpenAI <https://openai.com/>`_ and EdgeDB.

Here is what the final result looks like: // todo provide video.


Before we start, let's understand how it all works
==================================================

tl;dr- Training a language model is hard, but using embeddings to give it
access to information beyond what it's trained on is easy… so we will do that!
Now, `get started building <ref_guide_chatgpt_bot_start>`_ or read on for more
detail.

Our chatbot is backed by `OpenAI's ChatGPT <https://openai.com/blog/chatgpt>`_.
ChatGPT is an advanced language model that uses machine learning algorithms to
generate human-like responses based on the input it's given.

There are two options when integrating ChatGPT and language models in general:
fine-tuning the model or using embeddings. Fine-tuning produces the best
result, but it needs more of everything: more money, more time, more resources,
and more training data. That's why many people and businesses use embeddings
instead to provide additional context to an existing language model.

Embeddings are a way to convert words, phrases, or other types of data into a
numerical form that a computer can do math with. All of this is built on top
of the foundation of natural language processing (NLP) which allows computers
to fake an understanding of human language. In the context of NLP, word
embeddings are used to transform words into vectors. These vectors define a
word's position in space where the computer sorts them based on their
syntactic and semantic similarity. For instance, synonyms are closer to each
other, and words that often appear in similar contexts are grouped together.

When using embeddings we are not training the language model. Instead we're
creating embeddings vectors for every piece of documentation which will later
help you find which documentation likely answers a user's question. When a
user asks a question, we create a new embedding for that question and
compare it against the embeddings generated from our documentation to find
the most similar embeddings. The answer is generated using the content that
corresponds to these similar embeddings.


Implementation overview
-----------------------

The general implementation has these steps (which we'll also follow in the
guide):

1. convert documentation into a unified format that is easily digestible
   by the OpenAI API
2. split the converted documentation into sections that can fit into the GPT
   context window
3. create embeddings for each section using `OpenAI's embeddings API
   <https://platform.openai.com/docs/guides/embeddings>`_
4. store the embeddings data in EdgeDB using pgvector


Each time a user asks a question, our app will:

1. query the database for the documentation sections most relevant to
   the question using a similarity function
2. inject these sections as a context into the prompt — together with user's
   question — and submit this request to the OpenAI
3. stream the OpenAI response back to the user in realtime


Prerequisites
=============

This tutorial assumes you have `Node.js <https://nodejs.org/>`_ installed. If
you don't, please install it before continuing.

The build requires other software too, but we'll help you install it as part of
the tutorial.

.. _ref_guide_chatgpt_bot_start:


Let's get started
=================

Let's start by scaffolding our app with the Next.js ``create-next-app`` tool.
Run this in your projects directory or wherever you would like to create the
new directory for this project.

.. code-block:: bash

    $ npx create-next-app --typescript docs-chatbot
    Need to install the following packages:
      create-next-app@13.4.12
    Ok to proceed? (y) y
    ✔ Would you like to use ESLint? … No / Yes
    ✔ Would you like to use Tailwind CSS? … No / Yes
    ✔ Would you like to use `src/` directory? … No / Yes
    ✔ Would you like to use App Router? (recommended) … No / Yes
    ✔ Would you like to customize the default import alias? … No / Yes
    Creating a new Next.js app in /<path>/<to>/<project>/docs-chatbot.

Answer "Yes" to all prompts except "Would you like to use \`src/\` directory?"

Once bootstrapping is complete, you should see a success message:

.. code-block::

    Success! Created docs-chatbot at /<path>/<to>/<project>/docs-chatbot

Before we start writing code, let's first obtain an OpenAI API key, install the
EdgeDB CLI, and create a local EdgeDB instance. We need the API key in order to
use OpenAI's APIs for generating embeddings and answering questions. We need an
EdgeDB instance to store section contents and embeddings, and the EdgeDB CLI
will help us easily set up and manage that instance.


Get an OpenAI API key
---------------------

1. Log in or sign up to the `OpenAI platform
   <https://platform.openai.com/account/api-keys>`_.
2. Create new `secret key <https://platform.openai.com/account/api-keys>`_.
3. Create a ``.env.local`` file in the root of your new Next.js project and
   copy your key here in the following format:
   ``OPENAI_KEY="<my-openai-key>"``.


Install the EdgeDB CLI
----------------------

Before we can create an instance for our project, we need to install the EdgeDB
CLI. On Linux or MacOS, run the following in your terminal and follow the
on-screen instructions:

.. code-block:: bash

    $ curl --proto '=https' --tlsv1.2 -sSf https://sh.edgedb.com | sh

Windows Powershell users can use this command:

.. code-block:: powershell

    PS> iwr https://ps1.edgedb.com -useb | iex

For other installation scenarios, see the "Additional installation methods"
section of `our "Install" page <https://www.edgedb.com/install>`_.


Create a local EdgeDB instance
------------------------------

To create our instance, we will initialize a new EdgeDB project:

.. code-block:: bash

    $ edgedb project init
    No `edgedb.toml` found in `/<path>/<to>/<project>/docs-chatbot` or above

    Do you want to initialize a new project? [Y/n]
    > Y

    Specify the name of EdgeDB instance to use with this project
    [default: docs_chatbot]:
    > docs_chatbot

    Checking EdgeDB versions...
    Specify the version of EdgeDB to use with this project [default: 3.2]:
    > 3.2

The CLI should have set up an EdgeDB project, ad instance, and a database
within that instance. You can confirm project creation by checking for an
``edgedb.toml`` file and a ``dbschema`` directory in your project root. You can
check if the instance is running with the ``edgedb instance list`` command.
Search for the name of the instance you've just created (``docs_chatbot`` if
you're following along) and check the status. Don't worry if the status is
"inactive"; the status will change to "running" automatically when you connect
to the instance. You can connect to the created instance by running ``edgedb``
in the terminal to connect to it via REPL or by running ``edgedb ui`` to
connect using the UI.

Now, let's get the documentation ready to send to OpenAI!


Convert documentation into a unified format
===========================================

For this project, we will be using Markdown files since they are straightforward
for OpenAI's language models to use.

.. note::

    You *can* opt to other simple formats like plain text files or even more
    complex formats like HTML. Since more complex formats include additional
    data beyond what you want the language model to consume (like HTML's tags
    and their attributes), you should first clean those files and extract the
    content before sending it to OpenAI. It's possible to use more complex
    formats *without* doing this, but then you're paying for extra tokens that
    don't improve the answers your chatbot will give users.

Create a ``docs`` folder in the root of your project. We have provided some
Markdown files for this tutorial, but you can replace them with your own. Place
those files in the ``docs`` folder.

.. TODO: Where are these files and how should the user get them?
.. TODO: Devon pls include parts about text files. Files are inside docs folder, the section you deleted : )


Split the documentation into sections
=====================================

Our files are already short enough ..todo explain token limits

.. TODO: Does this ☝️ section just need to be removed since it's not pertinent
   to the build?


Create the schema to store embeddings
=====================================

To be able to store data in the database, we have to create its schema first.
We want to make the schema as simple as possible and store only the relevant
data. We need to store the section content and embeddings. We will also save
each section's relative path and content checksum. The checksum will allow us
to easily determine which files of the documentation has changed every time we
run the embeddings generation script. This way, we can re-generate the embeddings
and write to the database only for those changed sections. We will also need to
save the number of tokens for every section. We will need this later when
calculating how many similar sections fit inside the prompt context.

Open the empty schema file that was generated when you initialized the EdgeDB
project (located at ``dbschema/default.esdl`` from the project directory) and
add this code to it:

.. code-block:: sdl
    :caption: dbschema/default.esdl

    using extension pgvector;

    module default {
      scalar type OpenAIEmbedding extending
        ext::pgvector::vector<1536>;

      type Section {
        required path: str {
          constraint exclusive;
        }
        required content: str;
        required tokens: int16;
        required embedding: OpenAIEmbedding;

        index ext::pgvector::ivfflat_cosine(lists := 3)
          on (.embedding);
      }
    }

We are able to store embeddings and find similar embeddings in the EdgeDB
database because of the ``pgvector`` extension. In order to use it in our
schema, we have to activate the ``ext::pgvector`` module with ``using extension
pgvector`` at the beginning of the schema file. This module gives us access to
``ext::pgvector::vector`` as well as few similarity functions and indexes we
can use later to retrieve embeddings. Read our `pgvector documentation
<https://www.edgedb.com/docs/stdlib/pgvector>`_ for more details on the
extension.

With the extension active, we may now add vector properties when defining our
type. However, in order to be able to use indexes, the vectors in question need
to be a of a fixed length. This can be achieved by creating a custom scalar
extending the vector and specifying the desired length. OpenAI embeddings have
length of 1,536, so that's what we use in our schema.

There is also an index inside the ``Section`` type. In order to speed up
queries, we add the index that corresponds to the ``cosine_similarity``
function which is ``ivfflat_cosine``. We are using the value ``3`` for the
``lists`` parameter because best practice is to use the number of objects
divided by 1,000 for up to 1,000,000 entries. Our database will have around
3,000 total entries which falls well under that threshold. In our case indexing
does not have much impact, but if you plan to store and query a large number of
entries, an index is recommended.

We apply this schema by creating and running a migration.

.. code-block:: bash

    $ edgedb migration create
    $ edgedb migrate


Create embeddings and store them
================================

Before we can script the creation of embeddings, we need to install some
libraries that will help us.

.. lint-off

.. code-block:: bash

    $ npm install openai dotenv tsx edgedb @edgedb/generate gpt-tokenizer --save-dev

.. lint-on


Generating embeddings
---------------------

Finally, we're ready to create embeddings for all sections and store them in
the database we created earlier. Let's make a ``generate-embeddings.ts`` file
inside the project root.

.. code-block:: bash

    $ mkdir gpt && touch generate-embeddings.ts

Open the new file (which is at ``gpt/generate-embeddings.ts`` from your project
root). Let's write the script's skeleton and get an understanding the flow of
tasks we need to perform.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    import dotenv from "dotenv";
    import { Configuration, OpenAIApi } from "openai";
    import { promises as fs } from "fs";
    import { join } from "path";
    import getTokensLen from "./getTokensLen";
    import * as edgedb from "edgedb";
    import e from "../dbschema/edgeql-js";

    dotenv.config({ path: ".env.local" });

    interface Section {
      id?: string;
      path: string;
      tokens: number;
      content: string;
      embedding: number[];
    }

    async function walk(dir: string): Promise<string[]> {
      // ...
    }

    async function prepareSectionsData(
      sectionPaths: string[]
    ): Promise<Section[]> {
      // ...
    }


    async function storeEmbeddings() {
      // ...
    }

    (async function generateEmbeddings() {
      await storeEmbeddings();
    })();


At the top are all imports we will need throughout the file.

After the imports, we use the ``dotenv`` library to import environment
variables from the ``.env.local`` file. (In our case, that's just
``OPENAI_KEY``, which we will need to connect to the OpenAI API).

Next, we define a ``Section`` TypeScript interface that corresponds to
the ``Section`` type we have defined in the schema.

Then we have a few function definitions:

* ``walk`` and ``prepareSectionsData`` will be called from inside
  ``storeEmbeddings``. ``walk`` returns an array of all documentation page
  paths relative to the project root. ``prepareSectionsData`` takes care of
  preparing the ``Section`` objects we will insert into the database and
  returns those as an array.

* ``storeEmbeddings`` coordinates everything, but since it is asynchronous,
  we need to create an additional function to wrap it. CommonJS modules (which
  is what we are building here) cannot use ``await`` at the top level. We have
  to await ``storeEmbeddings`` since it is asynchronous, and the wrapper
  function allows us to do this. We are wrapping it with an IIFE (`immediately
  invoked function expression
  <https://developer.mozilla.org/en-US/docs/Glossary/IIFE>`_).

Apart from the functions in the script, you may have also noticed
``getTokensLen`` among the imports. This function calculates the number of
tokens each section will need as we prepare the section data. We will write it
a bit later.


Getting section paths
^^^^^^^^^^^^^^^^^^^^^

We will store the section paths in the database. This is not necessary, but we
want to associate content and embeddings with a section name or path. Our
sections don't have title or name, so we use their path as a unique identifier.

Since our ``docs`` folder contains files at multiple levels of nesting, we
need a function that loops through all section files, builds an array of all
paths relative to the project root, and sorts those paths. This is what the
``walk`` function does.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    async function walk(dir: string): Promise<string[]> {
      const immediateFiles = await fs.readdir(dir);

      const recursiveFiles: string[][] = await Promise.all(
        immediateFiles.map(async (file: any) => {
          const path = join(dir, file);
          const stats = await fs.stat(path);
          if (stats.isDirectory()) return walk(path);
          else if (stats.isFile()) return [path];
          else return [];
        })
      );

      const flattenedFiles: string[] = recursiveFiles.reduce(
        (all, folderContents) => all.concat(folderContents),
        []
      );

      return flattenedFiles.sort((a, b) => a.localeCompare(b));
    }


The output it produces looks like this:

.. code-block:: typescript

    [
      'docs/datamodel/introspection/functions.md',
      'docs/edgeql/index.md',
      'docs/edgeql/index1.md',
      'docs/edgeql/index2.md'
    ]


Preparing the ``Section`` objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This function will be responsible for collecting the data we need for each
``Section`` object we will store, including making the OpenAI API calls to
generate the embeddings for those.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    async function prepareSectionsData(
      sectionPaths: string[],
    ): Promise<Section[]> {
      const configuration = new Configuration({
        apiKey: process.env.OPENAI_KEY,
      });
      const openai = new OpenAIApi(configuration);

      const contents: string[] = [];
      const sections: Section[] = [];

      for (const path of sectionPaths) {
        const content = await fs.readFile(path, "utf8");
        // OpenAI recommends replacing newlines with spaces for best results (specific to embeddings)
        const contentTrimmed = content.replace(/\n/g, " ");
        contents.push(contentTrimmed);
        sections.push({
          path,
          content,
          tokens: 0,
          embedding: [],
        });
      }

      const embeddingResponse = await openai.createEmbedding({
        model: "text-embedding-ada-002",
        input: contents,
      });

      if (embeddingResponse.status !== 200) {
        throw new Error(embeddingResponse.statusText);
      }

      embeddingResponse.data.data.forEach((item, i) => {
        sections[i].embedding = item.embedding;
        sections[i].tokens = encode(contents[i]).length;
      });

      return sections;
    }

The first thing we do is initialize our OpenAI API library to use the API key
we stored in our ``.env.local`` file. Then, we create a couple of empty arrays
for storing our sections (which will later become ``Section`` objects in the
database) and their contents. But we also store the content in the ``section``,
just without the newlines replaced. Why store them twice? Because it allows us
to generate the embeddings much faster.

We need to be careful about how we approach the API calls to generate the
embeddings since they could have a big impact on how long generation takes,
especially as your documentation grows. The simplest solution would be to make
a single request to the API for each piece of content, but in the case of
EdgeDB's documentation, which has around 3,000 pages, this would take about
half an hour. Since OpenAI's embeddings API can take not only a *single* string
but also an *array* of strings, we can leverage this to batch up all our
content and generate the embeddings with a single request! You can see that
single API call when we set ``embeddingResponse`` to the result of the call to
``openai.createEmbedding``, specifying the model and passing the entire array
of contents.

One downside to this approach is that we do not get token counts from our
embeddings API call since OpenAI only provides these for a single string. We
need the token counts because we have to ensure everything we send to OpenAI's
Completions API — the one that answers the user's question — comes in under the
model's token limit. To do that, we need to know how many tokens we want to
send. That's where the `gpt-tokenizer
<https://www.npmjs.com/package/gpt-tokenizer>`_ library comes in. It counts the
tokens for us so we can store them in the database along with the content and
embeddings.

You see this in action next, as we iterate through all the embeddings we got
back, adding both the embedding and the token lengths to their respective
sections. We imported the ``encode`` function earlier, and you can see that
being called so that we can count and store those. These two additional pieces
of data make the section fully ready to store in the database.

Now that we have sections ready to be stored in the database, let's write the
actual ``storeEmbeddings`` function.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    async function storeEmbeddings() {
      if (!process.env.OPENAI_KEY) {
        return console.log(
          "Environment variable OPENAI_KEY is required: skipping embeddings generation."
        );
      }

      try {
        const client = edgedb.createClient();

        const sectionPaths = await walk("docs");

        console.log(`Discovered ${sectionPaths.length} sections`);

        const sections = await prepareSectionsData(sectionPaths, openai);

        // Delete old data from the DB.
        await e.delete(e.Section).run(client);

        // Bulk-insert all data into EdgeDB database.
        const query = e.params({ sections: e.json }, ({ sections }) => {
          return e.for(e.json_array_unpack(sections), (section) => {
            return e.insert(e.Section, {
              path: e.cast(e.str, section.path),
              content: e.cast(e.str, section.content),
              tokens: e.cast(e.int16, section.tokens),
              embedding: e.cast(e.OpenAIEmbedding, section.embedding),
            });
          });
        });

        await query.run(client, { sections });
      } catch (err) {
        console.error("Error while trying to regenerate all embeddings.", err);
      }

      console.log("Embedding generation complete");
    }

.. TODO: Devon got this far

At the top, we immediately return if ``OPENAI_KEY`` doesn't exist. Otherwise,
we create try/catch block and write the rest of the function inside try block.
If some error is thrown while we try to regenerate embeddings and update the
database we will safely catch it in the catch block.

We create OpenAI and EdgeDB clients. We use OpenAI client to get embeddings,
and EdgeDB client to access and query the database.

Next, we get sections paths and prepare all sections data.

Before we update the database we need to delete the old data from it.
We just delete all ``Section`` objects.

Typescript Query Builder
------------------------
Finally we bulk-insert all sections data in the database. The
`TS binding <https://www.edgedb.com/docs/clients/js/index>`_ offers several
options for writing queries. We recommend using our query builder, and that's
what we use here.

The ``@edgedb/generate`` package that we previously installed provides a set
of code generation tools that are useful when developing an EdgeDB-backed
applications with TypeScript / JavaScript. We need to run a
`query builder <https://www.edgedb.com/docs/clients/js/querybuilder>`_
generator.

.. code-block:: bash

    $ npx @edgedb/generate edgeql-js

This generator gives us a code-first way to write fully-typed EdgeQL
queries with TypeScript. The ``edgeql-js`` folder should have been created
inside ``dbschema`` folder. And from there we import query builder ``e`` that we use
to delete and insert data into the database.

.. code-block:: typescript

    import e from "../dbschema/edgeql-js";

Let's add script to ``package.json`` that will invoke and execute
``generate-embeddings.ts``.

.. code-block:: typescript

    "embeddings": "tsx generate-embeddings.ts"

So now we can invoke the ``generate-embeddings.ts`` script from our terminal
using ``yarn embeddings`` command.

After the script is done (should be less than  a min), we should be able to
open UI with:

.. code-block:: bash

  $ egdedb ui

and see that the DB is indeed updated with embeddings and other relevant data.



Handler function for user's questions
=====================================
Now that we have embeddings we can start working on the handler for user
requests. For the handler we will use next route handler
`<https://nextjs.org/docs/app/building-your-application/routing/route-handlers>`_
. Let's generate new file inside ``pages/api``.

.. code-block:: bash

    $ touch pages/api/generate.ts




.. do we want this at all
.. Why we need to know number of tokens per section
.. ================================================
.. Later when we want to answer to the user's question, we will need to send
.. similar sections as a context to the OpenAI completions endpoint, and we
.. need to know how many tokens each content has in order to stay under the
.. model's token limit.
.. OpenAI's token limit
.. --------------------
.. OpenAI's language models, like GPT-4, work by processing and generating text
.. in chunks referred to as "tokens." These tokens can be as short as one
.. character or as long as one word in English, or even other lengths in
.. different languages.

.. There are two main reasons for having a token limit:

.. 1. **Computational Efficiency**: Processing large amounts of text requires
..    significant computational resources. With each additional token, the model
..    has to keep track of more information and make more complex calculations.
..    Therefore, having a token limit helps to manage these computational
..    requirements and ensure that the model can operate effectively and efficiently.

.. 2. **Memory Constraints**: The models use a technique called "attention" to
..    consider the context in which each token appears. This context includes a
..    certain number of preceding tokens. If the number of tokens exceeds the
..    model's limit, it might lose context for some tokens, which could
..    negatively impact the quality of the generated text.

.. So in general, for the things to work, there is token limit per request which
.. includes both the prompt and the answer. As part of the prompt we will send
.. user's question and similar sections as context and we have to make sure to
.. not send too many sections as context because we will either get error back
.. or the answer can be cut off if there are few tokens left for the answer.
.. We will use in this tutorial GPT-4 and its token limit is 8192.

.. How to calculate number of tokens per section
.. ---------------------------------------------
.. There are at least 3 ways to solve this:

.. - when you send one string to the OpenAI embedding endpoint you will get back
..   together with the embedding array also the **prompt_tokens** field telling
..   you how many tokens the submitted content has and then you can store this
..   in the database together with other data
.. - second way is to use some npm library that generates tokens array for the
..   string you provide, and then you calculate the length of that array
..   (`gpt-tokenizer <https://www.npmjs.com/package/gpt-tokenizer>`_ for example)
.. - the 3rd way is to use OpenAI `tiktoken <https://github.com/openai/tiktoken>`_
..   library which should be faster than npm alternatives (and probably better
..   maintained), but it's supposed to be used with python so we need to write a
..   python script in order to calculate tokens in this way.

.. We can't go with the first approach because prompt_tokens field is received
.. inside embeddings response only when one string is submitted, if array of
.. strings is submitted you only get back the total_tokens number for the whole
.. submitted array.

.. We want to save tokens in the database so that we can retrieve them together
.. with contents when we get similar sections later for the user's request.
.. Another approach is to calculate tokens for every similar section every time
.. we need to construct the prompt, but this is probably a bit slower.

.. We use in the tutorial native OpenAI `tiktoken <https://github.com/openai/tiktoken>`_
.. tool. You can also use `gpt-tokeniser <https://www.npmjs.com/package/gpt-tokenizer>`_
.. . Using npm-library is also easier if you are not familiar with python at all.
