.. _ref_guide_chatgpt_bot:

=======
ChatGPT
=======

:edb-alt-title: Build your own documentation chatbot with ChatGPT and EdgeDB

*For additional context, check out* `our blog post about why and how we use
ChatGPT via embeddings`_ *to create our “Ask AI” bot which answers questions
related to the EdgeDB docs.*

.. lint-off

.. _our blog post about why and how we use ChatGPT via embeddings:
  https://www.edgedb.com/blog/chit-chatting-with-edgedb-docs-via-chatgpt-and-pgvector

.. lint-on

In this tutorial we're going to build a documentation chatbot with
`Next.js <https://nextjs.org/>`_, `OpenAI <https://openai.com/>`_, and EdgeDB.

.. warning::

    This project makes calls to OpenAI's APIs. At the time of publication, new
    users are granted $5 in API credits to use within the first three months
    after registration. Trials are granted per phone number, not per account.
    If you exhaust your trial credits or if your three months lapse, you will
    need to switch to a paid account in order to build the tutorial project.

How it works
============

*tl;dr- Training a language model is hard, but using embeddings to give it
access to information beyond what it's trained on is easy… so we will do that!
Now,* :ref:`skip ahead to get started building <ref_guide_chatgpt_bot_start>`
*or read on for more detail.*

Our chatbot is backed by `OpenAI's ChatGPT <https://openai.com/blog/chatgpt>`_.
ChatGPT is an advanced large language model (LLM) that uses machine learning
algorithms to generate human-like responses based on the input it's given.

There are two options when integrating ChatGPT and language models in general:
fine-tuning the model or using `embeddings
<https://platform.openai.com/docs/guides/embeddings/what-are-embeddings>`_.
Fine-tuning produces the best result, but it needs more of everything: more
money, more time, more resources, and more training data. That's why many
people and businesses use embeddings instead to provide additional context to
an existing language model.

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
help us find which documentation likely answers a user's question. When a
user asks a question, we create a new embedding for that question and
compare it against the embeddings generated from our documentation to find
the most similar embeddings. The answer is generated using the content that
corresponds to these similar embeddings.

With that out of the way, let's walk through how the pieces fit together.


Implementation overview
-----------------------

The general implementation has two steps (which we'll also follow in the
guide):

1. create embeddings for each section using `OpenAI's embeddings API
   <https://platform.openai.com/docs/guides/embeddings>`_
2. store the embeddings data in EdgeDB using pgvector


Each time a user asks a question, our app will:

1. query the database for the documentation sections most relevant to
   the question using a similarity function
2. inject these sections as a context into the prompt — together with user's
   question — and submit this request to the OpenAI
3. stream the OpenAI response back to the user in real time


Prerequisites
=============

This tutorial assumes you have `Node.js <https://nodejs.org/>`_ installed. If
you don't, please install it before continuing.

The build requires other software too, but we'll help you install it as part of
the tutorial.

.. _ref_guide_chatgpt_bot_start:


Initial setup
=============

Let's start by scaffolding our app with the Next.js ``create-next-app`` tool.
Run this wherever you would like to create the new directory for this project.

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

Choose "Yes" to all except "Would you like to use \`src/\` directory?".

Once bootstrapping is complete, you should see a success message:

.. code-block::

    Success! Created docs-chatbot at
    /<path>/<to>/<project>/docs-chatbot

Let's make a few changes to the ``tsconfig.json`` generated by
``create-next-app``. Update the ``compilerOptions`` object by setting the
``baseUrl`` property to the root with ``"baseUrl": "."``. Later when we add
modules to the root of the project, this will make it easier to import them.

While we're here, let's also change the value of the ``target`` property in
``compilerOptions`` to ``"es2017"``. This will allow us to use ``await`` in a
top-level function call which will make life slightly easier later.

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
   ``OPENAI_API_KEY="<my-openai-api-key>"``.


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

To create our instance, let's initialize our project as an EdgeDB project. Run
the following in the root of the project:

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

The CLI should set up an EdgeDB project, an instance, and a database within
that instance.

- Confirm project creation by checking for an ``edgedb.toml`` file and a
  ``dbschema`` directory in the project root.
- Confirm the instance is running with the ``edgedb instance list`` command.
  Search for the name of the instance you've just created (``docs_chatbot`` if
  you're following along) and check the status. (Don't worry if the status is
  "inactive"; the status will change to "running" automatically when you
  connect to the instance.)
- Confirm you can connect to the created instance by running ``edgedb`` in the
  terminal to connect to it via REPL or by running ``edgedb ui`` to connect
  using the UI.

Now, let's get the documentation ready!


Put the documentation in place
==============================

For this project, we will be using documentation written as Markdown files
since they are straightforward for OpenAI's language models to use.

Create a ``docs`` folder in the root of the project. Here we will place our
Markdown documentation files. You can grab the files we use from `the example
project's GitHub repo
<https://github.com/edgedb/edgedb-examples/tree/main/docs-chatbot/docs>`_ or
add your own.

.. note:: On using formats other than Markdown

    We *could* opt to use other simple formats like plain text files or more
    complex ones like HTML. Since more complex formats can include additional
    data beyond what we want the language model to consume (like HTML's tags
    and their attributes), we may first want to clean those files and extract
    the content before sending it to OpenAI. (We can write our own logic for
    this or use libraries that are available online for conversion, to Markdown
    for example.)

    It's possible to use more complex formats *without* cleaning them, but then
    we're paying for extra tokens that don't improve the answers our chatbot
    will give users.

.. note:: On longer documentation sections

    In this tutorial project, our documentation pages are short, but in
    practice, documentation files can get quite long and may need to be split
    into multiple sections because of the LLM's token limit. LLMs divide text
    into tokens. For English text, 1 token is approximately 4 characters or
    0.75 words. LLMs have limits on the number of tokens they can receive and
    send back.

    One approach to mitigate this is to parse your documentation files and
    create new sections every time you encounter a header. If you use this
    approach, consider section lengths when writing your documentation. Use
    your token limit to set a hard limit on documentation section length to
    make sure it's accessible to your chatbot. If you find a section is too
    long, consider ways you might break it up with additional headings. This
    will probably make it easier to read for your users too!

    To generate embeddings, we will use the ``text-embedding-ada-002`` model.
    Its input token limit is 8,191 tokens. Later, when answering a user's
    questions we will use the `chat completions
    <https://platform.openai.com/docs/guides/gpt/chat-completions-api>`_ model
    ``pt-3.5-turbo``. Its token limit is 4,096 tokens. This limit covers not
    only our input, but also the API's response.

    Later, when we send the user's question, we will also send related sections
    from our documentation as part of the input to the chat completions API.
    This is why it's important to keep our sections short: we want to leave
    enough space for the answer.

    If the related sections are too long and, together with the user's
    question, exceed the 4,096 token limit, we will get an error back from
    OpenAI. If the length of the question and similar sections are too close to
    the token limit but not over it, the API will send an answer, but the
    answer will be cut off when the limit is reached.

    We want to avoid either of these outcomes by making sure we always have
    enough token headroom for all the input and the LLM's response. That's why
    we will later set 1,500 tokens as the maximum number of tokens we will use
    for our related sections, and it's also why it's important that sections be
    relatively short.

    If your application has longer documentation files, make sure to figure out
    a strategy for splitting those before you generate your embeddings.


Create the schema to store embeddings
=====================================

To be able to store data in the database, we have to create its schema first.
We want to make the schema as simple as possible and store only the relevant
data. We need to store the section's embeddings, content, and the number of
tokens. The embeddings allow us to match content to questions. The content
gives us context to feed to the LLM. We will need the token count later when
calculating how many related sections fit inside the prompt context while
staying under the model's token limit.

Open the empty schema file that was generated when we initialized the EdgeDB
project (located at ``dbschema/default.esdl`` from the project directory).
We'll walk through what we'll add to it, one step at a time.

.. code-block:: sdl
    :caption: dbschema/default.esdl

    using extension pgvector;

We are able to store embeddings and find similar embeddings in the EdgeDB
database because of the ``pgvector`` extension. In order to use it in our
schema, we have to activate the ``ext::pgvector`` module with ``using extension
pgvector`` at the beginning of the schema file. This module gives us access to
``ext::pgvector::vector`` as well as few similarity functions and indexes we
can use later to retrieve embeddings. Read our `pgvector documentation
<https://www.edgedb.com/docs/stdlib/pgvector>`_ for more details on the
extension.

Just below that, we can start building our module by creating a new scalar
type.

.. code-block:: sdl
    :caption: dbschema/default.esdl

    …
    module default {
      scalar type OpenAIEmbedding extending
        ext::pgvector::vector<1536>;

      type Section {
        # We will build this out next
      }
    }

With the extension active, we may now add vector properties when defining our
type. However, in order to be able to use indexes, the vectors in question need
to be a of a fixed length. This can be achieved by creating a custom scalar
extending the vector and specifying the desired length. OpenAI embeddings have
length of 1,536, so that's what we use in our schema for this custom scalar.

Now, the ``Section`` type:

.. code-block:: sdl
    :caption: dbschema/default.esdl

    …
      type Section {
        required content: str;
        required tokens: int16;
        required embedding: OpenAIEmbedding;

        index ext::pgvector::ivfflat_cosine(lists := 3)
          on (.embedding);
      }
    …

The ``Section`` contains properties to store the path to the file, the content,
a count of tokens, and the embedding, which is of the custom scalar type we
created in the previous step.

We've also added an index inside the ``Section`` type to speed up queries. In
order to work properly, the index should correspond to the
``cosine_similarity`` function we're going to use to find sections related to
the user's question. That corresponding index is ``ivfflat_cosine``.

We are using the value ``3`` for the ``lists`` parameter because best practice
is to use the number of objects divided by 1,000 for up to 1,000,000 objects.
Our database will have around 3,000 total objects which falls well under that
threshold.

In our case indexing does not have much impact, but if you plan to store and
query a large number of entries, you'll see performance gains by adding this
index.

Put that all together, and your entire schema file should look like this:

.. code-block:: sdl
    :caption: dbschema/default.esdl

    using extension pgvector;

    module default {
      scalar type OpenAIEmbedding extending
        ext::pgvector::vector<1536>;

      type Section {
        required content: str;
        required tokens: int16;
        required embedding: OpenAIEmbedding;

        index ext::pgvector::ivfflat_cosine(lists := 3)
          on (.embedding);
      }
    }

We apply this schema by creating and running a migration.

.. code-block:: bash

    $ edgedb migration create
    $ edgedb migrate

.. note::

    In this tutorial we will regenerate all embeddings every time we run the
    embeddings generation script, wiping all data and saving new ``Section``
    objects for all of the documentation. This might be a reasonable approach
    if you don't have much documentation, but if you have a lot of
    documentation, you may want a more sophisticated approach that operates on
    only documentation sections which have changed.

    You can achieve this by saving content checksums and section paths as part
    of your ``Section`` objects. The next time you run generation, compare the
    section's current checksum with the one you stored in the database, finding
    it by its path. You don't need to generate embeddings and update the
    database for a given section unless the two checksums are different
    indicating something has changed.

    If you decide to go this route, add properties your ``Section`` as shown
    below:

    .. code-block:: sdl-diff
        :caption: dbschema/default.esdl

          type Section {
        +   required path: str {
        +     constraint exclusive;
        +   }
        +   required checksum: str;
            # The rest of the Section type
          }

    You'll need to store section paths, calculate and compare checksums, and
    update objects conditionally based on the outcome of those comparisons.


Create and store embeddings
===========================

Before we can script the creation of embeddings, we need to install some
libraries that will help us.

.. code-block:: bash

    $ npm install \
        openai \
        dotenv \
        tsx \
        edgedb \
        @edgedb/generate \
        gpt-tokenizer \
        --save-dev

The ``@edgedb/generate`` package provides a set of code generation tools that
are useful when developing an EdgeDB-backed applications with TypeScript /
JavaScript. We're going to write queries using our `query builder
<https://www.edgedb.com/docs/clients/js/querybuilder>`_, but before we can, we
need to run the query builder generator.

.. code-block:: bash

    $ npx @edgedb/generate edgeql-js

This generator gives us a code-first way to write fully-typed EdgeQL queries
with TypeScript. After running the generator, you should see a new ``edgeql-js``
folder inside ``dbschema``.

Finally, we're ready to create embeddings for all sections and store them in
the database we created earlier. Let's make a ``generate-embeddings.ts`` file
inside the project root.

.. code-block:: bash

    $ touch generate-embeddings.ts

Let's write the script's skeleton and get an understanding of the flow of
tasks we need to perform.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    import dotenv from "dotenv";
    import { Configuration, OpenAIApi } from "openai";
    import { promises as fs } from "fs";
    import { join } from "path";
    import { encode } from "gpt-tokenizer";
    import * as edgedb from "edgedb";
    import e from "dbschema/edgeql-js";

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
      sectionPaths: string[],
       openai: OpenAIApi
    ): Promise<Section[]> {
      // ...
    }


    async function storeEmbeddings() {
      // ...
    }

    (async function main() {
      await storeEmbeddings();
    })();


At the top are all imports we will need throughout the file. The last import is
the query builder we generated earlier.

After the imports, we use the ``dotenv`` library to import environment
variables from the ``.env.local`` file. (In our case, that's
``OPENAI_API_KEY``, which we will need to connect to the OpenAI API).

Next, we define a ``Section`` TypeScript interface that corresponds to
the ``Section`` type we have defined in the schema.

Then we have a few function definitions:

* ``walk`` and ``prepareSectionsData`` will be called from inside
  ``storeEmbeddings``. ``walk`` returns an array of all documentation page
  paths relative to the project root. ``prepareSectionsData`` takes care of
  preparing the ``Section`` objects we will insert into the database and
  returns those as an array.

* ``storeEmbeddings`` coordinates everything.


Getting section paths
---------------------

In order to get the sections' content, we first need to know where the files
are that need to be read. The ``walk`` function finds them for us and returns
all the paths.

Since our ``docs`` folder contains files at multiple levels of nesting
(assuming you copied our example documentation files across), we need a
function that loops through all section files and directories recursively. It
will build an array of all paths relative to the project root and sort those
paths.

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
---------------------------------

This function will be responsible for collecting the data we need for each
``Section`` object we will store, including making the OpenAI API calls to
generate the embeddings. Let's walk through it one piece at a time.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    async function prepareSectionsData(
      sectionPaths: string[],
      openai: OpenAIApi
    ): Promise<Section[]> {
      const contents: string[] = [];
      const sections: Section[] = [];

      for (const path of sectionPaths) {
        const content = await fs.readFile(path, "utf8");
        // OpenAI recommends replacing newlines with spaces for best results (specific to embeddings)
        const contentTrimmed = content.replace(/\n/g, " ");
        contents.push(contentTrimmed);
        sections.push({
          content,
          tokens: 0,
          embedding: [],
        });
      }
      // The rest of the function
    }

We start with two parameters: an array of section paths and an OpenAI client.
We create a couple of empty arrays for storing information about our sections
(which will later become ``Section`` objects in the database) and their
contents. We iterate through the paths, loading each file to get its content.
In the database we save the content as is, but when calling the
embedding API, OpenAI suggests that all newlines should be replaced with a
single space for the best results. ``contentTrimmed`` is the content with
newlines replaced. We push that onto our ``contents`` array and the un-trimmed
content onto ``sections``, along with a ``0`` for the token count and an empty
array we will later replace with the actual embeddings.

Onto the next bit!

.. code-block:: typescript
    :caption: generate-embeddings.ts

    async function prepareSectionsData(
      sectionPaths: string[],
      openai: OpenAIApi
    ): Promise<Section[]> {
      // Part we just talked about

      const embeddingResponse = await openai.createEmbedding({
        model: "text-embedding-ada-002",
        input: contents,
      });

      if (embeddingResponse.status !== 200) {
        throw new Error(embeddingResponse.statusText);
      }

      // The rest
    }

Now, we generate embeddings from the content. We need to be careful about how
we approach the API calls to generate the embeddings since they could have a
big impact on how long generation takes, especially as your documentation
grows. The simplest solution would be to make a single request to the API for
each section, but in the case of EdgeDB's documentation, which has around 3,000
pages, this would take about half an hour.

Since OpenAI's embeddings API can take not only a *single* string but also an
*array* of strings, we can leverage this to batch up all our content and
generate the embeddings with a single request! You can see that single API call
when we set ``embeddingResponse`` to the result of the call to
``openai.createEmbedding``, specifying the model and passing the entire array
of contents.

After that, we handle the case where the status of the embeddings call result is
anything other than ``200`` (``OK``).

Now, it's time to put those embeddings into our section objects by iterating
through the response data.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    async function prepareSectionsData(
      sectionPaths: string[],
      openai: OpenAIApi
    ): Promise<Section[]> {
      // The stuff we already talked about

      embeddingResponse.data.data.forEach((item, i) => {
        sections[i].embedding = item.embedding;
        sections[i].tokens = encode(contents[i]).length;
      });

      return sections;
    }

One downside to this one-shot embedding generation approach is that we do not
get back token counts with the result. Token counts are important because they
determine how many relevant sections we can send along with our input to the
chat completions API — the one that answers the user's question — and still be
within the model's token limit. To stay within the limit, we need to know how
many tokens each section has. That's where the `gpt-tokenizer
<https://www.npmjs.com/package/gpt-tokenizer>`_ library comes in. OpenAI only
provides token counts for a single string. That means, we'll need to count them
ourselves.

You see this in action next as we iterate through all the embeddings we got
back, adding both the embedding and the token lengths to their respective
sections. We imported the ``encode`` function earlier from ``gpt-tokenizer``,
and we call it, passing the contents, and measure the ``length`` to get the
token counts. These two additional pieces of data make the section fully ready
to store in the database, so we can return the fully-formed sections from the
function.

Here's the entire function assembled:

.. code-block:: typescript
    :caption: generate-embeddings.ts

    async function prepareSectionsData(
      sectionPaths: string[],
      openai: OpenAIApi
    ): Promise<Section[]> {
      const contents: string[] = [];
      const sections: Section[] = [];

      for (const path of sectionPaths) {
        const content = await fs.readFile(path, "utf8");
        // OpenAI recommends replacing newlines with spaces for best results (specific to embeddings)
        const contentTrimmed = content.replace(/\n/g, " ");
        contents.push(contentTrimmed);
        sections.push({
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

.. note::

    This is not the only approach to keeping track of tokens. We could choose
    *not* to save token counts in the database and to instead count section
    tokens later on the client after we find the relevant sections.

    We have other options for counting early too. Another tool you can use to
    count tokens in advance is `tiktoken
    <https://github.com/openai/tiktoken>`_. This is OpenAI's Python tokenizer
    and would give the more accurate result over the Node alternative, but
    using ``gpt-tokenizer`` is more straightforward for our use case.

Now that we have sections ready to be stored in the database, let's tie
everything together with the ``storeEmbeddings`` function.


Storing the ``Section`` objects
-------------------------------

.. TODO: Still need to edit this section

Again, we'll break this function apart and walk through it.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    async function storeEmbeddings() {
      if (!process.env.OPENAI_API_KEY) {
        return console.log(
          "Environment variable OPENAI_API_KEY is required: skipping embeddings generation."
        );
      }

      try {
        const configuration = new Configuration({
            apiKey: process.env.OPENAI_API_KEY,
        });
        const openai = new OpenAIApi(configuration);

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

.. code-block:: typescript
    :caption: generate-embeddings.ts

    async function storeEmbeddings() {
      if (!process.env.OPENAI_API_KEY) {
        return console.log(
          "Environment variable OPENAI_API_KEY is required: skipping embeddings generation."
        );
      }

      try {
        const configuration = new Configuration({
            apiKey: process.env.OPENAI_API_KEY,
        });
        const openai = new OpenAIApi(configuration);

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


At the top, we immediately return if ``OPENAI_API_KEY`` doesn't exist. Otherwise,
we create try/catch block and write the rest of the function inside try block.
If some error is thrown while we try to regenerate embeddings and update the
database we will safely catch it in the catch block.

We create OpenAI and EdgeDB clients. We use OpenAI client to get embeddings,
and EdgeDB client to access and query the database.

Next, we get sections paths and prepare all sections data.

Before we update the database we need to delete the old data from it.
We just delete all ``Section`` objects.

Finally we bulk-insert all sections data in the database. The `TypeScript
binding <https://www.edgedb.com/docs/clients/js/index>`_ offers several options
for writing queries. We recommend using our query builder, and that's what we
have used here.


Running the script
------------------

Let's add a script to ``package.json`` that will invoke and execute
``generate-embeddings.ts``.

.. code-block:: json-diff
    :caption: package.json

      {
        "name": "docs-chatbot",
        "version": "0.1.0",
        "private": true,
        "scripts": {
          "dev": "next dev",
          "build": "next build",
          "start": "next start",
          "lint": "next lint",
    +     "embeddings": "tsx generate-embeddings.ts"
        },
        "dependencies": {
          "@types/node": "20.4.8",
          "@types/react": "18.2.18",
          "@types/react-dom": "18.2.7",
          "autoprefixer": "10.4.14",
          "eslint": "8.46.0",
          "eslint-config-next": "13.4.13",
          "next": "13.4.13",
          "postcss": "8.4.27",
          "react": "18.2.0",
          "react-dom": "18.2.0",
          "tailwindcss": "3.3.3",
          "typescript": "5.1.6"
        },
        "devDependencies": {
          "@edgedb/generate": "^0.3.3",
          "dotenv": "^16.3.1",
          "edgedb": "^1.3.4",
          "gpt-tokenizer": "^2.1.1",
          "openai": "^3.3.0",
          "tsx": "^3.12.7"
        }
      }

Now we can invoke the ``generate-embeddings.ts`` script from our terminal using
a simple command:

.. code-block:: bash

   $ npm run embeddings

After the script finishes, open the EdgeDB UI.

.. code-block:: bash

  $ egdedb ui

Switch to the Data Explorer, and you should see that the database has been
updated with the embeddings and other relevant data.


Answering user questions
========================

Now that we have the content's embeddings stored, we can start working on the
handler for user questions. The user will submit a question to our server, and
the handler will send them and answer back. We will define a route and an HTTP
request handler for this task. Thanks to the power of Next.js, we can do all of
this within our project using a `route handler`_.

.. _route handler:
  https://nextjs.org/docs/app/building-your-application/routing/route-handlers

As we write our handler, one important consideration is that answers can be
quite long. We could wait on the server side to get the whole answer from
OpenAI and then send it to the client, but that would feel slow to the user.
OpenAI supports streaming, so instead we can send answer to the client in
chunks, as they arrive to the server. With this approach, the user doesn't have
to wait for the entire response before they start getting feedback and our API
seems faster.

In order to stream responses, we will use the browser's `server-sent events
(SSE) API`_. Server-sent events enables a client to receive automatic updates
from a server via an HTTP connection, and describes how servers can initiate
data transmission towards clients once an initial client connection has been
established. The client sends a request and with that request initiates a
connection with our server. With that connection established, the server will
send data back to the client in chunks until all of the data is sent, at which
point it closes the connection.

.. lint-off

.. _server-sent events (SSE) API:
  https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events

.. lint-on


Next.js route handler
---------------------

When using `Next.js's App Router <https://nextjs.org/docs/app>`_, route
handlers should be written inside an ``app/api`` folder. Every route should
have its own folder within that, and the handlers should be defined inside a
``route.ts`` file inside the route's folder.

Let's generate a new folder for the answer generation route inside ``app/api``.

.. code-block:: bash

    $ mkdir app/api && cd app/api
    $ mkdir generate-answer && touch generate-answer/route.ts

As the final setup step, we will install the ``common-tags`` NPM package which
gives us some useful template tags to make it easier to generate HTML from our
route handler. We will use it later when we create the prompt from user's
question and related sections.

.. code-block:: bash

    $ npm install common-tags

Let's talk briefly about runtimes. In the context of Next.js, "runtime" refers
to the set of libraries, APIs, and general functionality available to your code
during execution. Next.js supports `Node.js and Edge runtimes`_. (The "Edge"
runtime is coincidentally named but is not related to EdgeDB.) Streaming is
supported within both runtimes, but the implementation is a bit simpler when
using Edge, so that's what we will use here. The Edge runtime is based on Web
APIs. It has very low latency thanks to its minimal use of resources, but the
downside is that it doesn't support native Node.js APIs.

.. lint-off

.. _Node.js and Edge runtimes:
  https://nextjs.org/docs/app/building-your-application/rendering/edge-and-nodejs-runtimes

.. lint-on

We'll start by importing the modules we will need in the handler and
writing some configuration.

.. code-block:: typescript
    :caption: app/api/generate-answer/route.ts

    import { codeBlock, oneLineTrim } from "common-tags";
    import * as edgedb from "edgedb";
    import e from "dbschema/edgeql-js";

    export const config = {
        runtime: "edge",
    };

    const openAIApiKey = process.env.OPENAI_API_KEY;

    const client = edgedb.createHttpClient({ tlsSecurity: process.env.TLS_SECURITY });

    export async function POST(req: Request) {
        …
    }

    // other functions that are called inside POST handler...


The first imports are templates from the ``common-tags`` library we installed
earlier. Then, we import the EdgeDB binding. The third import is the query
builder we described previously.

By exporting ``config``, we override Next.js configuration defaults for this
handler. In this case, we want to override the runtime for this handler so that
Next.js will use the Edge runtime instead of the default Node.js runtime.

We need to use ``createHttpClient`` to connect to the EdgeDB client. The HTTP
client defaults to using HTTPS which needs a trusted TLS/SSL certificate. Local
development instances use self signed certificates, and using HTTPS with these
certificates will result in an error. To work around this error, we use HTTP
instead by passing ``{ tlsSecurity: "insecure" }`` when creating the client.
Bear in mind that this is only for local development, and you should always use
TLS in production. Instead of hardcoding the ``tlsSecurity`` value in our code,
let's add another environment variable to ``.env.local`` file so we can easily
change this value per environment.

.. code-block:: -diff
    :caption: .env.local

      OPENAI_API_KEY="<my-openai-api-key>"
    + TLS_SECURITY = "insecure"

We're ready now to write the handler function for HTTP POST requests. To do
this in Next.js, you export a function named for the request method you want it
to handle.

Our POST handler calls other functions that we won't define just yet, but we'll
circle back to them later.

.. code-block:: typescript
    :caption: app/api/generate-answer/route.ts

    …

    export const errors = {
        flagged: `OpenAI has declined to answer your question due to their
        [usage-policies](https://openai.com/policies/usage-policies). Please try
        another question.`,
        default: "There was an error processing your request. Please try again.",
    };

    export async function POST(req: Request) {
        try {
            if (!openAIApiKey)
                throw new Error("Missing environment variable OPENAI_API_KEY");

            const { query } = await req.json();
            const sanitizedQuery = query.trim();

            const moderatedQuery = await moderateQuery(sanitizedQuery, openAIApiKey);
            if (moderatedQuery.flagged) throw new Error(errors.flagged);

            const embedding = await getEmbedding(query, openAIApiKey);

            const context = await getContext(embedding);

            const prompt = createFullPrompt(sanitizedQuery, context);

            const answer = await getOpenAiAnswer(prompt, openAIApiKey);

            return new Response(answer, {
                headers: {
                    "Content-Type": "text/event-stream",
                },
            });
        } catch (error: any) {
            console.error(error);

            const uiError = error.message || errors.default;

            return new Response(uiError, {
                status: 500,
                headers: { "Content-Type": "application/json" },
            });
        }
    }

We start by writing a couple of error message for different error cases. You
might want to get even more granular with these to give your users more
granular error information, but we'll define just two cases for simplicity.

We should make sure that we have the ``OPENAI_API_KEY`` environment variable
set before proceeding, since we can't get an answer without it. We throw if it
isn't set.

Now, our handler will run the user's question through a few different steps as
we build toward an answer.

1. We check that the query complies with the OpenAI's `usage policies
   <https://openai.com/policies/usage-policies>`_, which means that it should
   not include any hateful, harassing, or violent content. This is handled by
   our ``moderateQuery`` function.
2. If the query passes moderation, we generate embeddings for it using the
   OpenAI embedding API. This is handled by our ``getEmbedding`` function.
3. We get related documentation sections from the EdgeDB database. This is
   handled by ``getContext``.
4. We create the full prompt as our input to the chat completions API by
   combining the question, related documentation sections, and a "system
   message."

.. note::

   The system message is a general instruction to the language model that it
   should follow when answering any question.

With the input fully prepared, we call the chat completions API using the
previously generated prompt, and we stream the response we get from OpenAI
to the user. In order to use streaming we need to provide the appropriate
``content-type`` header: ``"text/event-stream"``. (You'll see that in the
options object passed to the ``Response`` constructor.)

To keep things simple, we've wrapped most of these in a single
``try``/``catch`` block. If any error occurs we send the error message to the
user with status 500. In practice, you may want to split this up and respond
with different status codes based on the outcome. For example, in the case the
moderation request returns an error, you may want to send back a ``400``
response status ("Bad Request") instead of a ``500`` ("Internal Server Error").

Now that you can see broadly what we're doing in this handler, let's dig into
each of the functions we've called in it.


Moderation request
^^^^^^^^^^^^^^^^^^

For every request to OpenAI in this handler, we will write basic fetch
requests. We can't use the ``openai`` package (the one we used in our embedding
generation script), because it uses `Axios
<https://www.npmjs.com/package/axios>`_ to make requests, and Axios is not
supported in the Edge runtime.

We could use `openai-edge <https://www.npmjs.com/package/openai-edge>`_
instead, which works perfectly on the Edge runtime and would leave us with a
little less code, but it is also useful to understand how to implement things
*without* using additional libraries. Feel free to install ``openai-edge``, and
replace our fetch calls if you prefer.

Let's write our moderation request function: ``moderateQuery``. We will use the
``https://api.openai.com/v1/moderations`` endpoint. Read about it in the
`OpenAI moderation quickstart
<https://platform.openai.com/docs/guides/moderation/quickstart>`_.

.. code-block:: typescript
    :caption: app/api/generate-answer/route.ts

    async function moderateQuery(query: string, apiKey: string) {
        const moderationResponse = await fetch(
            "https://api.openai.com/v1/moderations",
            {
            method: "POST",
            headers: {
                Authorization: `Bearer ${apiKey}`,
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                input: query,
            }),
            }
        ).then((res) => res.json());

        const [results] = moderationResponse.results;
        return results;
    }

The function is pretty straightforward: it takes the question (the ``query``
parameter) and API key, fires off a request to the API, and returns the result.
If the API finds an issue with the user's question, the response will have the
``flagged`` property set to ``true``. In that case we will throw a general
error, but you could also inspect the response to find what categories are
problematic and include more info in the error.

If the question passes moderation then we can generate the embeddings for the
question.


Embeddings generation request
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For the embeddings request, we will build another fetch request, this time to
the ``https://api.openai.com/v1/embeddings`` API endpoint, in a new function
called ``getEmbedding``.

.. code-block:: typescript
    :caption: app/api/generate-answer/route.ts

    async function getEmbedding(query: string, apiKey: string) {
        const embeddingResponse = await fetch(
            "https://api.openai.com/v1/embeddings",
            {
            method: "POST",
            headers: {
                Authorization: `Bearer ${apiKey}`,
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                model: "text-embedding-ada-002",
                input: query.replaceAll("\n", " "),
            }),
            }
        );

        if (embeddingResponse.status !== 200) {
            throw new Error(embeddingResponse.statusText);
        }

        const {
            data: [{ embedding }],
        } = await embeddingResponse.json();

        return embedding;
    }

This new function again takes the question (as ``query``) and the API key. We
build a fetch request to the appropriate endpoint, specifying the model to use
for generation (the ``model`` property of the request's body).

If the response status is not 200 ("OK"), we will throw an error. If we get the
embeddings *without* an error we can query our embeddings database for related
documentation sections.


Get related documentation sections request
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's first write the database query that will give us back the related
sections in a variable named ``getSectionsQuery``.

.. code-block:: typescript
    :caption: app/api/generate-answer/route.ts

    const getSectionsQuery = e.params(
        {
            target: e.OpenAIEmbedding,
            matchThreshold: e.float64,
            matchCount: e.int16,
            minContentLength: e.int16,
        },
        (params) => {
            return e.select(e.Section, (section) => {
            const dist = e.ext.pgvector.cosine_distance(
                section.embedding,
                params.target
            );
            return {
                content: true,
                tokens: true,
                dist,
                filter: e.op(
                    e.op(e.len(section.content), ">", params.minContentLength),
                    "and",
                    e.op(dist, "<", params.matchThreshold)
                ),
                order_by: {
                    expression: dist,
                    empty: e.EMPTY_LAST,
                },
                limit: params.matchCount,
            };
            });
        }
    );

In the above code we use EdgeDB's TypeScript query builder to create a query.
The query takes a few parameters:

* ``target``: Embedding array to compare against to find related sections. In
  this case, these will be the questions's embeddings we just generated.
* ``matchThreshold``: Similarity threshold. Only matches with a similarity
  score below this threshold will be returned. This will be a number between
  ``0.0`` and ``1.0``. Values closer to ``0.0`` mean the documentation sections
  must be very similar to the question while values closer to ``1.0`` allow for
  more variance.
* ``matchCount``: Maximum number of sections to return
* ``minContentLength``: Minimum number of characters the sections should have in
  order to be considered

We write a select query by calling ``e.select`` and passing it the type we want
to select (``e.Section``). We return from that function an object representing
the shape we want back plus any other clauses we need: in this case, a filter,
ordering, and limit clause.

We use the ``cosine_distance`` function to calculate the similarity between the
user's question and our documentation sections. We have access to this function
through EdgeDB's pgvector extension. We then filter on that property by
comparing it to the ``matchThreshold`` value we will pass when executing the
query.

We want to get back the content and number of tokens for every related section
that passes the filter clause (i.e., has more than ``minContentLength`` tokens,
and the distance from the question embedding is less than our
``matchThreshold``). We want to order results in ascending order (which is the
default) by how related they are to the question (represented as ``dist``) and
to get back, at most, ``matchCount`` sections.

We've written the query, but it won't help us until we execute it. We'll do
that in the ``getContext`` function.

.. code-block:: typescript
    :caption: app/api/generate-answer/route.ts

    async function getContext(embedding: number[]) {
        const sections = await getSectionsQuery.run(client, {
            target: embedding,
            matchThreshold: 0.3,
            matchCount: 8,
            minContentLength: 20,
        });

        let tokenCount = 0;
        let context = "";

        for (let i = 0; i < sections.length; i++) {
            const section = sections[i];
            const content = section.content;
            tokenCount += section.tokens;

            if (tokenCount >= 1500) {
                tokenCount -= section.tokens;
                break;
            }

            context += `${content.trim()}\n---\n`;
        }

        return context;
    }

This function takes the embeddings of the question (the ``embedding``
parameter) and returns the related documentation sections.

We start by running the query and passing in some values for the parameters:

- the question embeddings that were passed to the function
- a ``matchThreshold`` value of ``0.3``. You can tinker with this if you don't
  like the results.
- a ``matchCount``. We've chosen ``8`` here which represents the most sections
  we'll get back.
- a ``minContentLength`` of 20 tokens

We then iterate through the sections that came back to prepare them to send on
to the chat completions API. This involves incrementing the token count for the
current section, making sure the overall token count doesn't exceed our maximum
of 1,500 for the context (to stay under the LLM's token limit), and, if the
token count isn't exceeded, adding the trimmed content of this section to
``context`` which we will ultimately return. Since we ordered this query by
``dist`` ascending, and since lower ``dist`` values mean more similar sections,
we will be sure to get the most similar sections before we hit our token limit.

With our context ready, it's time to get our user their answer.


Chat completions request
^^^^^^^^^^^^^^^^^^^^^^^^

Before we make our completion request, we will build the full input which
consists of the user's question, the related documentation, and the system
message. The system message should tell the language model what tone to use
when answering question and some general instructions on what is expected from
it. With that you can give it some personality that it will bake into every
response. We'll combine all of these parts in a function called
``createFullPrompt``.

.. code-block:: typescript
    :caption: app/api/generate-answer/route.ts

    function createFullPrompt(query: string, context: string) {
        const systemMessage = `
            As an enthusiastic EdgeDB expert keen to assist, respond to questions in
            markdown, referencing the given EdgeDB sections.

            If unable to help based on documentation, respond with:
            "Sorry, I don't know how to help with that."`;

        return codeBlock`
            ${oneLineTrim`${systemMessage}`}

            EdgeDB sections: """
            ${context}
            """

            Question: """
            ${query}
            """`;
    }

This function takes the question (as ``query``) and the related documentation
(as ``context``), combines them with a system message, and formats it all
nicely for easy consumption by the chat completions API.

We'll use the returned prompt from that function, along with the OpenAI API
key, as arguments to a new function (``getOpenAiAnswer``) that will get the
answer from the OpenAI and return it.

.. code-block:: typescript
    :caption: app/api/generate-answer/route.ts

    async function getOpenAiAnswer(prompt: string, apiKey: string) {
        const completionRequestObject = {
            model: "gpt-3.5-turbo",
            messages: [{ role: "user", content: prompt }],
            max_tokens: 1024,
            temperature: 0.1,
            stream: true,
        };

        const response = await fetch("https://api.openai.com/v1/chat/completions", {
            method: "POST",
            headers: {
            Authorization: `Bearer ${apiKey}`,
            "Content-Type": "application/json",
            },
            body: JSON.stringify(completionRequestObject),
        });

        return response.body;
    }

We start by building up an object to send through on the body of our request.
We need to provide these parameters inside the request body:

* ``model``: The language model we want the chat completions API to use when
  answering the question. (You can alternatively use ``gpt-4`` to if you have
  access to it.)

* ``messages``: We send the prompt as part of the messages property. It is
  possible to send the system message on the first object of the array, with
  ``role: system``, but since we also have the context sections as part of the
  input, we will just send everything with the role ``user``.

* ``max_tokens``: Maximum number of tokens to use for the answer.

* ``temperature``: Number between 0 and 2. From `OpenAI's description of
  temperature`_: "Higher values like 0.8 will make the output more random,
  while lower values like 0.2 will make it more focused and deterministic."

* ``stream``: Setting this to ``true`` will have the API stream the response

.. lint-off

.. _OpenAI's description of temperature:
  https://platform.openai.com/docs/api-reference/chat/create#chat/create-temperature

.. lint-on

With the route in place, let's update the UI and connect everything together.

Building the UI
===============

To make things as simple as possible, we will just update the ``Home``
component that's inside ``app/page.tsx`` file. By default all components inside
the App Router are server components, but we want to have client-side
interactivity and dynamic updates. In order to do that we have to use a client
component for our ``Home`` component. The way to accomplish that is to convert
the ``page.tsx`` file to use the client component. We do that by adding the
``use client`` directive to the top of the file.

.. code-block:: typescript
    :caption: app/page.tsx

    "use client";

Feel free to copy/paste the following HTML with Tailwind classes in order to
have a ready-made UI, or you can write your own from scratch.

.. code-block:: typescript
    :caption: app/page.tsx

    import { useState } from "react";

    export default function Home() {
        const [prompt, setPrompt] = useState("");
        const [question, setQuestion] = useState("");
        const [answer, setAnswer] = useState<string>("");
        const [isLoading, setIsLoading] = useState(false);
        const [error, setError] = useState<string | undefined>(undefined);

        const handleSubmit = () => {};

        return (
        <main className="w-screen h-screen flex items-center justify-center bg-[#2e2e2e]">
            <form className="bg-[#2e2e2e] w-[540px] relative">
            <input
                className={`py-5 pl-6 pr-[40px] rounded-md bg-[#1f1f1f] w-full
                outline-[#1f1f1f] focus:outline outline-offset-2 text-[#b3b3b3]
                mb-8 placeholder-[#4d4d4d]`}
                placeholder="Ask a question..."
                value={prompt}
                onChange={(e) => {
                setPrompt(e.target.value);
                }}
            ></input>
            <button
                onClick={handleSubmit}
                className="absolute top-[25px] right-4"
                disabled={!prompt}
            >
                <ReturnIcon
                className={`${!prompt ? "fill-[#4d4d4d]" : "fill-[#1b9873]"}`}
                />
            </button>
            <div className="h-96 px-6">
                {question && (
                <p className="text-[#b3b3b3] pb-4 mb-8 border-b border-[#525252] ">
                    {question}
                </p>
                )}
                {(isLoading && <LoadingDots />) ||
                (error && <p className="text-[#b3b3b3]">{error}</p>) ||
                (answer && <p className="text-[#b3b3b3]">{answer}</p>)}
            </div>
            </form>
        </main>
        );
    }

    function ReturnIcon({ className }: { className?: string }) {
        return (
            <svg
                width="20"
                height="12"
                viewBox="0 0 20 12"
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                className={className}
            >
                <path
                fillRule="evenodd"
                clipRule="evenodd"
                d={`M12 0C11.4477 0 11 0.447715 11 1C11 1.55228 11.4477 2 12
                2H17C17.5523 2 18 2.44771 18 3V6C18 6.55229 17.5523 7 17
                7H3.41436L4.70726 5.70711C5.09778 5.31658 5.09778 4.68342 4.70726
                4.29289C4.31673 3.90237 3.68357 3.90237 3.29304 4.29289L0.306297
                7.27964L0.292893 7.2928C0.18663 7.39906 0.109281 7.52329 0.0608469
                7.65571C0.0214847 7.76305 0 7.87902 0 8C0 8.23166 0.078771 8.44492
                0.210989 8.61445C0.23874 8.65004 0.268845 8.68369 0.30107
                8.71519L3.29289 11.707C3.68342 12.0975 4.31658 12.0975 4.70711
                11.707C5.09763 11.3165 5.09763 10.6833 4.70711 10.2928L3.41431
                9H17C18.6568 9 20 7.65685 20 6V3C20 1.34315 18.6568 0 17 0H12Z`}
                />
            </svg>
        );
    }

    function LoadingDots() {
        return (
            <div className="grid gap-2">
                <div className="flex items-center space-x-2 animate-pulse">
                <div className="w-1 h-1 bg-[#b3b3b3] rounded-full"></div>
                <div className="w-1 h-1 bg-[#b3b3b3] rounded-full"></div>
                <div className="w-1 h-1 bg-[#b3b3b3] rounded-full"></div>
                </div>
            </div>
        );
    }

We have created an input field where the user can enter a question. When they
submit, we show a loading indicator while we wait for the first answer chunk
from OpenAI. When the first chunk arrives, we start streaming the answer to the
user. In case of an error, we show an error message to the user.

The text the user types in the input field is captured as ``prompt``.
``question`` is the submitted prompt that we show under the input when user
submits their question. We clear the input and delete the prompt when user
submits it, but keep the ``question`` value so the user can reference it.

Let's write the form submission handler function.

.. code-block:: typescript
    :caption: app/page.tsx

    const handleSubmit = (
      e: KeyboardEvent | React.MouseEvent<HTMLButtonElement>
    ) => {
      e.preventDefault();

      setIsLoading(true);
      setQuestion(prompt);
      setAnswer(""");
      setPrompt("");
      generateAnswer(prompt);
    };

When the user submits a question, we set the ``isLoading`` state to ``true``
and show the loading indicator. We clear the prompt state and set the question
state. We also clear the answer state because the answer may hold an answer to
a previous question, but we want to start with an empty answer.

At this point we want to create a server-sent event and send a request to our
``api/generate-answer`` route. We will do this inside the ``generateAnswer``
function.

The browser-native SSE API doesn't allow the client to send a payload to the
server; the client is only able to open a connection to the server to begin
receiving events from it via a GET request. In order for the client to be able
to send a payload via a POST request to open the SSE connection, we will use
the `sse.js <https://npm.io/package/sse.js>`_ package, so let's install it.

.. code-block:: bash

    $ npm install sse.js --save

This package doesn't have a corresponding types package, so we need to add them
manually. Let's create a new folder named ``types`` in the project root and
an ``sse.d.ts`` file inside it.

.. code-block:: bash

    $ mkdir types && touch types/sse.d.ts

Open ``sse.d.ts`` and add this code:

.. code-block:: typescript
    :caption: types/sse.d.ts

    type SSEOptions = EventSourceInit & {
        payload?: string;
    };

    declare module "sse.js" {
        class SSE extends EventSource {
            constructor(url: string | URL, sseOptions?: SSEOptions);
            stream(): void;
        }
    }

This extends the native ``EventStream`` by adding a payload to the constructor.
We also added the ``stream`` function to it which is used to activate the
stream in the sse.js library.

Now, we can import ``SSE`` in ``page.tsx`` and use it to open a connection to
our handler route while also sending the user's query.

.. code-block:: typescript-diff
    :caption: app/page.tsx

      "use client";

      import { useState, useRef } from "react";
    + import { SSE } from "sse.js";

      export default function Home() {
          const eventSourceRef = useRef<SSE>();

          ...

          const generateAnswer = async (query: string) => {
              if (eventSourceRef.current) eventSourceRef.current.close();

              const eventSource = new SSE(`api/generate-answer`, {
                  payload: JSON.stringify({ query }),
              });
              eventSourceRef.current = eventSource;

              eventSource.onerror = handleError;
              eventSource.onmessage = handleMessage;
              eventSource.stream();
          };

          handleError() { ... }
          handleMessage() { ... }
      ...

Note that we save a reference to the ``eventSource`` object. We need this in
case a user submits a new question while answer to the previous one is still
assembling on the client. If we don't close the existing connection to the
server before opening the new one, this could cause problems since two
connections will be open and trying to receive data.

We opened a connection to the server, and we are now ready to receive events
from it. We just need to write handlers for those events so the UI knows what
to do with them. We will get the answer as part of a message event, and if an
error is returned, the server will send an error event to the client.

Let's write these handlers.

.. code-block:: typescript
    :caption: app/page.tsx

    import { errors } from "./api/generate-answer/route";
    …

    function handleError(err: any) {
        setIsLoading(false);

        const errMessage =
        err.data === errors.flagged ? errors.flagged : errors.default;

        setError(errMessage);
    }


    function handleMessage(e: MessageEvent<any>) {
        try {
            setIsLoading(false);
            if (e.data === "[DONE]") return;

            const chunkResponse = JSON.parse(e.data);
            const chunk = chunkResponse.choices[0].delta?.content || "";
            setAnswer((answer) => answer + chunk);
        } catch (err) {
            handleError(err);
        }
    }

When we get the message event, we extract the data from it and add it to the
``answer`` state until we receive all chunks. This is indicated when the data
is equal to ``[DONE]``, meaning the whole answer has been received and the
connection to the server will be closed. There is no data to be parsed in this
case, so we return instead of trying to parse it. (An error will be thrown if
we try to parse it in this case.)

With that, the UI can now get answers from the Next.js route. The build is
complete, and it's time to try it out!


Testing it out
==============

You should now be able to run the project with ``npm run dev`` to test it. If
you used our example documentation, the chatbot will know a few things about
EdgeDB along with whatever it was trained on (which would have been relatively
early in EdgeDB's development as of the time of this tutorial's publication).

Some questions you might try:

- "What is EdgeDB?"
- "How do I introspect a function in EdgeDB?"
- "Who is EdgeDB for?"
- "How should I get started with EdgeDB?"

If you don't like the responses you're getting, here are a few things you might
try tweaking:

- ``systemMessage`` in the ``createFullPrompt`` function in
  ``app/api/generate-answer/route.ts``
- ``temperature`` in the ``getOpenAiAnswer`` in
  ``app/api/generate-answer/route.ts``
- the ``matchThreshold`` value passed to the query from the ``getContext``
  function in ``app/api/generate-answer/route.ts``

You can see the finished source code for this build in `our examples repo on
GitHub
<https://github.com/edgedb/edgedb-examples/tree/main/docs-chatbot/docs>`_. You
might also find our actual implementation interesting. You'll find it in `our
website repo <https://github.com/edgedb/website>`_. Pay close attention to the
contents of `buildTools/gpt
<https://github.com/edgedb/website/tree/main/buildTools/gpt>`_, where the
embedding generation happens and `components/gpt
<https://github.com/edgedb/website/tree/main/components/gpt>`_, which contains
most of the UI for our chatbot.

If you have trouble with the build or just want to hang out with other EdgeDB
users, please join `our awesome community on Discord
<https://discord.gg/umUueND6ag>`_!
