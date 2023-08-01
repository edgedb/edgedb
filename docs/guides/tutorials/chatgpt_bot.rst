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
--------------------------------------------------

tl;dr- Training a language model is hard, but using embeddings to give it
access to information beyond what it's trained on is easy… so we will do that!
Now, `get started building <ref_guide_chatgpt_bot_start>`_!

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
^^^^^^^^^^^^^^^^^^^^^^^

General implementation has these steps (we'll stick to them too in the guide):

1. convert documentation into a unified format that is easily digestible
   by the OpenAI API
2. split the converted documentation into sections that can fit into the GPT
   context window
3. create embeddings for each section using `OpenAI's embeddings API
   <https://platform.openai.com/docs/guides/embeddings>`_
4. store the embeddings data in an vector database


Each time a user asks a question:

1. query the database for the documentation sections most relevant to
   the question using a similarity function
2. inject these sections as a context into the prompt together with user
   question and submit a request to the OpenAI
3. stream the OpenAI response back to the user in realtime

.. _ref_guide_chatgpt_bot_start:

Let's get started
-----------------

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
EdgeDB instance to store section contents and embeddings.

Get an OpenAI API key
^^^^^^^^^^^^^^^^^^^^^
1. Log in or sign up to the `OpenAI platform
   <https://platform.openai.com/account/api-keys>`_.
2. Create new `secret key <https://platform.openai.com/account/api-keys>`_.
3. Create a ``.env.local`` file in the root of your project and copy your key
   here in the following format: ``OPENAI_KEY="<my-openai-key>"``.


Install EdgeDB CLI and create a local EdgeDB instance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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

The CLI should have set up an EdgeDB project, and instance, and a database
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
-------------------------------------------
For this project, we will be using Markdown files since they are straightforward
for OpenAI's language models to use.


.. note::

    You *can* opt to use more complex formats like HTML, but since they include
    additional data beyond what you want the language model to consume (like
    HTML's tags and their attributes), you should first clean those files and
    extract the content before sending it to OpenAI. It's possible to use more
    complex formats *without* doing this, but then you're paying for extra
    tokens that don't improve the answers your chatbot will give users.

Create a ``docs`` folder in the root of your project. We have provided some
Markdown files for this tutorial, but you can replace them with your own. Place
those files in the ``docs`` folder.

.. TODO: Where are these files and how should the user get them?
.. TODO: Devon pls include parts about text files. Files are inside docs folder, the section you deleted : )
.. Devon got this far

Split the documentation into sections
-------------------------------------
Our files are already short enough ..todo explain token limits

Create embeddings and store them in the EdgeDB database
-------------------------------------------------------
Finally, we're ready to create embeddings for all sections and store them in
the database we've created earlier. Let's make ``gpt`` folder in the project's
root and ``generate-embeddings.ts`` file inside it, all code related to
embeddings generation will be inside this folder. And ``generate-embeddings.ts``
is the main script we will run every time we want to re-generate embeddings.

.. code-block:: bash

    $ mkdir gpt && touch gpt/generate-embeddings.ts

Schema
^^^^^^
To be able to store data in the DB we have to create the schema first. We
want to make it as simple as possible and store only the relevant data. We
need to store the section content and embeddings. We will also save
each section's relative path and content checksum. The checksum will allow
us to easily determine which files of the documentation has changed every
time we run the embeddings generation script. This way, we can re-generate
embeddings and write to the database only for those changed sections. We will
also need to save the number of tokens for every section. We will need this
later when calculating how many similar sections fit inside the prompt context.

Open the empty schema file generated when you initialized the EdgeDB project
``dbschema/default.esdl`` and add this code to it:

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
        required checksum: str;
        required tokens: int16;
        required embedding: OpenAIEmbedding;

        index ext::pgvector::ivfflat_cosine(lists := 3)
          on (.embedding);
      }
    }

We are able to store embeddings and search similar embeddings in the EdgeDB
database because of the ``pgvector`` extension. In order to use it in your
schema you have to activate the ``ext::pgvector`` module with ``using extension
pgvector`` at the beginning of the schema file. This module gives you access to
``ext::pgvector::vector`` as well as few similarity functions and indexes you
can use later to retrieve embeddings. Read our `pgvector documentation
<https://www.edgedb.com/docs/stdlib/pgvector>`_ for more details on the extension.

With the extension active, you may now add vector properties when defining
your type. However, in order to be able to use indexes, the vectors in
question need to be a of a fixed length. This can be achieved by creating
a custom scalar extending the vector and specifying the desired length.
OpenAI embeddings have length of 1,536, so that's what we use in our schema.

There is also index inside the Section type. In order to speed up queries, we
add the index that corresponds to the ``cosine_similarity`` function which is
``ivfflat_cosine``. We are using the value ``3`` for the ``lists`` parameter
because best practice is to use the number of objects divided by 1,000 for up
to 1,000,000 entries. Our database will have around 3,000 total entries which
falls well under that threshold. In our case indexing does not have much
impact, but if you plan to store and query huge amount ofentries, an index is
recommended.

We apply this schema by creating and running a migration.

.. code-block:: bash

    $ edgedb migration create
    $ edgedb migrate

Install required dependencies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's install few NPM dependencies our script will need.

.. code-block:: bash

    $ npm install openai gpt-tokenizer --save
    $ npm install dotenv tsx --save-dev

We'll kick off this script by opening the new file and importing all those
dependencies and the other modules we need.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    import { Configuration, OpenAIApi } from "openai";
    import dotenv from "dotenv";
    import { promises as fs } from "fs";
    import { inspect } from "util";
    import { join } from "path";
    import getTokensLen from "./getTokensLen";
    import * as edgedb from "edgedb";
    import e from "../dbschema/edgeql-js";

Next, we use the ``dotenv`` library to import the ``OPENAI_KEY`` we created
earlier in the ``.env.local`` file. We'll use this later to authenticate with
the API so we can make calls against it. If the user of our script hasn't set
it, we won't be able to generate the embeddings, so we can go ahead and throw
an exception.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    //…
    dotenv.config({ path: ".env.local" });
    if (!process.env.OPENAI_KEY) {
      throw new Error(
        "Environment variable OPENAI_KEY is required: skipping embeddings generation."
        );
      }

Then we need to define a ``Section`` TypeScript interface that corresponds to
the ``Section`` type we have defined in schema.

.. TODO: Would it be better to generate this with the interfaces generator and
   import it? It would allow us to show off the generator and would also
   slightly reduce the amount of code here.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    //…
    interface Section {
      id?: string;
      path: string;
      tokens: number;
      content: string;
      embedding: number[];
    }


We need to store the paths of documentation in the database. Since our ``docs``
folder contains sections at multiple levels of nesting, we need a function that
loops through all section files, builds an array of all paths relative to the
project root, and sorts those paths. This is what the ``walk`` function does.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    //…
    type WalkEntry = {
      path: string;
    };

    async function walk(dir: string): Promise<WalkEntry[]> {
      const immediateFiles = await fs.readdir(dir);

      const recursiveFiles: { path: string }[][] = await Promise.all(
        immediateFiles.map(async (file: any) => {
          const path = join(dir, file);
          const stats = await fs.stat(path);
          if (stats.isDirectory()) return walk(path);
          else if (stats.isFile()) return [{ path }];
          else return [];
        })
      );

      const flattenedFiles: { path: string }[] = recursiveFiles.reduce(
        (all, folderContents) => all.concat(folderContents),
        []
      );

      return flattenedFiles.sort((a, b) => a.path.localeCompare(b.path));
    }

The output it produces looks like this:

.. code-block:: typescript

    [
      // ...
      {path: ".docs/gpt/cli/edgedb_describe/edgedb_describe_schema2.md"},
      {path: ".docs/gpt/cli/edgedb_describe/index.md"},
      {path: ".docs/gpt/cli/edgedb_dump.md"},
      {path: ".docs/gpt/cli/edgedb_info.md"},
      {path: ".docs/gpt/cli/edgedb_instance/edgedb_instance_create.md"},
      // ...
    ];

With this list of files and their paths, we're ready to read in the contents so
we can generate the embeddings. We'll write a class to handle this for us. This
``EmbeddingSource`` class's constructor takes the relative section path. We can
call its ``load`` method to get the contents of the file from the file system.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    //…
    class EmbeddingSource {
      content?: string;

      constructor(public filePath: string) { }

      async load() {
        const content = await fs.readFile(this.filePath, "utf8");
        this.content = content;
        return content;
      }
    }


With all this setup out of the way, it's time to generate the actual
embeddings. We'll write a function called ``generateEmbeddings`` to take care
of this for us. It will fetch embeddings from OpenAI and store them inside our
EdgeDB database.

.. code-block:: typescript
    :caption: generate-embeddings.ts

    //…
    async function generateEmbeddings() {
      const args = process.argv.slice(2);


      const configuration = new Configuration({
        apiKey: process.env.OPENAI_KEY,
      });

      const openai = new OpenAIApi(configuration);

      const client = edgedb.createClient();

      const embeddingSources: EmbeddingSource[] = [
        ...(await walk("docs")).map((entry) => new EmbeddingSource(entry.path)),
      ];

      console.log(`Discovered ${embeddingSources.length} pages`);

      console.log("Re-generating pages.");

      try {
        // Delete old data from the DB.
        await e
          .delete(e.Section, (section) => ({
            filter: e.op(section.tokens, ">=", 0),
          }))
          .run(client);

        const contents: string[] = [];
        const sections: Section[] = [];

        for (const embeddingSource of embeddingSources) {
          const { path } = embeddingSource;
          const content = await embeddingSource.load();
          // OpenAI recommends replacing newlines with spaces for
          // best results when generating embeddings
          const contentTrimmed = content.replace(/\n/g, " ");
          contents.push(contentTrimmed);
          sections.push({ path, content, tokens: 0, embedding: [] });
        }

        const tokens = await getTokensLen(contents);

        const embeddingResponse = await openai.createEmbedding({
          model: "text-embedding-ada-002",
          input: contents,
        });

        if (embeddingResponse.status !== 200) {
          throw new Error(inspect(embeddingResponse.data, false, 2));
        }

        embeddingResponse.data.data.forEach((item, i) => {
          sections[i].embedding = item.embedding;
          sections[i].tokens = tokens[i];
        });

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
        console.error("Error while trying to regenerate embeddings.", err);
      }
      console.log("Embedding generation complete");
    }

    async function main() {
      await generateEmbeddings();
    }

    main().catch((err) =>
      console.error("Error has ocurred while generating embeddings.", err)
    );


- Let's add at this point additional script to ``package.json`` that we will
  use to call the embeddings generation script.

  .. code-block:: typescript

    "embeddings": "cross-env tsx gpt/generate-embeddings.ts"

  We also need to install ``cross-env`` npm package.

  .. code-block:: bash

    $ yarn add cross-env -D

  So now we can invoke the ``generate-embeddings.ts`` script from our terminal
  using ``yarn embeddings`` command. The idea is that when we invoke script
  like this we should just re-generate embeddings for sections that have been
  changed in the meantime. When we want to generate embeddings for all sections
  (first time run when our database is empty or whenever later if we decide we
  want to wipe the database and fill it again from scratch) we should apply
  additional ``--refresh`` argument, so the commands is
  ``yarn embeddings --refresh``.

- If for some reason ``OPENAI_KEY`` is not available we should throw an error
  right at the beginning.

- Otherwise we connect to the OpenAI with the key.

- And also create EdgeDB client that we will use later to access and query
  the database.

- We walk through all the docs files and create ``embeddingSources`` array.

- **Typescript Query Builder**
  Before we continue lets understand how can we query the EdgeDB database.
  The `TS binding <https://www.edgedb.com/docs/clients/js/index>`_ offers
  several options for writing queries. We (EdgeDB) recommend using our query
  builder, and that's what we use here.

  In order to be able to use query builder we need to install generators package.

  .. code-block:: bash

    $ yarn add @edgedb/generate -D

  The ``@edgedb/generate`` package provides a set of code generation tools
  that are useful when developing an EdgeDB-backed applications with
  TypeScript / JavaScript. We need to run a `query builder <https://www.edgedb.com/docs/clients/js/querybuilder>`_
  generator.

  .. code-block:: bash

    $ yarn run -B generate edgeql-js

  This generator gives us a code-first way to write fully-typed EdgeQL
  queries with TypeScript. The ``edgeql-js`` folder should have been created
  inside ``dbschema`` folder.

- **Re-generate all embeddings from scratch:**

  - Firstly we should wipe the database if there are old entries. We have to
    find a filter that will mark all entries. One way is to filter elements
    whose tokens property is ``>=0`` which is true for all elements. EdgeDB
    doesn't provide a simpler way to wipe all elements while not deleting the
    database too.

  - We already discussed that we want to paralellize things, so instead of
    generating embeddings and updating database per section we will create a
    ``const sections: Section[]`` array that we will update with all required
    data and insert the whole array in one go to the database.

  - We also create empty ``contents`` array and loop through ``embeddingSources``
    we have created earlier in order to fill contents and sections arrays with
    path, checksum and content.

  - Next, we get embeddings for the whole contents array using the OpenAI
    embeddings API and ``text-embedding-ada-002`` language model which is
    recommended by them for embeddings.

  - We also get all tokens with ``const tokens = await getTokensLen(contents);``.
    I'll explain getTokensLen function shortly, it gives us back the array of
    tokens numbers for the whole contents array.

  - We update sections with tokens and embeddings and we can finally insert them
    into the database. We perform `bulk-insert <https://www.edgedb.com/docs/edgeql/insert>`_
    with the query builder.

    Here is the side-by-side implementation of the bulk-insert from the code with
    TS query builder and raw edgeql:

    .. tabs::

        .. code-tab:: edgeql
            :caption: edgeql

            with
              sections := json_array_unpack(<json>$sections)

              for section in sections union (
                insert Section {
                  path := <str>section['path'],
                  content:= <str>section['content'],
                  tokens:= <int16>section['tokens'],
                  embedding:= <OpenAIEmbedding>section['embedding'],
                }
              )

        .. code-tab:: typescript
            :caption: TS query builder

            const query = e.params({sections: e.json}, ({sections}) => {
              return e.for(e.json_array_unpack(sections), (section) => {
                return e.insert(e.Section, {
                  path: e.cast(e.str, section.path),
                  content: e.cast(e.str, section.content),
                  tokens: e.cast(e.int16, section.tokens),
                  embedding: e.cast(e.OpenAIEmbedding, section.embedding),
                });
              });
            });

        await query.run(client, {sections});

Why we need to know number of tokens per section
------------------------------------------------
Later when we want to answer to the user's question, we will need to send
similar sections as a context to the OpenAI completions endpoint, and we
need to know how many tokens each content has in order to stay under the
model's token limit.

OpenAI's token limit
^^^^^^^^^^^^^^^^^^^^
OpenAI's language models, like GPT-4, work by processing and generating text
in chunks referred to as "tokens." These tokens can be as short as one
character or as long as one word in English, or even other lengths in
different languages.

There are two main reasons for having a token limit:

1. **Computational Efficiency**: Processing large amounts of text requires
   significant computational resources. With each additional token, the model
   has to keep track of more information and make more complex calculations.
   Therefore, having a token limit helps to manage these computational
   requirements and ensure that the model can operate effectively and efficiently.

2. **Memory Constraints**: The models use a technique called "attention" to
   consider the context in which each token appears. This context includes a
   certain number of preceding tokens. If the number of tokens exceeds the
   model's limit, it might lose context for some tokens, which could
   negatively impact the quality of the generated text.

So in general, for the things to work, there is token limit per request which
includes both the prompt and the answer. As part of the prompt we will send
user's question and similar sections as context and we have to make sure to
not send too many sections as context because we will either get error back
or the answer can be cut off if there are few tokens left for the answer.
We will use in this tutorial GPT-4 and its token limit is 8192.

How to calculate number of tokens per section
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
There are at least 3 ways to solve this:

- when you send one string to the OpenAI embedding endpoint you will get back
  together with the embedding array also the **prompt_tokens** field telling
  you how many tokens the submitted content has and then you can store this
  in the database together with other data
- second way is to use some npm library that generates tokens array for the
  string you provide, and then you calculate the length of that array
  (`gpt-tokenizer <https://www.npmjs.com/package/gpt-tokenizer>`_ for example)
- the 3rd way is to use OpenAI `tiktoken <https://github.com/openai/tiktoken>`_
  library which should be faster than npm alternatives (and probably better
  maintained), but it's supposed to be used with python so we need to write a
  python script in order to calculate tokens in this way.

We can't go with the first approach because prompt_tokens field is received
inside embeddings response only when one string is submitted, if array of
strings is submitted you only get back the total_tokens number for the whole
submitted array.

We want to save tokens in the database so that we can retrieve them together
with contents when we get similar sections later for the user's request.
Another approach is to calculate tokens for every similar section every time
we need to construct the prompt, but this is probably a bit slower.

We use in the tutorial native OpenAI `tiktoken <https://github.com/openai/tiktoken>`_
tool. You can also use `gpt-tokeniser <https://www.npmjs.com/package/gpt-tokenizer>`_
. Using npm-library is also easier if you are not familiar with python at all.

Using tiktoken tokeniser to generate and count tokens
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Since tiktoken is a python library we need to spawn child_process and use
tiktoken in appropriate python script to calculate tokens for every section.

Firstly we need to install tiktoken:

.. code-block:: bash

  $ pip install tiktoken

Next, we need to create two new files in our gpt folder.

.. code-block:: bash

  $ touch gpt/getTokensLen.py
  $ touch gpt/getTokensLen.ts

Below is the TS script that's responsible for spawning the child, providing
sections as the input to stdin and reading response from stdout.

.. code-block:: typescript

  import path from "path";
  import {spawn} from "child_process";
  import {pythonCmd} from "@edgedb/site-build-tools/utils";

  export default async function getTokensLen(
    sections: string[]
  ): Promise<number[]> {
    const process = spawn(pythonCmd(), [
      path.join(__dirname, "getTokensLen.py"),
    ]);

    let stderr = "";

    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (data) => {
      stderr += data;
    });

    process.stdin.write(JSON.stringify(sections));
    process.stdin.write("\n");

    return new Promise((resolve, reject) => {
      let tokens: string = "";

      process.stdout.on("data", (data) => {
        tokens += data.toString();
      });

      process.on("close", (code) => {
        if (code !== 0) {
          reject(stderr);
        } else {
          resolve(JSON.parse(tokens));
        }
      });

      process.on("error", reject);
    });
  }

And here is the python script that uses tiktoken, calculates tokens for every
section and prints result to the stdout.

.. code-block:: python

  import tiktoken
  import json
  import sys

  encoding = tiktoken.encoding_for_model("text-embedding-ada-002")

  sections = None

  for line in sys.stdin:
      line = line.rstrip()
      sections = json.loads(line)
      break

  num_tokens = []

  for section in sections:
      tokens = len(encoding.encode(section))
      num_tokens.append(tokens)

  print(num_tokens)


Graphical representation of the inserted data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's run embeddings script from the terminal with:

.. code-block:: bash

  $ yarn embeddings --refresh

After the script is done (should be less than  a min), we should be able to
open UI with

.. code-block:: bash

  $ egdedb ui

and see that the DB is indeed updated with embeddings and other relevant data.


Handler function for user's questions
-------------------------------------
