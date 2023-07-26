.. \_ref_guide_chatgpt_bot:

=======
ChatGPT
=======

:edb-alt-title: Build your own docs chatbot with ChatGPT and EdgeDB

*For additional context, check out* `our blog post about why and how we used
ChatGPT via embeddings <https://www.edgedb.com/blog/chit-chatting-with-edgedb-docs-via-chatgpt-and-pgvector>`_
*to create our* `“Ask AI”  <https://www.edgedb.com/blog/chit-chatting-with-edgedb-docs-via-chatgpt-and-pgvector>`_
*bot which answers questions related to the EdgeDB docs.*

In this tutorial we're going to build a documentation chatbot with
`Next.js <https://nextjs.org/>`_, `OpenAI <https://openai.com/>`_ and EdgeDB.

Here is how the final result looks like: // todo provide video.

Before we start let's understand how it all works
-------------------------------------------------

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
3. create embeddings for each section using `OpenAI's embeddings API <https://platform.openai.com/docs/guides/embeddings>`_
4. store the embeddings data in an vector database

Each time a user asks a question:

1. query the database for the documentation sections most relevant to
   the question using a similarity function
2. inject these sections as a context into the prompt together with user
   question and submit a request to the OpenAI
3. stream the OpenAI response back to the user in realtime


Let's get started
-----------------

Let's start by scaffolding our app with Next.js's ``create-next-app`` tool.
We'll be using TypeScript, ESLint and Tailwind CSS for this tutoria. Choose
all the defaults for answering prompts except for using src, we will opt out.
You can choose to use src but will have to update later some configuration, so
for simplicity we will not use it in the tutorial.

.. code-block:: bash

  $ npx create-next-app --typescript docs-chatbot

After the prompts, ``create-next-app`` will create a folder with your project
name and install the required dependencies.

Before we start writing code, let's first obtain an OpenAI API key, install
EdgeDB CLI and create a local EdgeDB instance. (We will use a local instance
here, but you could use an EdgeDB Cloud instance instead if you prefer).
We need the API key in order to use OpenAI's APIs for generating embeddings
and answering questions. We need an EdgeDB instance to store section contents
and the generated embeddings.

Get an OpenAI API key
^^^^^^^^^^^^^^^^^^^^^
1. Log in or sign up to the `OpenAI platform <https://platform.openai.com/account/api-keys>`_.
2. Create new `secret key <https://platform.openai.com/account/api-keys>`_.
3. Create a ``.env.local`` file in the root of your project and copy your key
   here in the following format: ``OPENAI_KEY="<my-openai-key>"``.


Install EdgeDB CLI and create a local EdgeDB instance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
We firstly need to install EdgeDB CLI. On Linux or MacOS, run the following
in your terminal and follow the on-screen instructions:

.. code-block:: bash

  $ curl --proto '=https' --tlsv1.2 -sSf https://sh.edgedb.com | sh

For installation on Windows machines and more info check `EdgeDB CLI <https://www.edgedb.com/docs/cli/index>`_.

The easiest and fastest way to create an EdgeDB project or convert existing
directory to EdgeDB project is to run ``edgedb project init`` in the terminal
inside the project root. The CLI will ask you what you want to call the local
instance that will be created. It will default to the name of your current
directory with any illegal characters replaced. It will also ask you for
EdgeDB version that you want to use with this project, pick the default one
(which is the latest stable one).

.. code-block:: bash

  $ edgedb project init
  No `edgedb.toml` found in `/Users/Documents/projects/edgedb/docs-chatbot` or above

  Do you want to initialize a new project? [Y/n]
  > Y

  Specify the name of EdgeDB instance to use with this project [default: chatgpt_guide]:
  > docs-chatbot

  Checking EdgeDB versions...
  Specify the version of EdgeDB to use with this project [default: 3.2]:
  > 3.2

Great, the CLI should have set up an EdgeDB project, and instance, and a
database within that instance. You can confirm project creation by checking
for an ``edgedb.toml`` file and a ``dbschema`` directory in your project. You
can check if the instance is running with the ``edgedb instance list``
command. Search for the name of the instance you've just created and check the
status (it is okay if it is inactive, the status will change into running when
you connect to the database). You can do that by running ``edgedb`` in the
terminal to connect to it via REPL or by running ``edgedb ui`` to connect
using the UI.

Ok, so now we can start with actual implementation details.

Convert documentation into a unified format
-------------------------------------------
OpenAI language models accept strings as input. So, the most common formats
are Markdown and plain text files because you can use them straight away
without any extra steps. It is possible to use HTML (and probably other
formats too) but they usually introduce a lot of selectors and tags that are
not relevant to the meaning of the text inside it, so you should either clean
those files and extract content before using it with OpenAI or you can
stringify and use the whole thing but then you will pay for all those extra
tokens (OpenAI pricing models are per number of tokens used). Usually all
available solutions firstly convert their docs into Markdown or text files.
There are different libraries and tools available online that can help with
this. But you maybe still need to write some custom scripts to further clean
your data, depending on what is your starting point.

We will here use ready Markdown files. Our starting point was ..todo ask james.

Create ``docs`` folder in the root of your project. You can copy paste here ..todo
EdgeDB markdown files that we will use or use your own markdown or text files
(if you use text files you should just be careful to later replace ``.md``
extension in the code with proper extension).

Split the converted documentation into sections
-----------------------------------------------
..todo

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
will need to store the section content and embeddings. We will also save
each section's relative path and content checksum. The checksum will allow
us to easily determine which files of the documentation has changed every
time we run the embeddings generation script. This way, we can re-generate
embeddings and write to the database only for those changed sections. We will
also need to save the number of tokens for every section. We will need this
later when calculating how many similar sections fit inside the prompt context.

Open the empty schema file generated when you initialized the EdgeDB project
``dbschema/default.esdl`` and add this code to it:

.. code-block:: sdl

    # dbschema/default.esdl

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
falls well under that threshold. (For more than 1,000,000 entries, you should
use the square root of the total number for lists.). In our case indexing
does not have much impact, but if you plan to store and query huge amount of
entries, an index is recommended.

We apply this schema by creating and running a migration.

.. code-block:: bash

  $ edgedb migration create
  $ edgedb migrate

Generate embeddings
^^^^^^^^^^^^^^^^^^^

Majority of the work related to embeddings generation, storage and update we
will do inside ``generate-embeddings.ts`` file. You can copy / paste the next
code inside your script file and then we will go through it piece by piece.

.. code-block:: typescript

  import { Configuration, OpenAIApi } from "openai";
  import { createHash } from "crypto";
  import dotenv from "dotenv";
  import { promises as fs } from "fs";
  import { inspect } from "util";
  import { join } from "path";
  import getTokensLen from "./getTokensLen";
  import * as edgedb from "edgedb";
  import e from "../dbschema/edgeql-js";

  dotenv.config({ path: ".env.local" });

  interface Section {
    id?: string;
    path: string;
    tokens: number;
    checksum: string;
    content: string;
    embedding: number[];
  }

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

  class EmbeddingSource {
    path: string;
    checksum?: string;
    content?: string;

    constructor(public filePath: string) {
      this.path = filePath.replace(/^.docs/, "");
    }

    async load() {
      const content = await fs.readFile(this.filePath, "utf8");
      const checksum = createHash("sha256").update(content).digest("base64");

      this.checksum = checksum;
      this.content = content;

      return {
        checksum,
        content,
      };
    }
  }

  // --refresh: Regenerate all embeddings, otherwise just new changes.
  async function generateEmbeddings() {
    const args = process.argv.slice(2);
    const shouldRefresh = args.includes("--refresh");

    if (!process.env.OPENAI_KEY) {
      return console.log(
        "Environment variable OPENAI_KEY is required: skipping embeddings generation."
      );
    }

    const configuration = new Configuration({
      apiKey: process.env.OPENAI_KEY,
    });

    const openai = new OpenAIApi(configuration);

    const client = edgedb.createClient();

    const embeddingSources: EmbeddingSource[] = [
      ...(await walk("docs")).map((entry) => new EmbeddingSource(entry.path)),
    ];

    console.log(`Discovered ${embeddingSources.length} pages`);

    if (shouldRefresh) {
      console.log("Refresh flag set, re-generating all pages.");

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
          const { checksum, content } = await embeddingSource.load();
          // OpenAI recommends replacing newlines with spaces for
          // best results (specific to embeddings)
          const contentTrimmed = content.replace(/\n/g, " ");
          contents.push(contentTrimmed);
          sections.push({ path, checksum, content, tokens: 0, embedding: [] });
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
              checksum: e.cast(e.str, section.checksum),
              tokens: e.cast(e.int16, section.tokens),
              embedding: e.cast(e.OpenAIEmbedding, section.embedding),
            });
          });
        });

        await query.run(client, { sections });
      } catch (err) {
        console.error("Error while trying to regenerate all embeddings.", err);
      }
    } else {
      console.log("Checking which pages are new or have changed.");

      try {
        const query = e.select(e.Section, () => ({
          path: true,
          checksum: true,
        }));

        const existingSections = await query.run(client);

        const updatedSections: Section[] = [];
        const newSections: Section[] = [];

        for (const embeddingSource of embeddingSources) {
          const { path } = embeddingSource;

          const { checksum, content } = await embeddingSource.load();

          // Check for existing section in DB and compare checksums
          const existingSection = existingSections.filter(
            (section) => section.path == path
          )[0];

          if (existingSection?.checksum === checksum) continue;

          const input = content.replace(/\n/g, " ");

          const embeddingResponse = await openai.createEmbedding({
            model: "text-embedding-ada-002",
            input,
          });

          if (embeddingResponse.status !== 200) {
            throw new Error(inspect(embeddingResponse.data, false, 2));
          }

          const [responseData] = embeddingResponse.data.data;

          const tokens = (await getTokensLen([input]))[0];

          if (existingSection) {
            updatedSections.push({
              path,
              content,
              checksum,
              tokens,
              embedding: responseData.embedding,
            });
          } else {
            newSections.push({
              path,
              content,
              checksum,
              tokens,
              embedding: responseData.embedding,
            });
          }
        }

        if (updatedSections.length) {
          console.log(
            "Update sections at paths",
            updatedSections.map((section) => section.path)
          );
          const query = e.params(
            {
              sections: e.array(
                e.tuple({
                  path: e.str,
                  content: e.str,
                  checksum: e.str,
                  tokens: e.int16,
                  embedding: e.OpenAIEmbedding,
                })
              ),
            },
            ({ sections }) => {
              return e.for(e.array_unpack(sections), (section) => {
                return e.update(e.Section, () => ({
                  filter_single: { path: section.path },
                  set: {
                    content: section.content,
                    checksum: section.checksum,
                    tokens: section.tokens,
                    embedding: section.embedding,
                  },
                }));
              });
            }
          );

          await query.run(client, {
            sections: updatedSections,
          });
        }
        // Insert new sections.
        if (newSections.length) {
          console.log(
            "Insert new section at paths",
            newSections.map((section) => section.path)
          );
          const query = e.params({ sections: e.json }, ({ sections }) => {
            return e.for(e.json_array_unpack(sections), (section) => {
              return e.insert(e.Section, {
                path: e.cast(e.str, section.path),
                content: e.cast(e.str, section.content),
                checksum: e.cast(e.str, section.checksum),
                tokens: e.cast(e.int16, section.tokens),
                embedding: e.cast(e.OpenAIEmbedding, section.embedding),
              });
            });
          });

          await query.run(client, {
            sections: newSections,
          });
        }

        // If some sections are deleted in docs delete them from db too
        const deletedSectionsPaths: string[] = [];

        for (const existingSection of existingSections) {
          const docsSection = embeddingSources.filter(
            (section) => section.path == existingSection.path
          )[0];

          if (!docsSection) deletedSectionsPaths.push(existingSection.path);
        }

        if (deletedSectionsPaths.length) {
          console.log("Delete sections at paths", deletedSectionsPaths);

          const query = e.params({ paths: e.array(e.str) }, ({ paths }) =>
            e.delete(e.Section, (section) => ({
              filter: e.op(section.path, "in", e.array_unpack(paths)),
            }))
          );
          await query.run(client, { paths: deletedSectionsPaths });
        }
      } catch (err) {
        console.error("Error while trying to update embeddings.", err);
      }
    }

    console.log("Embedding generation complete");
  }

    async function main() {
      await generateEmbeddings();
    }

    main().catch((err) =>
      console.error("Error has ocurred while generating embeddings.", err)
    );

Let's try to understand this huge pile of code.

- In order for the above script to compile and work we need to install
  ``openai`` and ``dotenv`` npm packages. You can use ``yarn`` or ``npm``
  package managers. We will use yarn in this guide.

  .. code-block:: bash

    $ yarn add openai dotenv

  Majority of other imports: ``crypto``, ``fs``, ``util`` and ``path`` are
  Node modules available to us. We will explain ``getTokensLength`` and
  ``dbschema/edgeql-js`` a bit later.

- Firstly we use ``dotenv`` to import environment variables we have inside
  ``.env.local`` file, specifically ``OPENAI_KEY``, we will need it later.

- Then we need to define Section TS interface that corresponds to the Section
  type we have defined in schema.

- We need to store sections paths to the database. Since our ``docs`` folder
  contains sections at multiple levels, we should have a function that loops
  through all section files and outputs an array of all paths relative to the
  project root, and sort them. This is what ``walk`` does. The output it
  produces looks like:

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

- We need to read the section content from it's path in the file system,
  create the checksum for the content and then save both in the database
  together with the path (we cut off the ``.gpt/`` repeating part from the
  beginning when storing path). ``EmbeddingSource`` class takes relative
  section path and generates path, content and checksum for that file.

- The next piece of code is actual ``generateEmbeddings`` function that
  fetches embeddings from the OpenAI and store them inside EdgeDB database.
  We don't have a lot of files to generate embeddings for, but in general
  real projects usually have thousands of files. For example, we at EdgeDB
  have around 3000, fetching and storing embeddings one by one will take more
  than half an hour so we try to parallelise and speed things up as much as
  possible.

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

  First two lines of generateEmbeddings function store inside ``shouldRefresh``
  variable how we want to run the script, re-generate all embeddings or perform
  just an update (the default).

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

- Now is the time to check the ``shouldRefresh`` variable and create two paths.

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
                  checksum:= <str>section['checksum'],
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
                  checksum: e.cast(e.str, section.checksum),
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
