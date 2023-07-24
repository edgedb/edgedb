.. \_ref_guide_chatgpt_bot:

=======
ChatGPT
=======

:edb-alt-title: Build your own docs chatbot with ChatGPT and EdgeDB

*For additional context, check out* `our blog post about why and how we used
ChatGPT via embeddings <https://www.edgedb.com/blog/chit-chatting-with-edgedb-docs-via-chatgpt-and-pgvector>`_
*to create our* `“Ask AI”  <https://www.edgedb.com/blog/chit-chatting-with-edgedb-docs-via-chatgpt-and-pgvector>`_
*bot which answers questions related to the EdgeDB docs.*

In this guide, you'll learn to use EdgeDB with OpenAI's ChatGPT to make your own
documentation chatbot like the one we built.

.. image::
    https://www.edgedb.com/docs/tutorials/chatgpt/ask_ai.png
    :alt:
    :width: 100%

How it works
------------

Our “Ask AI” chatbot is backed by `OpenAI's ChatGPT <https://openai.com/blog/chatgpt>`_.
ChatGPT is an advanced language model that uses machine learning algorithms to
generate human-like responses based on the input it's given.

You have two options when integrating ChatGPT and language models in general:
by fine-tuning the model or by using embeddings. Fine-tuning produces the best
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

When using embeddings you are not training the language model. Instead you're
creating embeddings vectors for every piece of documentation which will later
help you find which documentation likely answers a user's question. When a
user asks a question, you create a new embedding for that question and
compare it against the embeddings generated from your documentation to find
the most similar embeddings. The answer is generated using the content that
corresponds to these similar embeddings.

**Note:** this guide is written mostly in Typescript, with the exception of
one small script in Python. This is because our website uses NextJS, so the
code I wrote for this feature is mostly server-side inside a Next project.
You can of course use Python or other languages to get the similar outcome.

Implementation overview
-----------------------

Each time the documents are updated, our implementation will:

1. convert our documentation into a unified format that is easily digestible by the OpenAI API
2. split the converted documentation into sections that can fit into the GPT context window
3. create embeddings for each section using `OpenAI's embeddings API <https://platform.openai.com/docs/guides/embeddings>`_
4. store the embeddings data in an EdgeDB database

Each time a user asks a question, our implementation will:

1. query the EdgeDB database for the documentation sections most relevant to the question using a similarity function.
2. inject these sections as a context into the prompt together with user question and submit a request to the OpenAI (OpenAI will give us back an answer that relies on its previous general knowledge and on the context we sent to it with bigger focus on the context.)
3. stream the OpenAI response back to the user in realtime

All code shown here is also available on GitHub. // TODO

Prerequisites
-------------

Before we start writing code, let's first obtain an OpenAI API key and create
a local EdgeDB instance. (We will use a local instance here, but you could
use an EdgeDB Cloud instance instead if you prefer). We need the API key in
order to use OpenAI's APIs for generating embeddings and answering questions.
We need an EdgeDB instance to store section contents and the generated embeddings.

Create a new directory somewhere on your computer for your project — I'm
creating one named chatgpt-guide — and let's get started!

Get an OpenAI API key
^^^^^^^^^^^^^^^^^^^^^
1. Log in or sign up to the `OpenAI platform <https://platform.openai.com/account/api-keys>`_.
2. Create new secret key.
3. Create a `.env` file in the root of your project and copy your key here in
  the following format: `OPENAI_KEY="<my-openai-key>"`.

Create an EdgeDB project with a local EdgeDB instance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The easiest and fastest way to create an EdgeDB project is to run
``edgedb project init`` in the terminal inside the project root. The CLI will
ask you what you want to call the local instance that will be created. It
will default to the name of your current directory with any illegal
characters replaced. Feel free to use this name or set your own. It will also
ask which EdgeDB version you want to use. Any version greater than 3.0 should
work.

.. code-block:: bash
    $ edgedb project init

    No `edgedb.toml` found in `/Users/edgedb/Documents/projects/edgedb/demos/chatgpt-guide` or above
    Do you want to initialize a new project? [Y/n]
    > Y
    Specify the name of EdgeDB instance to use with this project [default: chatgpt_guide]:
    > chatgpt_guide
    Checking EdgeDB versions...
    Specify the version of EdgeDB to use with this project [default: 3.2]:
    > 3.2

Once you've answered the prompts, the CLI will create your project and
instance and a database within that instance. You can confirm project
creation by checking for an ``edgedb.toml`` file and a ``dbschema``
directory in your project. You can check if the instance is running with the
``edgedb instance list`` command. Search for the name of the instance you just
created and check the status. Confirm your database exists by running edgedb
to connect to it via REPL or by running edgedb ui to connect using the UI.

// TODO

Create embeddings and store them in the EdgeDB database
-------------------------------------------------------

Finally, we're ready to create embeddings for all sections and store them in
our database.

Schema
^^^^^^

Let's create the schema for the data we're going to save. We want to make it
as simple as possible and store only the relevant data. We will need to store
the section content and embeddings. We will also save each section's relative
path and content checksum. The checksum will allow us to easily determine on
subsequent documentation builds which parts of the documentation has changed.
This way, we can generate embeddings and write to the database only for those
changed parts. We will also need to save the number of tokens for every
section. We will need this later when calculating how many similar sections
fit inside the prompt context.

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
    }

    index ext::pgvector::ivfflat_cosine(lists := 3)
        on (.embedding);
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

Below that type, you'll find the index. In order to speed up queries, we add
the index that corresponds to the ``cosine_similarity`` function which is
``ivfflat_cosine``. We are using the value ``3`` for the ``lists`` parameter
because best practice is to use the number of objects divided by 1,000 for up
to 1,000,000 entries. Our database will have around 3,000 total entries which
falls well under that threshold. (For more than 1,000,000 entries, you would
use the square root of the total number for lists.). In our case indexing
does not have much impact, but if you plan to store and query huge amount of
objects, an index is recommended.

We apply this schema by creating and running a migration.
.. code-block:: bash
    edgeb migration create
    edgedb migrate


Generate embeddings
^^^^^^^^^^^^^^^^^^^

Let's create ``gpt`` folder in the root of the project and
``generate-embeddings.ts`` file inside it. Majority of the work related to
embeddings generation, storage and update we will do inside this file.

Let's type TS section interface that corresponds to the schema interface:

.. code-block:: typescript
    interface Section {
        id?: string;
        path: string;
        tokens: number;
        checksum: string;
        content: string;
        embedding: number[];
    }

We need to store sections paths to the database. If docs/gpt folder contains
sections at multiple levels (there are subfolders) we should create a
function that will output an array of all sections' paths relative to the
project root and sort them.

.. code-block:: typescript
    type WalkEntry = {
    path: string;
    };

    async function walk(dir: string): Promise<WalkEntry[]> {
    const immediateFiles = await fs.readdir(dir);

    const recursiveFiles: {path: string}[][] = await Promise.all(
        immediateFiles.map(async (file: any) => {
        const path = join(dir, file);
        const stats = await fs.stat(path);
        if (stats.isDirectory()) return walk(path);
        else if (stats.isFile()) return [{path}];
        else return [];
        })
    );

    const flattenedFiles: {path: string}[] = recursiveFiles.reduce(
        (all, folderContents) => all.concat(folderContents),
        []
    );

    return flattenedFiles.sort((a, b) => a.path.localeCompare(b.path));
    }

It will give us the output:

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

We will need to read the section content from it's path in the file system,
create the checksum for the content and then save both in the database
together with the path (I cut off the .docs/gpt/ repeating part from the
beginning when storing path). I created ``EmbeddingSource`` class for this
purpose, it takes relative section path and generates path, content and checksum.

.. code-block:: typescript
    class EmbeddingSource {
        checksum?: string;
        content?: string;
        path: string;

        constructor(public filePath: string) {
            this.path = filePath.replace(/^.docs\/gpt\//, "");
        }

        async load() {
            const content = await fs.readFile(this.filePath, "utf8");
            const checksum = createHash("sha256").update(content).digest("base64");

            this.content = content;
            this.checksum = checksum;

            return {
            checksum,
            content,
            };
        }
    }


The next piece of code is actual ``generateEmbeddings`` function that fetches
embeddings from the OpenAI and store them inside EdgeDB database. We have
around 3000 section at this moment, fetching and storing embeddings one by
one will take more than half an hour so I tried to parallelise and speed
things up as much as possible. OpenAI embeddings API endpoint let us send a
string or array of strings in order to get embeddings for those.

You will see in the code below that I make few calls to the OpenAI because
getting all embeddings in one request results with an error (I just get 400
error back meaning that input is invalid). I suspect this is connected to
`TPM <https://platform.openai.com/docs/guides/rate-limits/overview>`_ (tokens
per minute) limit even though this isn't mentioned anywhere in the error.

.. code-block:: typescript
    import {Configuration, OpenAIApi} from "openai";
    import {createHash} from "crypto";
    import {join} from "path";
    import * as edgedb from "edgedb";
    import e from "dbschema/edgeql-js";
    import dotenv from "dotenv";
    import getTokensLen from "./getTokensLen";

    dotenv.config();

    async function generateEmbeddings() {
        if (!process.env.OPENAI_KEY) {
            return console.error(
            "Environment variable OPENAI_KEY is required: skipping embeddings generation"
            );
        }

        const embeddingSources: EmbeddingSource[] = [
            ...(await walk(".build-cache/docs/gpt")).map(
            (entry) => new EmbeddingSource(entry.path)
            ),
        ];

        const configuration = new Configuration({
            apiKey: process.env.OPENAI_KEY,
        });

        const openai = new OpenAIApi(configuration);

        try {
            const contents: string[] = [];
            const sections: Section[] = [];

            for (const embeddingSource of embeddingSources) {
            const {path} = embeddingSource;
            const {checksum, content} = await embeddingSource.load();
            // OpenAI recommends replacing newlines with spaces for
            // the best results (specific to embeddings).
            const contentTrimmed = content.replace(/\n/g, " ");
            contents.push(contentTrimmed);
            sections.push({path, checksum, content, tokens: 0, embedding: []});
            }

            const tokens = await getTokensLen(contents);

            // We get error if we try to get embeddings for all sections at once,
            // so we'll create few chunks and make few calls to the OpenAI.
            const contentChunks: string[][] = [];
            const chunkSize = 500;

            for (let i = 0; i < contents.length; i += chunkSize) {
            const chunk = contents.slice(i, i + chunkSize);
            contentChunks.push(chunk);
            }

            for (let i = 0; i < contentChunks.length; i++) {
            const embeddingResponse = await openai.createEmbedding({
                model: "text-embedding-ada-002",
                input: contentChunks[i],
            });

            if (embeddingResponse.status !== 200) {
                throw new Error(inspect(embeddingResponse.data, false, 2));
            }

            embeddingResponse.data.data.forEach((item, j) => {
                sections[i * chunkSize + j].embedding = item.embedding;
                sections[i * chunkSize + j].tokens = tokens[i * chunkSize + j];
            });
            }

            // Bulk-insert all sections' data in EdgeDB database.
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
        } catch (err) {
            console.error("Error while trying to regenerate all embeddings.", err);
        }
    }

In the code above:

- we throw an error if the `OPENAI_KEY` is not available, we need it for
  accessing the OpenAI APIs.
- we create `contents` and `sections` arrays using `walk()` function and
  `EmbeddingSource` class we created earlier, `sections` will contain all
  data we want to store for a section and later we insert this array in
  the database
- we get number of tokens for each section with `const tokens = await getTokensLen(contents);`,
  I'll explain this function soon, just bare with me.
- we split `contents` array into chunks of 500 and fetch embeddings from
  OpenAI, we use `openai.createEmbedding` endpoint to get chunk embeddings
  and update  `sections` with embedding and token count
- we bulk-insert sections to the database

The util ``util.inspect()`` method just returns a string representation of
object that is intended for debugging. It's part of Node's util package.

**OpenAI token limit**

Later when we want to send similar sections as a context to the OpenAI
completions endpoint we need to know how many tokens each content has
in order to stay under the model token limit.

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
  model's limit, it might lose context for some tokens, which could negatively
  impact the quality of the generated text.

So in general, for the things to work, there is token limit per request which
includes both the prompt and the answer. As part of the prompt we will send
user's question and similar sections as context and we have to make sure to
not send too many sections as context because we will either get error back
or the answer can be cut off if there are few tokens left for the answer.

At the moment we use GPT-4 and its token limit is 8192.

**How to calculate number of tokens per section**

There are at least 3 ways to solve this:

- when you send one string to the OpenAI embedding endpoint you will get back
  together with the embedding array also the **prompt_tokens** field telling
  you how many tokens the submitted content has and then you can store this
  in the database together with other data
- second way is to use some npm library that generates tokens array for the
  string you provide, and then you calculate the length of that array
  (`gpt-tokenizer <https://www.npmjs.com/package/gpt-tokenizer>`_ for example)
- the 3rd way is to use OpenAI `tiktoken <https://github.com/openai/tiktoken >`_
  library which should be faster than npm alternatives (and probably better
  maintained), but it's supposed to be used with python so we need to write a
  python script in order to calculate tokens in this way
