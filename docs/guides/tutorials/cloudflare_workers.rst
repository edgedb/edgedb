.. _ref_guide_cloudflare_workers:

==================
Cloudflare Workers
==================

:edb-alt-title: Using Gel in Cloudflare Workers


This guide demonstrates how to integrate Gel with Cloudflare Workers to
build serverless applications that can interact with Gel.

It covers the following:

- Setting up a new Cloudflare Worker project
- Configuring Gel
- Using Gel in a Cloudflare Worker
- Deploying the Worker to Cloudflare

You can use this project as a reference: `Gel Cloudflare Workers Example`_.

Prerequisites
-------------

`Sign up for a Cloudflare account`_ to later deploy your worker.

Ensure you have the following installed:

- `Node.js`_
- :ref:`Gel CLI <ref_intro_cli>`

.. _Sign up for a Cloudflare account: https://dash.cloudflare.com/sign-up
.. _Node.js: https://nodejs.org/en/

Setup and configuration
-----------------------

Initialize a New Cloudflare Worker Project
===========================================

Use the `create-cloudflare`_ package to create a new Cloudflare Worker project.

.. _create-cloudflare: https://www.npmjs.com/package/create-cloudflare

.. code-block:: bash

    $ npm create cloudflare@latest # or pnpm, yarn, bun

    # or
    $ npx create-cloudflare@latest

Answer the prompts to create a new project. Pick the *"Hello World" Worker*
template to get started.

You'll be asked if you want to put your project on Cloudflare.
If you say yes, you'll need to sign in (if you haven't already).
If you don't want to deploy right away, switch to the project folder
you just made to start writing your code. When you're ready to deploy your
project on Cloudflare, you can run ``npx wrangler deploy`` to push it.

.. note:: Using Wrangler CLI

    If you prefer using `Wrangler`_ to set up your worker, you can use the
    :code:`wrangler generate` command to create a new project.

.. _Wrangler: https://developers.cloudflare.com/workers/cli-wrangler


Configure Gel
=============

You can use `Gel Cloud`_ for a managed service or run Gel locally.

.. _`Gel Cloud`: https://www.edgedb.com/cloud

**Local Gel Setup (Optional for Gel Cloud Users)**

If you're running Gel locally, you can use the following command
to create a new instance:

.. code-block:: bash

    $ gel project init

It creates an |gel.toml| config file and a schema file
:code:`dbschema/default.gel`.

It also spins up an Gel instance and associates it with the current
directory.
As long as you're inside the project directory, all CLI commands will
be executed against this instance.

You can run :code:`edgedb` in your terminal to open an
interactive REPL to your instance.

.. code-block:: bash

    $ gel

**Install the Gel npm package**

.. code-block:: bash

    $ npm install gel # or pnpm, yarn, bun

**Extend The Default Schema (Optional)**

You can extend the default schema, :code:`dbschema/default.gel`, to define
your data model, and then try it out in the Cloudflare Worker code.

Add new types to the schema file:

.. code-block:: sdl

    module default {
      type Movie {
        required title: str {
          constraint exclusive;
        };
        multi actors: Person;
      }

      type Person {
        required name: str;
      }
    }

Then apply the schema schema to your Gel instance:

.. code-block:: bash

    $ gel migration create
    $ gel migrate

Using Gel in a Cloudflare Worker
================================

Open the :code:`index.ts` file from the :code:`src` directory in your project,
and remove the default code.

To interact with your **local Gel instance**, use the following code:

.. code-block:: typescript

    import * as edgedb from "edgedb";

    export default {
      async fetch(
        _request: Request,
        env: Env,
        ctx: ExecutionContext,
      ): Promise<Response> {
        const client = edgedb.createHttpClient({
          tlsSecurity: "insecure",
          dsn: "<your-edgedb-dsn>",
        });
        const movies = await client.query(`select Movie { title }`);
        return new Response(JSON.stringify(movies, null, 2), {
          headers: {
            "content-type": "application/json;charset=UTF-8",
          },
        });
      },
    } satisfies ExportedHandler<Env>;


.. note:: Gel DSN

    Replace :code:`<your-edgedb-dsn>` with your Gel DSN.
    You can obtain your Gel DSN from the command line by running:

    .. code-block:: bash

        $ gel instance credentials --insecure-dsn

.. note:: tlsSecurity

    The :code:`tlsSecurity` option is set to :code:`insecure` to allow
    connections to a local Gel instance. This lets you test your
    Cloudflare Worker locally. **Don't use this option in production.**

**Client Setup with Gel Cloud**

If you're using Gel Cloud, you can instead use the following code to
set up the client:

.. code-block:: typescript

   const client = edgedb.createHttpClient({
     instanceName: env.EDGEDB_INSTANCE,
     secretKey: env.EDGEDB_SECRET_KEY,
   });

.. note:: Environment variables

    You can obtain :code:`EDGEDB_INSTANCE` and :code:`EDGEDB_SECRET_KEY`
    values from the Gel Cloud dashboard.

You will need to set the :code:`EDGEDB_INSTANCE` and :code:`EDGEDB_SECRET`
environment variables in your Cloudflare Worker project.

Add the following to your :code:`wrangler.toml` file:

.. code-block:: toml

    [vars]
    EDGEDB_INSTANCE = "your-edgedb-instance"
    EDGEDB_SECRET_KEY = "your-edgedb-secret-key"

Next, you can run :code:`wrangler types` to generate the types for your
environment variables.

**Running the Worker**

.. note:: Adding polyfills for Node.js

    The :code:`edgedb` package currently uses Node.js built-in modules
    that are not available in the Cloudflare Worker environment.
    You have to add the following line to your :code:`wrangler.toml` file
    to include the polyfills:

    .. code-block:: toml

        node_compat = true

To run the worker locally, use the following command:

.. code-block:: bash

    $ npm run dev # or pnpm, yarn, bun

This will start a local server at :code:`http://localhost:8787`.
Run :code:`curl http://localhost:8787` to see the response.

**Deploying the Worker to Cloudflare**

To deploy the worker to Cloudflare, use the following command:

.. code-block:: bash

    $ npm run deploy # or pnpm, yarn, bun

This will deploy the worker to Cloudflare and provide you with a URL
to access your worker.

Wrapping up
===========

Congratulations! You have successfully integrated Gel with
Cloudflare Workers.

Here's a minimal starter project that you can use as a
reference: `Gel Cloudflare Workers Example`_.

Check out the `Cloudflare Workers documentation`_ for more information and
to learn about the various features and capabilities of Cloudflare Workers.

.. _`Gel Cloudflare Workers Example`:
  https://github.com/geldata/gel-examples/tree/main/cloudflare-workers
.. _`Cloudflare Workers documentation`:
  https://developers.cloudflare.com/workers
