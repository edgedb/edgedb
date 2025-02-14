.. _ref_guide_nextjs:

=======
Next.js
=======

:edb-alt-title: Using EdgeDB with Next.js

This guide shows how to integrate EdgeDB with Next.js applications.

.. note::

   If you have an existing Next.js project, skip to :ref:`Adding EdgeDB to an existing project <adding_edgedb_existing>` section.

Creating a new project
-----------------------

We recommend using the ``@edgedb/create`` package to set up a complete Next.js + EdgeDB project.

.. code-block:: bash

  $ npx @edgedb/create

It's a guided CLI tool that helps you configure your new app. Follow these prompts:

1. Enter your project name
2. Choose ``Next.js`` as the web framework
3. Choose whether to use EdgeDB Auth (optional)
4. Choose TypeScript (recommended)
5. Select your preferred router (App or Pages)
6. Choose whether to use Tailwind CSS
7. Configure project structure preferences
8. Let the tool initialize EdgeDB and git repo

The tool will create a new directory with your project name containing a fully configured Next.js + EdgeDB setup.

.. _adding_edgedb_existing:

Adding EdgeDB to an Existing Project
-------------------------------------

If you already have a Next.js project, follow these steps to add EdgeDB:

1. Initialize EdgeDB in your project directory:

   .. code-block:: bash

     $ npx edgedb project init

2. Install the EdgeDB client library:

   .. code-block:: bash

     $ npm install edgedb
     # or yarn add edgedb
     # or pnpm add edgedb

3. Install the query builder generator (recommended for TypeScript projects):

   .. code-block:: bash

     $ npm install --save-dev @edgedb/generate
     # or yarn add --dev @edgedb/generate
     # or pnpm add --dev @edgedb/generate

Updating the schema
-------------------

Start by adding new types to your EdgeDB schema:

.. code-block:: sdl
  :caption: dbschema/default.esdl

  module default {
    type BlogPost {
      required title: str;
      required content: str {
        default := ""
      }
    }
  }

Then create and apply your first migration:

.. code-block:: bash

   $ npx edgedb migration create
   $ npx edgedb migrate

You can now run queries against your new schema. Run the following command to open the EdgeDB REPL:

.. code-block:: bash

  $ npx edgedb

You can then execute the following ``insert`` statements:

.. code-block:: edgeql-repl

  edgedb> insert BlogPost {
  .......   title := "This one weird trick makes using databases fun",
  .......   content := "Use EdgeDB"
  ....... };
  {default::BlogPost {id: 7f301d02-c780-11ec-8a1a-a34776e884a0}}
  edgedb> insert BlogPost {
  .......   title := "How to build a blog with EdgeDB and Next.js",
  .......   content := "Let's start by scaffolding our app..."
  ....... };
  {default::BlogPost {id: 88c800e6-c780-11ec-8a1a-b3a3020189dd}}


Alternatively, you can use the EdgeDB UI to manage your schema and data. Open the EdgeDB UI by running:

.. code-block:: bash

  $ npx edgedb ui


Generating the query builder​ 
-----------------------------

This step is optional but recommended for TypeScript projects. The query builder generates TypeScript types for your EdgeQL queries:

.. code-block:: bash

  $ npx @edgedb/generate edgeql-js

The command introspects your schema and generates a query builder in the ``dbschema/edgeql-js`` directory.

Using EdgeDB in Next.js
-----------------------

EdgeDB with React Server Components (App Router)
================================================

Create a server component that fetches data directly from EdgeDB:

.. code-block:: tsx
  :caption: app/page.tsx

  import { createClient } from 'edgedb';
  import e from '@/dbschema/edgeql-js';

  const client = createClient();

  export default async function Posts() {
    const posts = await e.select(e.BlogPost, () => ({
      id: true,
      title: true,
      content: true,
    })).run(client);

    return (
      <div>
        {posts.map(post => (
          <article key={post.id}>
            <h2>{post.title}</h2>
            <p>{post.content}</p>
          </article>
        ))}
      </div>
    );
  }

With API Routes
===============

Create an API route and fetch data from the client side:

.. code-block:: tsx
  :caption: pages/api/posts.ts

  import type { NextApiRequest, NextApiResponse } from 'next';
  import { createClient } from 'edgedb';
  import e from '@/dbschema/edgeql-js';

  const client = createClient();

  export default async function handler(
    req: NextApiRequest,
    res: NextApiResponse
  ) {
    const posts = await e.select(e.BlogPost, () => ({
      id: true,
      title: true,
      content: true,
    })).run(client);
    
    res.status(200).json(posts);
  }

.. code-block:: tsx
  :caption: pages/api/posts.ts

  import { useEffect, useState } from 'react';

  export default function Posts() {
    const [posts, setPosts] = useState(null);

    useEffect(() => {
      fetch('/api/posts')
        .then(res => res.json())
        .then(setPosts);
    }, []);

    if (!posts) return <div>Loading...</div>;

    return (
      <div>
        {posts.map(post => (
          <article key={post.id}>
            <h2>{post.title}</h2>
            <p>{post.content}</p>
          </article>
        ))}
      </div>
    );
  }

Deployment
-----------

First, add a ``prebuild`` script to your ``package.json`` to generate the query builder during deployment:

.. code-block:: json

  {
    "scripts": {
      "prebuild": "npx @edgedb/generate edgeql-js"
    }
  }

Using Vercel Marketplace (Recommended)
======================================

The easiest way to deploy your Next.js application with EdgeDB is through the `Vercel Marketplace <https://vercel.com/blog/introducing-the-vercel-marketplace>`_:

1. Open your project's dashboard in Vercel
2. Navigate to the Storage tab
3. Select EdgeDB from the Marketplace
4. Follow the prompts to provision your database

Benefits of using Vercel Marketplace integration:

- Seamless authentication with EdgeDB Cloud using your Vercel account
- Automatic configuration of environment variables
- Integration with Vercel Preview deployments
- Consolidated billing through your Vercel account
- GitHub integration for continuous deployment

.. note::
   
   The pricing remains the same whether you access EdgeDB through Vercel or directly via `EdgeDB Cloud <https://cloud.edgedb.com>`_.

Alternative Deployment Options
==============================

If you prefer to manage your EdgeDB deployment separately, you can:

1. Use EdgeDB Cloud directly through `EdgeDB Cloud <https://cloud.edgedb.com>`_
2. Self-host on your preferred cloud provider (`AWS <https://www.edgedb.com/docs/guides/deployment/aws_aurora_ecs>`_, `GCP <https://www.edgedb.com/docs/guides/deployment/gcp>`_, `Azure <https://www.edgedb.com/docs/guides/deployment/azure_flexibleserver>`_)

For these options, you'll need to configure the appropriate environment variables in your Vercel project settings.

Next Steps
----------

- Explore the `EdgeDB documentation <https://www.edgedb.com/docs>`_ for more advanced queries and features
- Check out the `query builder documentation <https://docs.edgedb.com/libraries/js>`_ for TypeScript integration
- View example projects in the `edgedb-examples repository <https://github.com/edgedb/edgedb-examples>`_
