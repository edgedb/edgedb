.. _ref_guide_nextjs:

=======
Next.js
=======

:edb-alt-title: Using |Gel| with Next.js

This guide shows how to integrate |Gel| with Next.js applications.

.. note::

   If you have an existing Next.js project, skip to :ref:`Adding |Gel| to an existing project <adding_gel_existing>` section.

Creating a new project
-----------------------

We recommend using the ``@gel/create`` package to set up a complete Next.js + |Gel| project.

.. code-block:: bash

  $ npx @gel/create

It's a guided CLI tool that helps you configure your new app. Follow these prompts:

1. Enter your project name
2. Choose ``Next.js`` as the web framework
3. Choose whether to use |Gel| Auth (optional)
4. Choose TypeScript (recommended)
5. Select your preferred router (App or Pages)
6. Choose whether to use Tailwind CSS
7. Configure project structure preferences
8. Let the tool initialize |Gel| and git repo

The tool will create a new directory with your project name containing a fully configured Next.js + |Gel| setup.

.. _adding_gel_existing:

Adding |Gel| to an Existing Project
-------------------------------------

If you already have a Next.js project, follow these steps to add |Gel|:

1. Initialize |Gel| in your project directory:

   .. code-block:: bash

     $ npx gel project init

2. Install the |Gel| client library:

   .. code-block:: bash

     $ npm install gel
     # or yarn add gel
     # or pnpm add gel

3. Install the query builder generator (recommended for TypeScript projects):

   .. code-block:: bash

     $ npm install --save-dev @gel/generate
     # or yarn add --dev @gel/generate
     # or pnpm add --dev @gel/generate

Updating the schema
-------------------

Start by adding new types to your |Gel| schema:

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

   $ npx gel migration create
   $ npx gel migrate

You can now run queries against your new schema. Run the following command to open the |Gel| REPL:

.. code-block:: bash

  $ npx gel

You can then execute the following ``insert`` statements:

.. code-block:: edgeql-repl

  gel> insert BlogPost {
  .......   title := "This one weird trick makes using databases fun",
  .......   content := "Use Gel"
  ....... };
  {default::BlogPost {id: 7f301d02-c780-11ec-8a1a-a34776e884a0}}
  gel> insert BlogPost {
  .......   title := "How to build a blog with |Gel| and Next.js",
  .......   content := "Let's start by scaffolding our app..."
  ....... };
  {default::BlogPost {id: 88c800e6-c780-11ec-8a1a-b3a3020189dd}}


Alternatively, you can use the |Gel| UI to manage your schema and data. Open the |Gel| UI by running:

.. code-block:: bash

  $ npx gel ui


Generating the query builder​
-----------------------------

This step is optional but recommended for TypeScript projects. The query builder generates TypeScript types for your EdgeQL queries:

.. code-block:: bash

  $ npx @gel/generate edgeql-js

The command introspects your schema and generates a query builder in the ``dbschema/edgeql-js`` directory.

Using |Gel| in Next.js
-----------------------

|Gel| with React Server Components
==================================

Server Components allow you to fetch data directly from the server:

.. code-block:: tsx
  :caption: app/page.tsx

  import { createClient } from 'gel';
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

Server Actions
==============

Server Actions provide a way to mutate data directly from the client. Create an ``actions.ts`` file and add the following code:

.. code-block:: tsx
  :caption: app/actions.ts

  'use server';

  import { createClient } from 'gel';
  import e from '@/dbschema/edgeql-js';
  import { revalidatePath } from 'next/cache';

  const client = createClient();

  export async function createPost(formData: FormData) {
    const title = formData.get('title') as string;
    const content = formData.get('content') as string;

    await e.insert(e.BlogPost, {
      title,
      content,
    }).run(client);

    revalidatePath('/');
  }


Then, in your client component, you can use the ``createPost`` function:

.. code-block:: tsx
  :caption: app/CreatePost.tsx

  'use client';
  
  import { createPost } from './actions';

  export default function CreatePost() {
    return (
      <form action={createPost}>
        <input
          type="text"
          name="title"
          placeholder="Post title"
          required
        />
        <textarea
          name="content"
          placeholder="Post content"
          required
        />
        <button type="submit">Create Post</button>
      </form>
    );
  }

With API Routes (Pages Router)
==============================

Create an API route and fetch data from the client side:

.. code-block:: tsx
  :caption: pages/api/posts.ts

  import type { NextApiRequest, NextApiResponse } from 'next';
  import { createClient } from 'gel';
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

To ensure the query builder is generated both during local development and deployment, you'll need to configure it in two places:

1. In your ``gel.toml`` for local development:

   .. code-block:: toml

     [hooks.generate_qb]
     command = "npx @gel/generate edgeql-js"
     after = ["migrate"]

2. In your ``package.json`` for deployment builds:

   .. code-block:: json

     {
       "scripts": {
         "prebuild": "npx @gel/generate edgeql-js"
       }
     }

Using Vercel Marketplace (Recommended)
======================================

The easiest way to deploy your Next.js application with |Gel| is through the `Vercel Marketplace <https://vercel.com/blog/introducing-the-vercel-marketplace>`_:

1. Open your project's dashboard in Vercel
2. Navigate to the Storage tab
3. Select |Gel| from the Marketplace
4. Follow the prompts to provision your database

Benefits of using Vercel Marketplace integration:

- Seamless authentication with |Gel| Cloud using your Vercel account
- Automatic configuration of environment variables
- Integration with Vercel Preview deployments
- Consolidated billing through your Vercel account
- GitHub integration for continuous deployment

.. note::

   The pricing remains the same whether you access |Gel| through Vercel or directly via `Gel Cloud <https://cloud.edgedb.com>`_.

Alternative Deployment Options
==============================

If you prefer to manage your |Gel| deployment separately, you can:

1. Use |Gel| Cloud directly through `Gel Cloud <https://cloud.edgedb.com>`_
2. Self-host on your preferred cloud provider (`AWS <https://www.edgedb.com/docs/guides/deployment/aws_aurora_ecs>`_, `GCP <https://www.edgedb.com/docs/guides/deployment/gcp>`_, `Azure <https://www.edgedb.com/docs/guides/deployment/azure_flexibleserver>`_)

For these options, you'll need to configure the appropriate environment variables in your Vercel project settings.

Next Steps
----------

- Explore the `Gel documentation <https://www.edgedb.com/docs>`_ for more advanced queries and features
- Check out the `query builder documentation <https://docs.edgedb.com/libraries/js>`_ for TypeScript integration
- View example projects in the `gel-examples repository <https://github.com/edgedb/gel-examples>`_
