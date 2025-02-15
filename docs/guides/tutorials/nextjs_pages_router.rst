.. _ref_guide_nextjs_pages_router:

======================
Next.js (Pages Router)
======================

:edb-alt-title: Building a simple blog application with Gel and
   Next.js (Pages Router)

We're going to build a simple blog application with
`Next.js <https://nextjs.org/>`_ and Gel. Let's start by scaffolding our
app with Next.js's ``create-next-app`` tool. We'll be using TypeScript for
this tutorial.

.. code-block:: bash

  $ npx create-next-app --typescript nextjs-blog

This will take a minute to run. The scaffolding tool is creating a simple
Next.js app and installing all our dependencies for us. Once it's complete,
let's navigate into the directory and start the dev server.

.. code-block:: bash

  $ cd nextjs-blog
  $ yarn dev

Open `localhost:3000 <http://localhost:3000>`_ to see the default Next.js
homepage. At this point the app's file structure looks like this:

.. code-block::

  README.md
  tsconfig.json
  package.json
  next.config.js
  next-env.d.ts
  pages
  ├── _app.tsx
  ├── api
  │   └── hello.ts
  └── index.tsx
  public
  ├── favicon.ico
  └── vercel.svg
  styles
  ├── Home.module.css
  └── globals.css

There's a custom App component defined in ``pages/_app.tsx`` that loads some
global CSS, plus the homepage at ``pages/index.tsx`` and a single API route at
``pages/api/hello.ts``. The ``styles`` and ``public`` directories contain some
other assets.

Updating the homepage
---------------------

Let's start by implementing a simple homepage for our blog application using
static data. Replace the contents of ``pages/index.tsx`` with the following.

.. code-block:: tsx

  // pages/index.tsx

  import type {NextPage} from 'next';
  import Head from 'next/head';
  import styles from '../styles/Home.module.css';

  type Post = {
    id: string;
    title: string;
    content: string;
  };

  const HomePage: NextPage = () => {
    const posts: Post[] = [
      {
        id: 'post1',
        title: 'This one weird trick makes using databases fun',
        content: 'Use Gel',
      },
      {
        id: 'post2',
        title: 'How to build a blog with Gel and Next.js',
        content: "Let's start by scaffolding our app with `create-next-app`.",
      },
    ];

    return (
      <div className={styles.container}>
        <Head>
          <title>My Blog</title>
          <meta name="description" content="An awesome blog" />
          <link rel="icon" href="/favicon.ico" />
        </Head>

        <main className={styles.main}>
          <h1 className={styles.title}>Blog</h1>
          <div style={{height: '50px'}}></div>
          {posts.map((post) => {
            return (
              <a href={`/post/${post.id}`} key={post.id}>
                <div className={styles.card}>
                  <p>{post.title}</p>
                </div>
              </a>
            );
          })}
        </main>
      </div>
    );
  };

  export default HomePage;

After saving, Next.js should hot-reload, and the homepage should look
something like this.


.. image::
    /docs/tutorials/nextjs/basic_home.png
    :alt: Basic blog homepage with static content
    :width: 100%

Initializing Gel
----------------

Now let's spin up a database for the app. You have two options to initialize
an Gel project: using ``$ npx gel`` without installing the CLI, or
installing the gel CLI directly. In this tutorial, we'll use the first
option. If you prefer to install the CLI, see the
:ref:`Gel CLI guide <ref_cli_overview>` for more information.
From the application's root directory, run the following command:

.. code-block:: bash

  $ npx gel project init
  No `gel.toml` found in `~/nextjs-blog` or above
  Do you want to initialize a new project? [Y/n]
  > Y
  Specify the name of Gel instance to use with this project [default:
  nextjs_blog]:
  > nextjs_blog
  Checking Gel versions...
  Specify the version of Gel to use with this project [default: x.x]:
  >
  ┌─────────────────────┬──────────────────────────────────────────────┐
  │ Project directory   │ ~/nextjs-blog                                │
  │ Project config      │ ~/nextjs-blog/gel.toml                       │
  │ Schema dir (empty)  │ ~/nextjs-blog/dbschema                       │
  │ Installation method │ portable package                             │
  │ Start configuration │ manual                                       │
  │ Version             │ x.x                                          │
  │ Instance name       │ nextjs_blog                                  │
  └─────────────────────┴──────────────────────────────────────────────┘
  Initializing Gel instance...
  Applying migrations...
  Everything is up to date. Revision initial.
  Project initialized.

This process has spun up an Gel instance called ``nextjs-blog`` and
"linked" it with your current directory. As long as you're inside that
directory, CLI commands and client libraries will be able to connect to the
linked instance automatically, without additional configuration.

To test this, run the |gelcmd| command to open a REPL to the linked instance.

.. code-block:: bash

  $ gel
  Gel x.x (repl x.x)
  Type \help for help, \quit to quit.
  gel> select 2 + 2;
  {4}
  >

From inside this REPL, we can execute EdgeQL queries against our database. But
there's not much we can do currently, since our database is schemaless. Let's
change that.

The project initialization process also created a new subdirectory in our
project called ``dbschema``. This is folder that contains everything
pertaining to Gel. Currently it looks like this:

.. code-block::

  dbschema
  ├── default.gel
  └── migrations

The :dotgel:`default` file will contain our schema. The ``migrations``
directory is currently empty, but will contain our migration files. Let's
update the contents of :dotgel:`default` with the following simple blog schema.

.. code-block:: sdl

  # dbschema/default.gel

  module default {
    type BlogPost {
      required property title -> str;
      required property content -> str {
        default := ""
      };
    }
  }

.. note::

  Gel lets you split up your schema into different ``modules`` but it's
  common to keep your entire schema in the ``default`` module.

Save the file, then let's create our first migration.

.. code-block:: bash

  $ npx gel migration create
  did you create object type 'default::BlogPost'? [y,n,l,c,b,s,q,?]
  > y
  Created ./dbschema/migrations/00001.edgeql

The ``dbschema/migrations`` directory now contains a migration file called
``00001.edgeql``. Currently though, we haven't applied this migration against
our database. Let's do that.

.. code-block:: bash

  $ npx gel migrate
  Applied m1fee6oypqpjrreleos5hmivgfqg6zfkgbrowx7sw5jvnicm73hqdq (00001.edgeql)

Our database now has a schema consisting of the ``BlogPost`` type. We can
create some sample data from the REPL. Run the |gelcmd| command to re-open
the REPL.

.. code-block:: bash

  $ gel
  Gel x.x (repl x.x)
  Type \help for help, \quit to quit.
  gel>


Then execute the following ``insert`` statements.

.. code-block:: edgeql-repl

  gel> insert BlogPost {
  ....   title := "This one weird trick makes using databases fun",
  ....   content := "Use Gel"
  .... };
  {default::BlogPost {id: 7f301d02-c780-11ec-8a1a-a34776e884a0}}
  gel> insert BlogPost {
  ....   title := "How to build a blog with Gel and Next.js",
  ....   content := "Let's start by scaffolding our app..."
  .... };
  {default::BlogPost {id: 88c800e6-c780-11ec-8a1a-b3a3020189dd}}


Loading posts with an API route
-------------------------------

Now that we have a couple posts in the database, let's load them dynamically
with a Next.js `API route <https://nextjs.org/docs/api-routes/introduction>`_.
To do that, we'll need the ``gel`` client library. Let's install that from
NPM:

.. code-block:: bash

  $ npm install gel

Then create a new file at ``pages/api/post.ts`` and copy in the following code.

.. code-block:: typescript

  // pages/api/post.ts

  import type {NextApiRequest, NextApiResponse} from 'next';
  import {createClient} from 'gel';

  export const client = createClient();

  export default async function handler(
    req: NextApiRequest,
    res: NextApiResponse
  ) {
    const posts = await client.query(`select BlogPost {
      id,
      title,
      content
    };`);
    res.status(200).json(posts);
  }

This file initializes an Gel client, which manages a pool of connections to
the database and provides an API for executing queries. We're using the
``.query()`` method to fetch all the posts in the database with a simple
``select`` statement.

If you visit `localhost:3000/api/post <http://localhost:3000/api/post>`_ in
your browser, you should see a plaintext JSON representation of the blog posts
we inserted earlier.

To fetch these from the homepage, we'll use ``useState``, ``useEffect``, and
the built-in ``fetch`` API. At the top of the ``HomePage`` component in
``pages/index.tsx``, replace the static data and add the missing imports.

.. code-block:: tsx-diff

     // pages/index.tsx
  +  import {useState, useEffect} from 'react';

     type Post = {
       id: string;
       title: string;
       content: string;
     };

     const HomePage: NextPage = () => {
  -    const posts: Post[] = [
  -      {
  -        id: 'post1',
  -        title: 'This one weird trick makes using databases fun',
  -        content: 'Use Gel',
  -      },
  -      {
  -        id: 'post2',
  -        title: 'How to build a blog with Gel and Next.js',
  -        content: "Let's start by scaffolding our app...",
  -      },
  -    ];

  +    const [posts, setPosts] = useState<Post[] | null>(null);
  +    useEffect(() => {
  +      fetch(`/api/post`)
  +        .then((result) => result.json())
  +        .then(setPosts);
  +    }, []);
  +    if (!posts) return <p>Loading...</p>;

       return <div>...</div>;
     }

When you refresh the page, you should briefly see a ``Loading...`` indicator
before the homepage renders the (dynamically loaded!) blog posts.

Generating the query builder
----------------------------

Since we're using TypeScript, it makes sense to use Gel's powerful query
builder. This provides a schema-aware client API that makes writing strongly
typed EdgeQL queries easy and painless. The result type of our queries will be
automatically inferred, so we won't need to manually type something like
``type Post = { id: string; ... }``.

First, install the generator to your project.

.. code-block:: bash

  $ yarn add --dev @gel/generate

Then generate the query builder with the following command.

.. code-block:: bash

  $ npx @gel/generate edgeql-js
  Generating query builder...
  Detected tsconfig.json, generating TypeScript files.
     To override this, use the --target flag.
     Run `npx @gel/generate --help` for full options.
  Introspecting database schema...
  Writing files to ./dbschema/edgeql-js
  Generation complete! 🤘
  Checking the generated query builder into version control
  is not recommended. Would you like to update .gitignore to ignore
  the query builder directory? The following line will be added:

     dbschema/edgeql-js

  [y/n] (leave blank for "y")
  > y


This command introspected the schema of our database and generated some code
in the ``dbschema/edgeql-js`` directory. It also asked us if we wanted to add
the generated code to our ``.gitignore``; typically it's not good practice to
include generated files in version control.

Back in ``pages/api/post.ts``, let's update our code to use the query builder
instead.

.. code-block:: typescript-diff

    // pages/api/post.ts

    import type {NextApiRequest, NextApiResponse} from 'next';
    import {createClient} from 'gel';
  + import e, {$infer} from '../../dbschema/edgeql-js';

    export const client = createClient();

  + const selectPosts = e.select(e.BlogPost, () => ({
  +   id: true,
  +   title: true,
  +   content: true,
  + }));

  + export type Posts = $infer<typeof selectPosts>;

    export default async function handler(
      req: NextApiRequest,
      res: NextApiResponse
    ) {
  -   const posts = await client.query(`select BlogPost {
  -     id,
  -     title,
  -     content
  -   };`);
  +   const posts = await selectPosts.run(client);
      res.status(200).json(posts);
    }

Instead of writing our query as a plain string, we're now using the query
builder to declare our query in a code-first way. As you can see we import the
query builder as a single default import ``e`` from the ``dbschema/edgeql-js``
directory.

We're also using a utility called ``$infer`` to extract the inferred type of
this query. In VSCode you can hover over ``Posts`` to see what this type is.

.. image::
    /docs/tutorials/nextjs/inference.png
    :alt: Inferred type of posts query
    :width: 100%

Back in ``pages/index.tsx``, let's update our code to use the inferred
``Posts`` type instead of our manual type declaration.

.. code-block:: typescript-diff

     // pages/index.tsx

     import type {NextPage} from 'next';
     import Head from 'next/head';
     import {useEffect, useState} from 'react';
     import styles from '../styles/Home.module.css';
  +  import {Posts} from "./api/post";

  -  type Post = {
  -    id: string;
  -    title: string;
  -    content: string;
  -  };

     const Home: NextPage = () => {

  +    const [posts, setPosts] = useState<Posts | null>(null);
       // ...

     }

Now, when we update our ``selectPosts`` query, the type of our dynamically
loaded ``posts`` variable will update automatically—no need to keep
our type definitions in sync with our API logic!

Rendering blog posts
--------------------

Our homepage renders a list of links to each of our blog posts, but we haven't
implemented the page that actually displays the posts. Let's create a new page
at ``pages/post/[id].tsx``. This is a
`dynamic route <https://nextjs.org/docs/routing/dynamic-routes>`_ that
includes an ``id`` URL parameter. We'll use this parameter to fetch the
appropriate post from the database.

Create ``pages/post/[id].tsx`` and add the following code. We're using
``getServerSideProps`` to load the blog post data server-side, to avoid
loading spinners and ensure the page loads fast.

.. code-block:: tsx

  import React from 'react';
  import {GetServerSidePropsContext, InferGetServerSidePropsType} from 'next';

  import {client} from '../api/post';
  import e from '../../dbschema/edgeql-js';

  export const getServerSideProps = async (
    context?: GetServerSidePropsContext
  ) => {
    const post = await e
      .select(e.BlogPost, (post) => ({
        id: true,
        title: true,
        content: true,
        filter_single: e.op(
          post.id,
          '=',
          e.uuid(context!.params!.id as string)
        ),
      }))
      .run(client);
    return {props: {post: post!}};
  };

  export type GetPost = InferGetServerSidePropsType<typeof getServerSideProps>;

  const Post: React.FC<GetPost> = (props) => {
    return (
      <div
        style={{
          margin: 'auto',
          width: '100%',
          maxWidth: '600px',
        }}
      >
        <h1 style={{padding: '50px 0px'}}>{props.post.title}</h1>
        <p style={{color: '#666'}}>{props.post.content}</p>
      </div>
    );
  };

  export default Post;


Inside ``getServerSideProps`` we're extracting the ``id`` parameter from
``context.params`` and using it in our EdgeQL query. The query is a ``select``
query that fetches the ``id``, ``title``, and ``content`` of the post with a
matching ``id``.

We're using Next's ``InferGetServerSidePropsType`` utility to extract the
inferred type of our query and pass it into ``React.FC``. Now, if we update
our query, the type of the component props will automatically update too. In
fact, this entire application is end-to-end typesafe.

Now, click on one of the blog post links on the homepage. This should bring
you to ``/post/<uuid>``, which should display something like this:

.. image::
    /docs/tutorials/nextjs/post.png
    :alt: Basic blog homepage with static content
    :width: 100%

Deploying to Vercel
-------------------

**#1 Deploy Gel**

First deploy an Gel instance on your preferred cloud provider:

- :ref:`AWS <ref_guide_deployment_aws_aurora_ecs>`
- :ref:`Azure <ref_guide_deployment_azure_flexibleserver>`
- :ref:`DigitalOcean <ref_guide_deployment_digitalocean>`
- :ref:`Fly.io <ref_guide_deployment_fly_io>`
- :ref:`Google Cloud <ref_guide_deployment_gcp>`
- :ref:`Heroku <ref_guide_deployment_heroku>`

or use a cloud-agnostic deployment method:

- :ref:`Docker <ref_guide_deployment_docker>`
- :ref:`Bare metal <ref_guide_deployment_bare_metal>`

**#2. Find your instance's DSN**

The DSN is also known as a connection string. It will have the format
:geluri:`username:password@hostname:port`. The exact instructions for this
depend on which cloud you are deploying to.

**#3 Apply migrations**

Use the DSN to apply migrations against your remote instance.

.. code-block:: bash

  $ npx gel migrate --dsn <your-instance-dsn> --tls-security insecure

.. note::

  You have to disable TLS checks with ``--tls-security insecure``. All Gel
  instances use TLS by default, but configuring it is out of scope of this
  project.

Once you've applied the migrations, consider creating some sample data in your
database. Open a REPL and ``insert`` some blog posts:

.. code-block:: bash

  $ npx gel --dsn <your-instance-dsn> --tls-security insecure
  Gel x.x (repl x.x)
  Type \help for help, \quit to quit.
  gel> insert BlogPost { title := "Test post" };
  {default::BlogPost {id: c00f2c9a-cbf5-11ec-8ecb-4f8e702e5789}}


**#4 Set up a `prebuild` script**

Add the following ``prebuild`` script to your ``package.json``. When Vercel
initializes the build, it will trigger this script which will generate the
query builder. The ``npx @gel/generate edgeql-js`` command will read the
value of the :gelenv:`DSN` variable, connect to the database, and generate the
query builder before Vercel starts building the project.

.. code-block:: javascript-diff

    // package.json
    "scripts": {
      "dev": "next dev",
      "build": "next build",
      "start": "next start",
      "lint": "next lint",
  +   "prebuild": "npx @gel/generate edgeql-js"
    },

**#5 Deploy to Vercel**

Deploy this app to Vercel with the button below.

.. XXX -- update URL
.. lint-off

.. image:: https://vercel.com/button
  :width: 150px
  :target: https://vercel.com/new/git/external?repository-url=https://github.com/geldata/gel-examples/tree/main/nextjs-blog&project-name=nextjs-edgedb-blog&repository-name=nextjs-edgedb-blog&env=EDGEDB_DSN,EDGEDB_CLIENT_TLS_SECURITY

.. lint-on

When prompted:

- Set :gelenv:`DSN` to your database's DSN
- Set :gelenv:`CLIENT_TLS_SECURITY` to ``insecure``. This will disable
  Gel's default TLS checks; configuring TLS is beyond the scope of this
  tutorial.

.. image::
    /docs/tutorials/nextjs/env.png
    :alt: Setting environment variables in Vercel
    :width: 100%


**#6 View the application**

Once deployment has completed, view the application at the deployment URL
supplied by Vercel.

Wrapping up
-----------

Admittedly this isn't the prettiest blog of all time, or the most
feature-complete. But this tutorial demonstrates how to work with Gel in a
Next.js app, including data fetching with API routes and
``getServerSideProps``.

The next step is to add a ``/newpost`` page with a form for writing new blog
posts and saving them into Gel. That's left as an exercise for the reader.

To see the final code for this tutorial, refer to
`github.com/geldata/gel-examples/tree/main/nextjs-blog
<https://github.com/geldata/gel-examples/tree/main/nextjs-blog>`_.
