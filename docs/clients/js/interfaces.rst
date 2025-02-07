.. _edgedb-js-interfaces:

====================
Interfaces Generator
====================

The ``interfaces`` generator introspects your schema and generates file containing *TypeScript interfaces* that correspond to each object type. This is useful for writing typesafe code to interact with EdgeDB.

Installation
------------

To get started, install the following packages. (If you're using Deno, you can skip this step.)

Install the ``edgedb`` package.

.. code-block:: bash

  $ npm install edgedb       # npm users
  $ yarn add edgedb          # yarn users
  $ bun add edgedb           # bun users

Then install ``@edgedb/generate`` as a dev dependency.

.. code-block:: bash

  $ npm install @edgedb/generate --save-dev      # npm users
  $ yarn add @edgedb/generate --dev              # yarn users
  $ bun add --dev @edgedb/generate               # bun users


Generation
----------

Assume your database contains the following EdgeDB schema.

.. code-block:: sdl

  module default {
    type Person {
      required name: str;
    }

    scalar type Genre extending enum<Horror, Comedy, Drama>;

    type Movie {
      required title: str;
      genre: Genre;
      multi actors: Person;
    }
  }

The following command will run the ``interfaces`` generator.

.. tabs::

  .. code-tab:: bash
    :caption: Node.js

    $ npx @edgedb/generate interfaces

  .. code-tab:: bash
    :caption: Deno

    $ deno run --allow-all --unstable https://deno.land/x/edgedb/generate.ts interfaces

  .. code-tab:: bash
    :caption: Bun

    $ bunx @edgedb/generate interfaces

.. note:: Deno users

    Create these two files in your project root:

    .. code-block:: json
        :caption: importMap.json

        {
          "imports": {
            "edgedb": "https://deno.land/x/edgedb/mod.ts",
            "edgedb/": "https://deno.land/x/edgedb/"
          }
        }

    .. code-block:: json
        :caption: deno.js

        {
          "importMap": "./importMap.json"
        }

This will introspect your schema and generate TypeScript interfaces that correspond to each object type. By default, these interfaces will be written to a single file called ``interfaces.ts`` into the ``dbschema`` directory in your project root. The file will contain the following contents (roughly):

.. code-block:: typescript

  export interface Person {
    id: string;
    name: string;
  }

  export type Genre = "Horror" | "Comedy" | "Drama";

  export interface Movie {
    id: string;
    title: string;
    genre?: Genre | null;
    actors: Person[];
  }

Any types declared in a non-``default`` module  will be generated into an accordingly named ``namespace``.

.. note::

   Generators work by connecting to the database to get information about the current state of the schema. Make sure you run the generators again any time the schema changes so that the generated code is in-sync with the current state of the schema.


Customize file path
~~~~~~~~~~~~~~~~~~~

Pass a ``--file`` flag to specify the output file path.

.. code-block:: bash

  $ npx @edgedb/generate interfaces --file schema.ts

If the value passed as ``--file`` is a relative path, it will be evaluated relative to the current working directory (``process.cwd()``). If the value is an absolute path, it will be used as-is.

.. note::

  Because this generator is TypeScript-specific, the ``--target`` flag is not supported as in other generators.


Version control
~~~~~~~~~~~~~~~

To exclude the generated file, add the following lines to your ``.gitignore`` file.

.. code-block:: text

  dbschema/interfaces.ts

Usage
-----

The generated interfaces can be imported like so.

.. code-block:: typescript

  import {Genre, Movie} from "./dbschema/interfaces";

You will need to manipulate the generated interfaces to match your application's needs. For example, you may wish to strip the ``id`` property for a ``createMovie`` mutation.

.. code-block:: typescript

  function createMovie(data: Omit<Movie, "id">) {
    // ...
  }

.. note::

  Refer to the `TypeScript docs <https://www.typescriptlang.org/docs/handbook/utility-types.html>`_ for information about built-in utility types like ``Pick``, ``Omit``, and ``Partial``.

For convenience, the file also exports a namespace called ``helper`` containing a couple useful utilities for extracting the properties or links from an object type interface.

.. code-block:: typescript

  import {Movie, helper} from "./dbschema/interfaces";

  type MovieProperties = helper.Props<Movie>;
  // { id: string; title: string; ... }

  type MovieLinks = helper.Links<Movie>;
  // { actors: Person[]; }


Enums
~~~~~

Note that an ``enum`` in your schema will be represented in the generated code as a union of string literals.

.. code-block:: typescript

  export type Genre = "Horror" | "Comedy" | "Drama";

We do *not* generate TypeScript enums for a number of reasons.

- In TypeScript, enums are nominally typed. Two identically named enums are not
  considered equal, even if they have the same members.
- Enums are both a runtime and static construct. Hovever, for simplicity we want the ``interfaces`` generator to produce exclusively static (type-level) code.
