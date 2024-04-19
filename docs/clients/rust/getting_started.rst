.. _ref_rust_getting_started:

===============
Getting started
===============

From examples repo
==================

If you just want a working repo to get started, clone the Rust client 
`examples repo`_, type ``edgedb project init`` to start an EdgeDB
project, and then ``cargo run`` to run the samples.

This tutorial contains a lot of similar examples to those found in the
``main.rs`` file inside that repo.

From scratch
============

The minimum to add to your Cargo.toml to use the client is `edgedb-tokio`_:

.. code-block:: toml

  edgedb-tokio = "0.5.0"

The next most common dependency is `edgedb-protocol`_, which includes the
EdgeDB types used for data modeling:

.. code-block:: toml

  edgedb-protocol = "0.6.0"

A third crate called `edgedb-derive`_ contains the ``#[derive(Queryable)]``
derive macro which is the main way to unpack EdgeDB output into Rust types:

.. code-block:: toml

  edgedb-derive = "0.5.1"
    
The Rust client uses tokio so add this to Cargo.toml as well:

.. code-block:: toml
    
  tokio = { version = "1.29.1", features = ["macros", "rt-multi-thread"] }

If you are avoiding async code and want to emulate a blocking client, you will
still need to use tokio as a dependency but can bridge with async using one of
the `bridging methods`_ recommended by tokio. This won't require any
added features:

.. code-block:: toml
  
  tokio = "1.29.1"

Then you can start a runtime. Block and wait for futures to resolve by calling
the runtime's ``.block_on()`` method:

.. code-block:: rust

  let rt = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()?;
  let just_a_string: String =
      rt.block_on(client.query_required_single("select 'A string'", &()))?;

Edgedb project setup
====================

The EdgeDB CLI initializes an EdgeDB project with a single command in the same
way that Cargo initializes a Rust project, except it does not create a 
new directory. So to start a project: 

- Use ``cargo new <your_crate_name>`` as usual, then:
- Go into the directory and type ``edgedb project init``.

The CLI will prompt you for the instance name and version of EdgeDB to use.
It will look something like this:

.. code-block:: powershell

  PS> edgedb project init
  No `edgedb.toml` found in `\\?\C:\rust\my_db` or above
  Do you want to initialize a new project? [Y/n]
  > Y
  Specify the name of EdgeDB instance to use 
  with this project [default: my_db]:
  > my_db
  Checking EdgeDB versions...
  Specify the version of EdgeDB to use with this project [default: 3.0]:
  > 3.0
  ┌─────────────────────┬─────────────────────────────────┐
  │ Project directory   │ \\?\C:\rust\my_db               │
  │ Project config      │ \\?\C:\rust\my_db\edgedb.toml   │
  │ Schema dir (empty)  │ \\?\C:\rust\my_db\dbschema      │
  │ Installation method │ WSL                             │
  │ Version             │ 3.0+e7d38e9                     │
  │ Instance name       │ my_db                           │
  └─────────────────────┴─────────────────────────────────┘
  Version 3.0+e7d38e9 is already installed
  Initializing EdgeDB instance...
  Applying migrations...
  Everything is up to date. Revision initial
  Project initialized.
  To connect to my_db, run `edgedb`

Inside your project directory you'll notice some new items:

- ``edgedb.toml``, which is used to mark the directory as an EdgeDB project.

The file itself doesn't contain much — just the version of EdgeDB being 
used — but is used by the CLI to run commands without connection flags. 
(E.g., ``edgedb -I my_project migrate`` becomes simply ``edgedb migrate``).
See more in our :ref:`edgedb.toml reference <ref_reference_edgedb_toml>` or on
the `blog post introducing the EdgeDB projects CLI`_.

- A ``/dbschema`` folder, inside which you'll see:

  - a ``default.esdl`` file which holds your schema. You can change the schema
    by directly modifying this file followed by ``edgedb migration create`` 
    and ``edgedb migrate``.

  - a ``/migrations`` folder with ``.edgeql`` files named starting at 
    ``00001``. These hold the :ref:`ddl <ref_eql_ddl>` commands that were used
    to migrate your schema. A new file shows up in this directory every time
    your schema is migrated.

If you are running EdgeDB 3.0 and above, you also have the option of using 
the :ref:`edgedb watch <ref_cli_edgedb_watch>` command. Doing so starts a
long-running process that keeps an eye on changes in ``/dbschema``,
automatically applying these changes in real time.

Now that you have the right dependencies and an EdgeDB instance, 
you can create a client.

.. _`blog post introducing the EdgeDB projects CLI`:
    https://www.edgedb.com/blog/introducing-edgedb-projects
.. _`bridging methods`: https://tokio.rs/tokio/topics/bridging
.. _`edgedb-derive`: https://docs.rs/edgedb-derive/latest/edgedb_derive/
.. _`edgedb-protocol`: https://docs.rs/edgedb-protocol/latest/edgedb_protocol
.. _`edgedb-tokio`: https://docs.rs/edgedb-tokio/latest/edgedb_tokio
.. _`examples repo`: https://github.com/Dhghomon/edgedb_rust_client_examples