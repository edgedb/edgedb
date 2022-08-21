.. _ref_intro_clients:

===============
Running queries
===============

EdgeDB provides client libraries for the following languages.

.. list-table::

  * - **Language**
    - **Client**
  * - TypeScript
    - `TypeScript/Javascript <js/index>`_
  * - Python
    - `Python <python/index>`_
  * - Go
    - `Go <go/index>`_
  * - Rust
    - `Rust <rust/index>`_
  * - .NET (community-maintained)
    - `.NET <https://github.com/quinchs/EdgeDB.Net>`_
  * - Elixir (community-maintained)
    - `Elixir <https://github.com/nsidnev/edgedb-elixir>`_

There are a few core concepts that are common to all libraries.

Installation
------------

.. list-table::

  * - **Language**
    - **Client**
  * - TypeScript
    - ``npm install edgedb``
      ``yarn add edgedb``
  * - Python
    - ``pip install edgedb``
  * - Go
    - ``go get github.com/edgedb/edgedb-go``
  * - Rust
    - ``cargo add edgedb-tokio``
  * - .NET (community-maintained)
    - ``dotnet add package EdgeDB.Net.Driver``
  * - Elixir (community-maintained)
    - Add ``:edgedb`` to ``mix.ecs``


Clients
-------

Each library provides a way to initialize a *Client*. Clients manage a
pool of connections to your database and provide a set of methods for
executing queries.

.. tabs::

  .. code-tab:: typescript

    import {createClient} from "edgedb";

    const client = createClient();

    const result = await client.querySingle(`select "Hello world!"`);

  .. code-tab:: python

    from edgedb import create_client

    client = create_client()

    result = client.query("""
        select "Hello world!";
    """)

  .. code-tab:: go

    package main

    import (
      "context"
      "fmt"
      "log"

      "github.com/edgedb/edgedb-go"
    )

    func main() {
      ctx := context.Background()
      client, err := edgedb.CreateClient(ctx, edgedb.Options{})
      if err != nil {
        log.Fatal(err)
      }
      defer client.Close()

      var result string
      err = client.
        QuerySingle(ctx, "SELECT 'Hello world!';", &result)
      if err != nil {
        log.Fatal(err)
      }

      fmt.Println(result)
    }


Connection
----------

There are a couple ways to provide connection information to a client
library.

- Use **projects**. This is the recommended approach for *local
  development*. Once the project is initialized, all client libraries that are
  running inside the project directory will automatically connect to the
  project-linked instance—no need for environment variables or hard-coded
  credentials. Follow the :ref:`Using projects <ref_guide_using_projects>`
  guide to get started.

- Set the ``EDGEDB_DSN`` environment to a valid DSN (connection string). This
  is the recommended approach in *production*. A DSN is a
  connection URL of the form ``edgedb://user:pass@host:port/database``. For a
  guide to DSNs, see the :ref:`DSN Specification <ref_dsn>`.

  The value of ``EDGEDB_DSN`` can also be an :ref:`instance name
  <ref_reference_connection_instance_name>`. You can create new instances
  manually with the :ref:`edgedb instance create
  <ref_cli_edgedb_instance_create>` command.

- Hard-code the credentials (not recommended.) Pass a DSN or
  :ref:`instance name <ref_reference_connection_instance_name>`
  directly when initializing a client.

  .. code-block:: typescript

    const client = edgedb.createClient({
      dsn: "edgedb://..."
    });

  This approach recommended to include
  sensitive information hard-coded in your production source code. Use
  environment variables instead. Different languages, frameworks, cloud hosting
  providers, and container-based workflows each provide various mechanisms for
  setting environment variables.

These are the most common ways to connect to an instance, however EdgeDB
supports several other options for advanced use cases. For a complete reference
on connection configuration, see :ref:`Reference > Connection Parameters
<ref_reference_connection>`.


