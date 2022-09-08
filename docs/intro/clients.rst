.. _ref_intro_clients:

================
Client Libraries
================

To execute queries from your application code, use one of EdgeDB's *client
libraries* for the following languages.

- `JavaScript/TypeScript <https://github.com/edgedb/edgedb-js>`_
- `Go <https://github.com/edgedb/edgedb-go>`_
- `Python <https://github.com/edgedb/edgedb-python>`_
- `Rust <https://github.com/edgedb/edgedb-rust>`_
- `.NET <https://github.com/quinchs/edgedb-dotnet>`_ (unofficial)

Walkthrough
-----------

To follow along with the guide below, first create a new directory and
initialize a project.

.. code-block::

  $ mydir myproject
  $ cd myproject
  $ edgedb project init

Configure the environment as needed for your preferred language.

.. tabs::

  .. code-tab:: bash#Node.js

    $ npm init -y
    $ tsc --init # (TypeScript only)
    $ touch index.ts

  .. code-tab:: txt#Deno

    $ touch index.ts

  .. code-tab:: txt#Python

    $ python -m venv venv
    $ source venv/bin/activate
    $ touch main.py

  .. code-tab:: bash#Rust

    $ cargo init

  .. code-tab:: bash#Go

    $ go mod init example/quickstart
    $ touch hello.go

  ..   code-tab:: bash#.NET

    $ dotnet new console -o . -f net6.0

Install the EdgeDB client library.

.. tabs::

  .. code-tab:: bash#Node.js

    $ npm install edgedb
    # or
    $ yarn add edgedb

  .. code-tab:: txt#Deno

    n/a

  .. code-tab:: txt#Python

    $ pip install edgedb

  .. code-tab:: toml#Rust

    # Cargo.toml

    [dependencies]
    edgedb-tokio = "0.3.0"
    # additional dependencies
    tokio = { version = "1", features = ["full"] }
    anyhow = "1.0.63"

  .. code-tab:: bash#Go

    $ go get github.com/edgedb/edgedb-go

  .. code-tab:: bash#.NET

    $ dotnet add package EdgeDB.Net.Driver

Copy and paste the following simple script. This script initializes a
``Client`` instance. Clients manage an internal pool of connections to your
database and provide a set of methods for executing queries.

.. note::

  Note that we aren't passing connection information (say, a connection
  URL) when creating a client. The client libraries can detect that
  they are inside a project directory and connect to the project-linked
  instance automatically. (More on this later.)

.. tabs::

  .. code-tab:: typescript#Node.js

    import {createClient} from 'edgedb';

    const client = createClient();

    const result = await client.querySingle(`select random()`);
    console.log(result);

  .. code-tab:: typescript#Deno

    import {createClient} from 'https://deno.land/x/edgedb';

    const client = createClient();

    const result = await client.querySingle(`select random()`);
    console.log(result);

  .. code-tab:: python#Python

    from edgedb import create_client

    client = create_client()

    result = client.query_single("select random()")
    print(result)

  .. code-tab:: rust#Rust

    // src/main.rs
    #[tokio::main]
    async fn main() -> anyhow::Result<()> {
        let conn = edgedb_tokio::create_client().await?;
        let val = conn
            .query_required_single::<f64, _>("select random()", &())
            .await?;
        println!("Result: {}", val);
        Ok(())
    }

  .. code-tab:: go#Go

    // hello.go
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

      var result float64
      err = client.
        QuerySingle(ctx, "select random();", &result)
      if err != nil {
        log.Fatal(err)
      }

      fmt.Println(result)
    }

  .. code-tab:: dotnet#.NET

    using EdgeDB;

    var client = new EdgeDBClient();

    var result = await client.QuerySingleAsync<double>("select random();");
    Console.WriteLine(result);

Finally, execute the file.

.. tabs::

  .. code-tab:: bash#Node.js

    $ npx tsx index.ts

  .. code-tab:: txt#Deno

    $ deno run --allow-all --unstable index.deno.ts

  .. code-tab:: txt#Python

    $ python index.p

  .. code-tab:: toml#Rust

    cargo run

  .. code-tab:: bash#Go

    $ go run .

  .. code-tab:: bash#.NET

    $ dotnet run

You should see a random number get printed to the console. This number was
generated inside your EdgeDB instance using the built-in ``random()``
function.


Connection
----------

All client libraries (and the EdgeDB CLI) implement a standard protocol for
determining how to connect to your instance. Below is the breakdown of the
most common approaches.

Using projects
^^^^^^^^^^^^^^

**Development only** In development, we recommend :ref:`initializing a project
<ref_intro_projects>` in the root of your codebase.

.. code-block:: bash

  $ edgedb project init


Once the project is initialized, any code that uses an official client library
will automatically connect to the project-linked instance—no need for
environment variables or hard-coded credentials. Follow the :ref:`Using
projects <ref_guide_using_projects>` guide to get started.

Using ``EDGEDB_DSN``
^^^^^^^^^^^^^^^^^^^^

In production, connection information can be securely passed to the client
library via environment variables. All official client libraries will read the
following set of variables to determine how to connect.

Most commonly, you pass a value for ``EDGEDB_DSN``. A DSN is also known as a
"connection string" and takes the following form.

.. code-block::

  edgedb://<username>:<password>@<hostname>:<port>

For instance, a sample DSN may look like this:

.. code-block::

  edgedb://username:pas$$word@db.domain.com:8080

Each element of the DSN is optional; in fact ``edgedb://`` is a technically a
valid DSN. Any unspecified element will default to the following values.

.. code-block::

  Host             "localhost"
  Port             5656
  User             "edgedb"
  Password         null
  Database         "edgedb"

DSNs can also contain the following query parameters.

.. list-table::

  * - ``database``
    - The database to connect to within the given instance. Defaults to
      ``edgedb``.

      .. code-block::

        edgedb://user:pass@example.com:8080?database=my_db

  * - ``tls_security``
    - The TLS security mode.

      - ``"strict"`` (**default**) — verify certificates and hostnames
      - ``"no_host_verification"`` — verify certificates only
      - ``"insecure"`` — trust self-signed certificates

      .. code-block::

        edgedb://user:pass@example.com:8080?tls_security=insecure
  * - ``tls_ca_file``
    - A path pointing to a CA root certificate. This is usually needed
      when your remote instance is using self-signed certificates.

      .. code-block::

        edgedb://user:pass@example.com:8080?tls_ca_file=/path/to/server.crt


For a more comprehensive guide to DSNs, see the :ref:`DSN Specification
<ref_dsn>`.

Using separate environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If needed for your deployment pipeline, each element of the DSN can be
specified independently.

- ``EDGEDB_HOST``
- ``EDGEDB_PORT``
- ``EDGEDB_DATABASE``
- ``EDGEDB_USER``
- ``EDGEDB_PASSWORD``
- ``EDGEDB_TLS_CA_FILE``
- ``EDGEDB_CLIENT_TLS_SECURITY``

.. note::

  If a value for ``EDGEDB_DSN`` is defined, it will override these variables!

Other mechanisms
^^^^^^^^^^^^^^^^

``EDGEDB_CREDENTIALS_FILE``
  A path to a ``.json`` file containing connection information. In some
  scenarios (including local Docker development) its useful to represent
  connection information with files.

  .. code-block:: json

    {
      "host": "localhost",
      "port": 10700,
      "user": "testuser",
      "password": "testpassword",
      "database": "edgedb",
      "tls_cert_data": "-----BEGIN CERTIFICATE-----\nabcdef..."
    }

``EDGEDB_INSTANCE`` (local only)
  The name of a local instance. Only useful in development.


The value of ``EDGEDB_DSN`` can also be an :ref:`instance name
<ref_reference_connection_instance_name>`. You can create new instances
manually with the :ref:`edgedb instance create
<ref_cli_edgedb_instance_create>` command.

These are the most common ways to connect to an instance, however EdgeDB
supports several other options for advanced use cases. For a complete reference
on connection configuration, see :ref:`Reference > Connection Parameters
<ref_reference_connection>`.
