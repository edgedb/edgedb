.. _ref_intro_clients:

================
Client Libraries
================

EdgeDB implements libraries for popular languages that make it easier to work
with EdgeDB. These libraries provide a common set of functionality.

- *Instantiating clients.* Most libraries implement a ``Client`` class that
  internally manages a pool of physical connections to your EdgeDB instance.
- *Resolving connections.* All client libraries implement a standard protocol
  for determining how to connect to your database. In most cases, this will
  involve checking for special environment variables like ``EDGEDB_DSN`` or, in
  the case of EdgeDB Cloud instances, ``EDGEDB_INSTANCE`` and
  ``EDGEDB_SECRET_KEY``.
  (More on this in :ref:`the Connection section below
  <ref_intro_clients_connection>`.)
- *Executing queries.* A ``Client`` will provide some methods for executing
  queries against your database. Under the hood, this query is executed using
  EdgeDB's efficient binary protocol.

.. note::

  For some use cases, you may not need a client library. EdgeDB allows you to
  execute :ref:`queries over HTTP <ref_edgeql_http>`. This is slower than the
  binary protocol and lacks support for transactions and rich data types, but
  may be suitable if a client library isn't available for your language of
  choice.

Available libraries
===================

To execute queries from your application code, use one of EdgeDB's *client
libraries* for the following languages.

- :ref:`JavaScript/TypeScript <edgedb-js-intro>`
- :ref:`Go <edgedb-go-intro>`
- :ref:`Python <edgedb-python-intro>`
- :ref:`Rust <ref_rust_index>`
- :ref:`C# and F# <edgedb-dotnet-intro>`
- :ref:`Java <edgedb-java-intro>`
- :ref:`Dart <edgedb-dart-intro>`
- :ref:`Elixir <edgedb-elixir-intro>`

Usage
=====

To follow along with the guide below, first create a new directory and
initialize a project.

.. code-block:: bash

  $ mydir myproject
  $ cd myproject
  $ edgedb project init

Configure the environment as needed for your preferred language.

.. tabs::

  .. code-tab:: bash
    :caption: Node.js

    $ npm init -y
    $ tsc --init # (TypeScript only)
    $ touch index.ts

  .. code-tab:: bash
    :caption: Deno

    $ touch index.ts

  .. code-tab:: bash
    :caption: Python

    $ python -m venv venv
    $ source venv/bin/activate
    $ touch main.py

  .. code-tab:: bash
    :caption: Rust

    $ cargo init

  .. code-tab:: bash
    :caption: Go

    $ go mod init example/quickstart
    $ touch hello.go

  .. code-tab:: bash
    :caption: .NET

    $ dotnet new console -o . -f net6.0

  .. code-tab:: bash
    :caption: Maven (Java)

    $ touch Main.java

  .. code-tab:: bash
    :caption: Gradle (Java)

    $ touch Main.java

  .. code-tab:: bash
    :caption: Elixir

    $ mix new edgedb_quickstart

Install the EdgeDB client library.

.. tabs::

  .. code-tab:: bash
    :caption: Node.js

    $ npm install edgedb    # npm
    $ yarn add edgedb       # yarn

  .. code-tab:: txt
    :caption: Deno

    n/a

  .. code-tab:: bash
    :caption: Python

    $ pip install edgedb

  .. code-tab:: toml
    :caption: Rust

    # Cargo.toml

    [dependencies]
    edgedb-tokio = "0.5.0"
    # Additional dependency
    tokio = { version = "1.28.1", features = ["macros", "rt-multi-thread"] }

  .. code-tab:: bash
    :caption: Go

    $ go get github.com/edgedb/edgedb-go

  .. code-tab:: bash
    :caption: .NET

    $ dotnet add package EdgeDB.Net.Driver

  .. code-tab:: xml
    :caption: Maven (Java)

    // pom.xml
    <dependency>
        <groupId>com.edgedb</groupId>
        <artifactId>driver</artifactId>
    </dependency>

  .. code-tab::
    :caption: Gradle (Java)

    // build.gradle
    implementation 'com.edgedb:driver'

  .. code-tab:: elixir
    :caption: Elixir

    # mix.exs
    {:edgedb, "~> 0.6.0"}

Copy and paste the following simple script. This script initializes a
``Client`` instance. Clients manage an internal pool of connections to your
database and provide a set of methods for executing queries.

.. note::

  Note that we aren't passing connection information (say, a connection
  URL) when creating a client. The client libraries can detect that
  they are inside a project directory and connect to the project-linked
  instance automatically. For details on configuring connections, refer
  to the :ref:`Connection <ref_intro_clients_connection>` section below.

.. lint-off

.. tabs::

  .. code-tab:: typescript
    :caption: Node.js

    import {createClient} from 'edgedb';

    const client = createClient();

    client.querySingle(`select random()`).then((result) => {
      console.log(result);
    });


  .. code-tab:: typescript
    :caption: Deno

    import {createClient} from 'https://deno.land/x/edgedb/mod.ts';

    const client = createClient();

    const result = await client.querySingle(`select random()`);
    console.log(result);

  .. code-tab:: python

    from edgedb import create_client

    client = create_client()

    result = client.query_single("select random()")
    print(result)

  .. code-tab:: rust

    // src/main.rs
    #[tokio::main]
    async fn main() {
        let conn = edgedb_tokio::create_client()
            .await
            .expect("Client initiation");
        let val = conn
            .query_required_single::<f64, _>("select random()", &())
            .await
            .expect("Returning value");
        println!("Result: {}", val);
    }

  .. code-tab:: go

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

  .. code-tab:: csharp
    :caption: .NET

    using EdgeDB;

    var client = new EdgeDBClient();
    var result = await client.QuerySingleAsync<double>("select random();");
    Console.WriteLine(result);

  .. code-tab:: java
    :caption: Futures (Java)

    import com.edgedb.driver.EdgeDBClient;
    import java.util.concurrent.CompletableFuture;

    public class Main {
        public static void main(String[] args) {
            var client = new EdgeDBClient();

            client.querySingle(String.class, "select random();")
                .thenAccept(System.out::println)
                .toCompletableFuture().get();
        }
    }

  .. code-tab:: java
    :caption: Reactor (Java)

    import com.edgedb.driver.EdgeDBClient;
    import reactor.core.publisher.Mono;

    public class Main {
        public static void main(String[] args) {
            var client = new EdgeDBClient();

            Mono.fromFuture(client.querySingle(String.class, "select random();"))
                .doOnNext(System.out::println)
                .block();
        }
    }

  .. code-tab:: elixir
    :caption: Elixir

    # lib/edgedb_quickstart.ex
    defmodule EdgeDBQuickstart do
      def run do
        {:ok, client} = EdgeDB.start_link()
        result = EdgeDB.query_single!(client, "select random()")
        IO.inspect(result)
      end
    end

.. lint-on


Finally, execute the file.

.. tabs::

  .. code-tab:: bash
    :caption: Node.js

    $ npx tsx index.ts

  .. code-tab:: bash
    :caption: Deno

    $ deno run --allow-all --unstable index.deno.ts

  .. code-tab:: bash
    :caption: Python

    $ python index.py

  .. code-tab:: bash
    :caption: Rust

    $ cargo run

  .. code-tab:: bash
    :caption: Go

    $ go run .

  .. code-tab:: bash
    :caption: .NET

    $ dotnet run

  .. code-tab:: bash
    :caption: Java

    $ javac Main.java
    $ java Main

  .. code-tab:: bash
    :caption: Elixir

    $ mix run -e EdgeDBQuickstart.run

You should see a random number get printed to the console. This number was
generated inside your EdgeDB instance using EdgeQL's built-in
:eql:func:`random` function.

.. _ref_intro_clients_connection:

Connection
==========

All client libraries implement a standard protocol for determining how to
connect to your database.

Using projects
--------------

In development, we recommend :ref:`initializing a
project <ref_intro_projects>` in the root of your codebase.

.. code-block:: bash

  $ edgedb project init

Once the project is initialized, any code that uses an official client library
will automatically connect to the project-linked instance—no need for
environment variables or hard-coded credentials. Follow the :ref:`Using
projects <ref_guide_using_projects>` guide to get started.

Using environment variables
---------------------------

.. _ref_intro_clients_connection_cloud:

For EdgeDB Cloud
^^^^^^^^^^^^^^^^

In production, connection information can be securely passed to the client
library via environment variables. For EdgeDB Cloud instances, the recommended
variables to set are ``EDGEDB_INSTANCE`` and ``EDGEDB_SECRET_KEY``.

Set ``EDGEDB_INSTANCE`` to ``<org-name>/<instance-name>`` where
``<instance-name>`` is the name you set when you created the EdgeDB Cloud
instance.

If you have not yet created a secret key, you can do so in the EdgeDB Cloud UI
or by running :ref:`ref_cli_edgedb_cloud_secretkey_create` via the CLI.

For self-hosted instances
^^^^^^^^^^^^^^^^^^^^^^^^^

Most commonly for self-hosted remote instances, you set a value for the
``EDGEDB_DSN`` environment variable.

.. note::

  If environment variables like ``EDGEDB_DSN`` are defined inside a project
  directory, the environment variables will take precedence.

A DSN is also known as a "connection string" and takes the
following form.

.. code-block::

  edgedb://<username>:<password>@<hostname>:<port>

Each element of the DSN is optional; in fact ``edgedb://`` is a technically a
valid DSN. Any unspecified element will default to the following values.

.. list-table::

  * - ``<host>``
    - ``localhost``
  * - ``<port>``
    - ``5656``
  * - ``<user>``
    - ``edgedb``
  * - ``<password>``
    -  ``null``

A typical DSN may look like this:

.. code-block::

  edgedb://username:pas$$word@db.domain.com:8080

DSNs can also contain the following query parameters.

.. list-table::

  * - ``branch``
    - The database branch to connect to within the given instance. Defaults to
      ``main``.

  * - ``tls_security``
    - The TLS security mode. Accepts the following values.

      - ``"strict"`` (**default**) — verify certificates and hostnames
      - ``"no_host_verification"`` — verify certificates only
      - ``"insecure"`` — trust self-signed certificates

  * - ``tls_ca_file``
    - A filesystem path pointing to a CA root certificate. This is usually only
      necessary when attempting to connect via TLS to a remote instance with a
      self-signed certificate.

These parameters can be added to any DSN using web-standard query string
notation.

.. code-block::

  edgedb://user:pass@example.com:8080?branch=my_branch&tls_security=insecure

For a more comprehensive guide to DSNs, see the :ref:`DSN Specification
<ref_dsn>`.

Using multiple environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If needed for your deployment pipeline, each element of the DSN can be
specified independently.

- ``EDGEDB_HOST``
- ``EDGEDB_PORT``
- ``EDGEDB_USER``
- ``EDGEDB_PASSWORD``
- ``EDGEDB_BRANCH``
- ``EDGEDB_TLS_CA_FILE``
- ``EDGEDB_CLIENT_TLS_SECURITY``

.. note::

  If a value for ``EDGEDB_DSN`` is defined, it will override these variables!

Other mechanisms
----------------

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
      "branch": "main",
      "tls_cert_data": "-----BEGIN CERTIFICATE-----\nabcdef..."
    }

``EDGEDB_INSTANCE`` (local/EdgeDB Cloud only)
  The name of an instance. Useful only for local or EdgeDB Cloud instances.

  .. note::

      For more on EdgeDB Cloud instances, see the :ref:`EdgeDB Cloud instance
      connection section <ref_intro_clients_connection_cloud>` above.

Reference
---------

These are the most common ways to connect to an instance, however EdgeDB
supports several other options for advanced use cases. For a complete reference
on connection configuration, see :ref:`Reference > Connection Parameters
<ref_reference_connection>`.
