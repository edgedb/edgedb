.. versionadded:: 2.0

.. _ref_datamodel_globals:

=======
Globals
=======

Schemas can contain scalar-typed *global variables*.

.. code-block:: sdl
    :version-lt: 3.0

    global current_user_id -> uuid;

.. code-block:: sdl

    global current_user_id: uuid;

These provide a useful mechanism for specifying session-level data that can be
referenced in queries with the ``global`` keyword.

.. code-block:: edgeql

  select User {
    id,
    posts: { title, content }
  }
  filter .id = global current_user_id;


As in the example above, this is particularly useful for representing the
notion of a session or "current user".

Setting global variables
^^^^^^^^^^^^^^^^^^^^^^^^

Global variables are set when initializing a client. The exact API depends on
which client library you're using.

.. tabs::

  .. code-tab:: typescript

    import createClient from 'edgedb';

    const baseClient = createClient();
    // returns a new Client instance that stores the provided 
    // globals and sends them along with all future queries:
    const clientWithGlobals = baseClient.withGlobals({
      current_user_id: '2141a5b4-5634-4ccc-b835-437863534c51',
    });

    await clientWithGlobals.query(`select global current_user_id;`);

  .. code-tab:: python

    from edgedb import create_client

    client = create_client().with_globals({
        'current_user_id': '580cc652-8ab8-4a20-8db9-4c79a4b1fd81'
    })

    result = client.query("""
        select global current_user_id;
    """)
    print(result)

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

      id, err := edgedb.ParseUUID("2141a5b4-5634-4ccc-b835-437863534c51")
      if err != nil {
        log.Fatal(err)
      }

      var result edgedb.UUID
      err = client.
        WithGlobals(map[string]interface{}{"current_user": id}).
        QuerySingle(ctx, "SELECT global current_user;", &result)
      if err != nil {
        log.Fatal(err)
      }

      fmt.Println(result)
    }

  .. code-tab:: rust

    use uuid::Uuid;

    let client = edgedb_tokio::create_client().await.expect("Client init");

    let client_with_globals = client.with_globals_fn(|c| {
        c.set(
            "current_user_id",
            Value::Uuid(
                Uuid::parse_str("2141a5b4-5634-4ccc-b835-437863534c51")
                    .expect("Uuid should have parsed"),
            ),
        )
    });
    let val: Uuid = client_with_globals
        .query_required_single("select global current_user_id;", &())
        .await
        .expect("Returning value");
    println!("Result: {val}");

  .. code-tab:: edgeql

    set global current_user_id := 
      <uuid>'2141a5b4-5634-4ccc-b835-437863534c51';


Cardinality
-----------

Global variables can be marked ``required``; in this case, you must specify a
default value.

.. code-block:: sdl
    :version-lt: 3.0

    required global one_string -> str {
      default := "Hi Mom!"
    };

.. code-block:: sdl

    required global one_string: str {
      default := "Hi Mom!"
    };

Computed globals
----------------

Global variables can also be computed. The value of computed globals are
dynamically computed when they are referenced in queries.

.. code-block:: sdl

  required global random_global := datetime_of_transaction();

The provided expression will be computed at the start of each query in which
the global is referenced. There's no need to provide an explicit type; the
type is inferred from the computed expression.

Computed globals are not subject to the same constraints as non-computed ones;
specifically, they can be object-typed and have a ``multi`` cardinality.

.. code-block:: sdl
    :version-lt: 3.0

    global current_user_id -> uuid;

    # object-typed global
    global current_user := (
      select User filter .id = global current_user_id
    );

    # multi global
    global current_user_friends := (global current_user).friends;

.. code-block:: sdl

    global current_user_id: uuid;

    # object-typed global
    global current_user := (
      select User filter .id = global current_user_id
    );

    # multi global
    global current_user_friends := (global current_user).friends;


Usage in schema
---------------

.. You may be wondering what purpose globals serve that can't.
.. For instance, the simple ``current_user_id`` example above could easily
.. be rewritten like so:

.. .. code-block:: edgeql-diff

..     select User {
..       id,
..       posts: { title, content }
..     }
..   - filter .id = global current_user_id
..   + filter .id = <uuid>$current_user_id

.. There is a subtle difference between these two in terms of
.. developer experience. When using parameters, you must provide a
.. value for ``$current_user_id`` on each *query execution*. By constrast,
.. the value of ``global current_user_id`` is defined when you initialize
.. the client; you can use this "sessionified" client to execute
.. user-specific queries without needing to keep pass around the
.. value of the user's UUID.

.. But that's a comparatively marginal difference.

Unlike query parameters, globals can be referenced
*inside your schema declarations*.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      property name -> str;
      property is_self := (.id = global current_user_id)
    };


.. code-block:: sdl
    :version-lt: 4.0

    type User {
      name: str;
      property is_self := (.id = global current_user_id)
    };

.. code-block:: sdl

    type User {
      name: str;
      is_self := (.id = global current_user_id)
    };

This is particularly useful when declaring :ref:`access policies
<ref_datamodel_access_policies>`.

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      required property name -> str;
      access policy my_policy allow all using (.id = global current_user_id);
    }

.. code-block:: sdl

    type Person {
      required name: str;
      access policy my_policy allow all using (.id = global current_user_id);
    }

Refer to :ref:`Access Policies <ref_datamodel_access_policies>` for complete
documentation.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Globals <ref_eql_sdl_globals>`
  * - :ref:`DDL > Globals <ref_eql_ddl_globals>`
