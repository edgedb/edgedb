.. _ref_datamodel_globals:

=======
Globals
=======

.. index:: global, required global

Schemas in Gel can contain typed *global variables*. These create a mechanism
for specifying session-level context that can be referenced in queries,
access policies, triggers, and elsewhere with the ``global`` keyword.

Here's a very common example of a global variable representing the current
user ID:

.. code-block:: sdl

  global current_user_id: uuid;

.. tabs::

  .. code-tab:: edgeql

    select User {
      id,
      posts: { title, content }
    }
    filter .id = global current_user_id;

  .. code-tab:: python

    # In a non-trivial example, `global current_user_id` would
    # be used indirectly in an access policy or some other context.
    await client.with_globals({'user_id': user_id}).qeury('''
      select User {
        id,
        posts: { title, content }
      }
      filter .id = global current_user_id;
    ''')

  .. code-tab:: typescript

    // In a non-trivial example, `global current_user_id` would
    // be used indirectly in an access policy or some other context.
    await client.withGlobals({user_id}).qeury('''
      select User {
        id,
        posts: { title, content }
      }
      filter .id = global current_user_id;
    ''')


Setting global variables
========================

Global variables are set at session level or when initializing a client.
The exact API depends on which client library you're using, but the general
behavior and principles are the same across all libraries.

.. tabs::

  .. code-tab:: typescript

    import createClient from 'gel';

    const baseClient = createClient();

    // returns a new Client instance, that shares the underlying
    // network connection with `baseClient` , but sends the configured
    // globals along with all queries run through it:
    const clientWithGlobals = baseClient.withGlobals({
      current_user_id: '2141a5b4-5634-4ccc-b835-437863534c51',
    });

    const result = await clientWithGlobals.query(
      `select global current_user_id;`
    );

  .. code-tab:: python

    from gel import create_client

    base_client = create_client()

    # returns a new Client instance, that shares the underlying
    # network connection with `base_client` , but sends the configured
    # globals along with all queries run through it:
    client = base_client.with_globals({
        'current_user_id': '580cc652-8ab8-4a20-8db9-4c79a4b1fd81'
    })

    result = client.query("""
        select global current_user_id;
    """)

  .. code-tab:: go

    package main

    import (
      "context"
      "fmt"
      "log"

      "github.com/geldata/gel-go"
    )

    func main() {
      ctx := context.Background()
      client, err := gel.CreateClient(ctx, gel.Options{})
      if err != nil {
        log.Fatal(err)
      }
      defer client.Close()

      id, err := gel.ParseUUID("2141a5b4-5634-4ccc-b835-437863534c51")
      if err != nil {
        log.Fatal(err)
      }

      var result gel.UUID
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

    let client = gel_tokio::create_client().await.expect("Client init");

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
===========

A global variable can be declared with one of two cardinalities:

- ``single`` (the default): At most one value.
- ``multi``: A set of values. Only valid for computed global variables.

In addition, a global can be marked ``required`` or ``optional`` (the default).
If marked ``required``, a default value must be provided.


Computed globals
================

.. index:: global, :=

Global variables can also be computed. The value of computed globals is
dynamically computed when they are referenced in queries.

.. code-block:: sdl

  required global now := datetime_of_transaction();

The provided expression will be computed at the start of each query in which
the global is referenced. There's no need to provide an explicit type; the type
is inferred from the computed expression.

Computed globals can also be object-typed and have ``multi`` cardinality.
For example:

.. code-block:: sdl

  global current_user_id: uuid;

  # object-typed global
  global current_user := (
    select User filter .id = global current_user_id
  );

  # multi global
  global current_user_friends := (global current_user).friends;


Referencing globals
===================

Unlike query parameters, globals can be referenced *inside your schema
declarations*:

.. code-block:: sdl

  type User {
    name: str;
    is_self := (.id = global current_user_id)
  };

This is particularly useful when declaring :ref:`access policies
<ref_datamodel_access_policies>`:

.. code-block:: sdl

  type Person {
    required name: str;

    access policy my_policy allow all
      using (.id = global current_user_id);
  }

Refer to :ref:`Access Policies <ref_datamodel_access_policies>` for complete
documentation.

.. _ref_eql_sdl_globals:
.. _ref_eql_sdl_globals_syntax:

Declaring globals
=================

This section describes the syntax to declare a global variable in your schema.

Syntax
------

Define a new global variable in SDL, corresponding to the more explicit DDL
commands described later:

.. sdl:synopsis::

  # Global variable declaration:
  [{required | optional}] [single]
    global <name> -> <type>
    [ "{"
        [ default := <expression> ; ]
        [ <annotation-declarations> ]
        ...
      "}" ]

  # Computed global variable declaration:
  [{required | optional}] [{single | multi}]
    global <name> := <expression>;


Description
^^^^^^^^^^^

There are two different forms of ``global`` declarations, as shown in the
syntax synopsis above:

1. A *settable* global (defined with ``-> <type>``) which can be changed using
   a session-level :ref:`set <ref_eql_statements_session_set_alias>` command.

2. A *computed* global (defined with ``:= <expression>``), which cannot be
   directly set but instead derives its value from the provided expression.

The following options are available:

:eql:synopsis:`required`
  If specified, the global variable is considered *required*. It is an
  error for this variable to have an empty value. If a global variable is
  declared *required*, it must also declare a *default* value.

:eql:synopsis:`optional`
  The global variable is considered *optional*, i.e. it is possible for the
  variable to have an empty value. (This is the default.)

:eql:synopsis:`multi`
  Specifies that the global variable may have a set of values. Only
  *computed* global variables can have this qualifier.

:eql:synopsis:`single`
  Specifies that the global variable must have at most a *single* value. It
  is assumed that a global variable is ``single`` if neither ``multi`` nor
  ``single`` is specified. All non-computed global variables must be *single*.

:eql:synopsis:`<name>`
  The name of the global variable. It can be fully-qualified with the module
  name, or it is assumed to belong to the module in which it appears.

:eql:synopsis:`<type>`
  The type must be a valid :ref:`type expression <ref_eql_types>` denoting a
  non-abstract scalar or a container type.

:eql:synopsis:`<name> := <expression>`
  Defines a *computed* global variable. The provided expression must be a
  :ref:`Stable <ref_reference_volatility>` EdgeQL expression. It can refer
  to other global variables. The type of a *computed* global variable is
  not limited to scalar and container types; it can also be an object type.

The valid SDL sub-declarations are:

:eql:synopsis:`default := <expression>`
  Specifies the default value for the global variable as an EdgeQL
  expression. The default value is used in a session if the value was not
  explicitly specified by the client, or was reset with the :ref:`reset
  <ref_eql_statements_session_reset_alias>` command.

:sdl:synopsis:`<annotation-declarations>`
  Set global variable :ref:`annotation <ref_eql_sdl_annotations>`
  to a given *value*.


Examples
--------

Declare a new global variable:

.. code-block:: sdl

  global current_user_id -> uuid;
  global current_user := (
      select User filter .id = global current_user_id
  );

Set the global variable to a specific value using :ref:`session-level commands
<ref_eql_statements_session_set_alias>`:

.. code-block:: edgeql

  set global current_user_id :=
      <uuid>'00ea8eaa-02f9-11ed-a676-6bd11cc6c557';

Use the computed global variable that is based on the value that was just set:

.. code-block:: edgeql

  select global current_user { name };

:ref:`Reset <ref_eql_statements_session_reset_alias>` the global variable to
its default value:

.. code-block:: edgeql

  reset global user_id;


.. _ref_eql_ddl_globals:


DDL commands
============

This section describes the low-level DDL commands for creating, altering, and
dropping globals. You typically don't need to use these commands directly, but
knowing about them is useful for reviewing migrations.


Create global
-------------

:eql-statement:
:eql-haswith:

Declare a new global variable using DDL.

.. eql:synopsis::

  [ with <with-item> [, ...] ]
  create [{required | optional}] [single]
    global <name> -> <type>
      [ "{" <subcommand>; [...] "}" ] ;

  # Computed global variable form:

  [ with <with-item> [, ...] ]
  create [{required | optional}] [{single | multi}]
    global <name> := <expression>;

  # where <subcommand> is one of

    set default := <expression>
    create annotation <annotation-name> := <value>

Description
^^^^^^^^^^^

As with SDL, there are two different forms of ``global`` declaration:

- A global variable that can be :ref:`set <ref_eql_statements_session_set_alias>`
  in a session.
- A *computed* global that is derived from an expression (and so cannot be
  directly set in a session).

The subcommands mirror those in SDL:

:eql:synopsis:`set default := <expression>`
  Specifies the default value for the global variable as an EdgeQL
  expression. The default value is used by the session if the value was not
  explicitly specified or was reset with the :ref:`reset
  <ref_eql_statements_session_reset_alias>` command.

:eql:synopsis:`create annotation <annotation-name> := <value>`
  Assign an annotation to the global variable. See :eql:stmt:`create annotation`
  for details.


Examples
^^^^^^^^

Define a new global property ``current_user_id``:

.. code-block:: edgeql

  create global current_user_id -> uuid;

Define a new *computed* global property ``current_user`` based on the
previously defined ``current_user_id``:

.. code-block:: edgeql

  create global current_user := (
      select User filter .id = global current_user_id
  );


Alter global
------------

:eql-statement:
:eql-haswith:

Change the definition of a global variable.

.. eql:synopsis::

  [ with <with-item> [, ...] ]
  alter global <name>
    [ "{" <subcommand>; [...] "}" ] ;

  # where <subcommand> is one of

    set default := <expression>
    reset default
    rename to <newname>
    set required
    set optional
    reset optionalily
    set single
    set multi
    reset cardinality
    set type <typename> reset to default
    using (<computed-expr>)
    create annotation <annotation-name> := <value>
    alter annotation <annotation-name> := <value>
    drop annotation <annotation-name>

Description
^^^^^^^^^^^

The command :eql:synopsis:`alter global` changes the definition of a global
variable. It can modify default values, rename the global, or change other
attributes like optionality, cardinality, computed expressions, etc.

Examples
^^^^^^^^

Set the ``description`` annotation of global variable ``current_user``:

.. code-block:: edgeql

  alter global current_user
      create annotation description :=
          'Current User as specified by the global ID';

Make the ``current_user_id`` global variable ``required``:

.. code-block:: edgeql

  alter global current_user_id {
      set required;
      # A required global variable MUST have a default value.
      set default := <uuid>'00ea8eaa-02f9-11ed-a676-6bd11cc6c557';
  }


Drop global
-----------

:eql-statement:
:eql-haswith:

Remove a global variable from the schema.

.. eql:synopsis::

  [ with <with-item> [, ...] ]
  drop global <name> ;

Description
^^^^^^^^^^^

The command :eql:synopsis:`drop global` removes the specified global variable
from the schema.

Example
^^^^^^^

Remove the ``current_user`` global variable:

.. code-block:: edgeql

  drop global current_user;
