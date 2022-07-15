.. _ref_datamodel_globals:

=======
Globals
=======

.. note::

  ⚠️ Only available in EdgeDB 2.0 or later.

Schemas can contain scalar-typed *global variables*.

.. code-block:: sdl

  global current_user -> uuid;

These provide a useful mechanism for specifying session-level data that can be
referenced in queries with the ``global`` keyword.

.. code-block:: edgeql

  select User {
    id,
    posts: { title, content }
  }
  filter .id = (global current_user)

As in the example above, this is particularly useful for representing the
notion of a session or "current user". The value of global variables is set
and stored with the client library.

.. tabs::

  .. code-tab:: typescript

    import createClient from 'edgedb';

    const baseClient = createClient()
    const clientWithGlobals = baseClient.withGlobals({
      current_user: '2141a5b4-5634-4ccc-b835-437863534c51',
    });

    await client.query(`select (global current_user)`);

  .. code-tab:: python

    from edgedb import create_client

    client = create_client().with_globals({
        'current_user': '580cc652-8ab8-4a20-8db9-4c79a4b1fd81'
    })

    result = client.query("""
        select (global current_user);
    """)
    print(result)

The ``.withGlobals()`` method returns a new ``Client`` instance that
internally stores the assigned global variable values. The new instance shares
a connection pool with the original instance.

Cardinality
------------

Global variables can be marked ``required``; in this case, you must specify a
default value.

.. code-block:: sdl

  required global one_string -> str {
    default := "Hi Mom!"
  };

Computed globals
----------------

Global variables can also be computed. Declare a computed global with the
following shorthand.

.. code-block:: sdl

  required global random_global := datetime_of_transaction();

The provided expression will be computed at the start of each query in which
the global is referenced. There's no need to provide an explicit type; the
type is inferred from the computed expression.

Computed globals can also have a ``multi`` cardinality. This isn't the case
for non-computed globals.

.. code-block:: sdl

  multi global str_multi := {'hi', 'mom'};


Comparison to parameters
------------------------

.. You may be wondering what purpose globals serve that can't.
.. For instance, the simple ``current_user`` example above could easily
.. be rewritten like so:

.. .. code-block:: edgeql-diff

..     select User {
..       id,
..       posts: { title, content }
..     }
..   - filter .id = global current_user
..   + filter .id = <uuid>$current_user

.. There is a subtle difference between these two in terms of
.. developer experience. When using parameters, you must provide a
.. value for ``$current_user`` on each *query execution*. By constrast,
.. the value of ``global current_user`` is defined when you initialize
.. the client; you can use this "sessionified" client to execute
.. user-specific queries without needing to keep pass around the
.. value of the user's UUID.

.. But that's a comparatively marginal difference.

Unlike query parameters, globals can be referenced
*inside your schema declarations*.

.. code-block:: sdl

  type User {
    property name -> str;
    property is_self := (.id = global current_user)
  };

This is particularly useful when declaring :ref:`object-level security
policies <ref_datamodel_ols>`.

.. code-block::

  type Person {
    required property name -> str;
    access policy my_policy allow delete using (.id = global current_user);
  }

Refer to :ref:`Object-Level Security <ref_datamodel_ols>` for complete
documentation.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Object types <ref_eql_sdl_object_types>`
  * - :ref:`DDL > Object types <ref_eql_ddl_object_types>`
  * - :ref:`Introspection > Object types <ref_eql_introspection_object_types>`
  * - :ref:`Cheatsheets > Object types <ref_cheatsheet_object_types>`
