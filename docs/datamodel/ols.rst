.. _ref_datamodel_ols:

=====================
Object-Level Security
=====================

.. note::

  ⚠️ Only available in EdgeDB 2.0 or later.

Object types can contain security policies that restrict the set of objects that can be selected, inserted, updated, or deleted by a particular query.

Let's start with a simple schema.

.. code-block:: sdl

  type User {
    required property email -> str { constraint exclusive; };
  }

  type BlogPost {
    link author -> Person;
  }

When no access policies are defined, any properly authenticated query can select or modify any object in the database. Once a policy is added to a particular object type, this switches; all operations (``select``, ``insert``, etc.) are *disallowed by default* except those specifically permitted by the access policies.

Defining a policy
^^^^^^^^^^^^^^^^^

Let's add a policy to our sample schema.

.. code-block:: sdl-diff

  +   global current_user -> uuid;

      type User {
        required property email -> str { constraint exclusive; };
      }

      type BlogPost {
        link author -> Person;
  +     access policy own_posts allow all using (
  +       .author.id ?= global current_user
  +     )
      }

Note that we've added a global variable called ``current_user``. Global variables are a mechanism for providing context to a query; you set their value when you first initialize your client. The exact API depends on which client library you're using; refer to the :ref:`Global Variables <ref_datamodel_globals>` page for complete documentation.

.. tabs::

  .. code-tab:: typescript

    import createClient from 'edgedb';

    const client = createClient().withGlobals({
      current_user: '2141a5b4-5634-4ccc-b835-437863534c51',
    });

    await client.query(`select global current_user;`);

  .. code-tab:: python

    from edgedb import create_client

    client = create_client().with_globals({
        'current_user': '580cc652-8ab8-4a20-8db9-4c79a4b1fd81'
    })

    result = client.query("""
        select global current_user;
    """)

Syntax breakdown
^^^^^^^^^^^^^^^^

Let's break this down the access policy syntax piece-by-piece.

.. code-block:: sdl

  access policy own_posts allow all using (...)


- ``access policy``: the keyword used to declare a policy inside an object type.
- ``own_posts``: the name of this policy; could be any string.
- ``allow``: the kind of policy; could be ``allow`` or ``deny``
- ``all``: the set of operations being allowed/denied; one of the following: ``all``, ``select``, ``insert``, ``delete``, ``update``, ``update read``, ``update write``.
- ``using (<expr>)``: a filter expression that determines the set of objects to which the policy applies.

This policy grants full read-write access (``all``) to the ``author`` of each ``BlogPost``. Let's do some experiments.

.. code-block:: edgeql-repl

  db> insert User { email := "test@edgedb.com" };
  {default::User {id: be44b326-03db-11ed-b346-7f1594474966}}
  db> set global current_user := <uuid>"be44b326-03db-11ed-b346-7f1594474966";
  OK: SET GLOBAL
  db> insert BlogPost {
  ...    title := "My post",
  ...    author := (select User filter .id = global current_user)
  ...  };
  {default::BlogPost {id: e76afeae-03db-11ed-b346-fbb81f537ca6}}

We've created a ``User``, set the value of ``current_user`` to its ``id``, and created a new ``BlogPost``. When we try to select all ``BlogPost`` objects, we'll see the post we just created.

.. code-block:: edgeql-repl

  db> select BlogPost;
  {default::BlogPost {id: e76afeae-03db-11ed-b346-fbb81f537ca6}}
  db> select count(BlogPost);
  {1}

Now let's unset ``current_user`` and see what happens.

.. code-block:: edgeql-repl

  db> set global current_user := {};
  OK: SET GLOBAL
  db> select BlogPost;
  {}
  db> select count(BlogPost);
  {0}

Now ``select BlogPost`` returns zero results. We can only ``select`` the *posts* written by the *user* specified by ``current_user``. When ``current_user`` has no value, we can't read any posts.

The access policies use global variables to define a "subgraph" of data that is visible to a particular query.

Policy types
^^^^^^^^^^^^

For the most part, the policy types correspond to EdgeQL's *statement types*:

- ``select``
- ``insert``
- ``update``
- ``delete``

Additionally, the ``update`` operation can broken down into two sub-policies: ``update read`` and ``update write``.

- ``update read``: this policy restricts *which* objects can be updated. It runs *pre-update*; that is, this policy is executed before the updates have been applied.
- ``update write``: this policy restricts *how* you update the objects; you can think of it as a *post-update* validity check. This could be used to prevent a ``User`` from transferring a ``BlogPost`` to another ``User``.

Finally, there's an umbrella policy that can be used as a shorthand for all the others.

- ``all``: a shorthand policy that can be used to allow or deny full read/write permissions.


Resolution algorithm
^^^^^^^^^^^^^^^^^^^^

An object type can contain an arbitrary number of access policies, including several conflicting ``allow`` and ``deny`` policies. EdgeDB uses a particular algorithm for resolving these policies.

.. figure:: images/ols.png

  The access policy resolution algorithm, explained with Venn diagrams.

1. As stated previously, when no policies are defined on a given object type, all objects of that type can be read or modified by any appropriately authenticated connection.

2. EdgeDB then applies all ``allow`` policies. Each policy grants a *permission* that is scoped to a particular *set of objects*. Conceptually, these permissions are merged with the ``union`` / ``or`` operator to determine the set of allowable actions.

3. After the ``allow`` policies are resolved, the ``deny`` policies can be used to carve out exceptions.

4. Once the ``deny`` policies are applied, we're left with a final access level: a set of objects targetable by each of ``select``, ``insert``, ``update read``, ``update write``, and ``delete``.


.. .. list-table::
..   :class: seealso

..   * - **See also**
..   * - :ref:`SDL > Object types <ref_eql_sdl_object_types>`
..   * - :ref:`DDL > Object-level security <ref_eql_ddl_acl>`
..   * - :ref:`Introspection > Object types <ref_eql_introspection_object_types>`
..   * - :ref:`Cheatsheets > Object types <ref_cheatsheet_object_types>`
