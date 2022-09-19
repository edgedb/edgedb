.. _ref_datamodel_access_policies:

====================
Access Policies #New
====================

.. note::

  This type is only available in EdgeDB 2.0 or later.

Object types can contain security policies that restrict the set of objects
that can be selected, inserted, updated, or deleted by a particular query.
This is known as *object-level security*.

Let's start with a simple schema.

.. code-block:: sdl

  type User {
    required property email -> str { constraint exclusive; };
  }

  type BlogPost {
    required property title -> str;
    required link author -> User;
  }


When no access policies are defined, object-level security is not activated.
Any properly authenticated client can select or modify any object in the
database.

⚠️ Once a policy is added to a particular object type, **all operations**
(``select``, ``insert``, ``delete``, and ``update`` etc.) on any object of
that type are now *disallowed by default* unless specifically allowed by an
access policy!

Defining a global
^^^^^^^^^^^^^^^^^

To start, we'll add a *global variable* to our schema. We'll use this global
to represent the identity of the user executing the query.

.. code-block:: sdl-diff

  +   global current_user -> uuid;

      type User {
        required property email -> str { constraint exclusive; };
      }

      type BlogPost {
        required property title -> str;
        required link author -> User;
      }

Global variables are a generic mechanism for providing *context* to a query.
Most commonly, they are used in the context of access policies.

The value of these variables is attached to the *client* you use to execute
queries. The exact API depends on which client library you're using:

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


Defining a policy
^^^^^^^^^^^^^^^^^

Let's add a policy to our sample schema.

.. code-block:: sdl-diff

      global current_user -> uuid;

      type User {
        required property email -> str { constraint exclusive; };
      }

      type BlogPost {
        required property title -> str;
        required link author -> User;

  +     access policy author_has_full_access
  +       allow all
  +       using (global current_user ?= .author.id);
      }


Let's break down the access policy syntax piece-by-piece. This policy grants
full read-write access (``all``) to the ``author`` of each ``BlogPost``. No
one else will be able to edit, delete, or view this post.

.. note::

  We're using the *coalescing equality* operator ``?=`` which returns
  ``false`` even if one of its arguments is an empty set.

- ``access policy``: The keyword used to declare a policy inside an object
  type.
- ``own_posts``: The name of this policy; could be any string.
- ``allow``: The kind of policy; could be ``allow`` or ``deny``
- ``all``: The set of operations being allowed/denied; a comma-separated list
  of the following: ``all``, ``select``, ``insert``, ``delete``, ``update``,
  ``update read``, ``update write``.
- ``using (<expr>)``: A boolean expression. Think of this as a ``filter``
  expression that defined the set of objects to which the policy applies.

Let's do some experiments.

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

We've created a ``User``, set the value of ``current_user`` to its ``id``, and
created a new ``BlogPost``. When we try to select all ``BlogPost`` objects,
we'll see the post we just created.

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

Now ``select BlogPost`` returns zero results. We can only ``select`` the
*posts* written by the *user* specified by ``current_user``. When
``current_user`` has no value, we can't read any posts.

The access policies use global variables to define a "subgraph" of data that
is visible to a particular query.

Policy types
^^^^^^^^^^^^

For the most part, the policy types correspond to EdgeQL's *statement types*:

- ``select``: Applies to all queries; objects without a ``select`` permission
  cannot be modified either.
- ``insert``: Applies to insert queries; executed *post-insert*. If an
  inserted object violates the policy, the query will fail.
- ``delete``: Applies to delete queries.
- ``update``: Applies to update queries.

Additionally, the ``update`` operation can broken down into two sub-policies:
``update read`` and ``update write``.

- ``update read``: This policy restricts *which* objects can be updated. It
  runs *pre-update*; that is, this policy is executed before the updates have
  been applied.
- ``update write``: This policy restricts *how* you update the objects; you
  can think of it as a *post-update* validity check. This could be used to
  prevent a ``User`` from transferring a ``BlogPost`` to another ``User``.

Finally, there's an umbrella policy that can be used as a shorthand for all
the others.

- ``all``: A shorthand policy that can be used to allow or deny full read/
  write permissions. Exactly equivalent to ``select, insert, update, delete``.

Resolution order
^^^^^^^^^^^^^^^^

An object type can contain an arbitrary number of access policies, including
several conflicting ``allow`` and ``deny`` policies. EdgeDB uses a particular
algorithm for resolving these policies.

.. figure:: images/ols.png

  The access policy resolution algorithm, explained with Venn diagrams.

1. When no policies are defined on a given object type, object-level security
   is all objects of that type can be read or modified by any appropriately
   authenticated connection.

2. EdgeDB then applies all ``allow`` policies. Each policy grants a
   *permission* that is scoped to a particular *set of objects* as defined by
   the ``using`` clause. Conceptually, these permissions are merged with
   the ``union`` / ``or`` operator to determine the set of allowable actions.

3. After the ``allow`` policies are resolved, the ``deny`` policies can be
   used to carve out exceptions to the ``allow`` rules. Deny rules *supersede*
   allow rules! As before, the set of objects targeted by the policy is
   defined by the ``using`` clause.

4. This results in the final access level: a set of objects targetable by each
   of ``select``, ``insert``, ``update read``, ``update write``, and
   ``delete``.

Currently, by default the access policies affect the values visible
in expressions of *other* access
policies. This means that they can affect each other in various ways. Because
of this great care needs to be taken when creating access policies based on
objects other than the ones they are defined on. For example:

.. code-block:: sdl

    global current_user_id -> uuid;
    global current_user := (
      select User filter .id = global current_user_id
    );

    type User {
      required property email -> str { constraint exclusive; };
      required property is_admin -> bool { default := false };

      access policy admin_only
        allow all
        using (global current_user.is_admin ?? false);
    }

    type BlogPost {
      required property title -> str;
      link author -> User;

      access policy author_has_full_access
        allow all
        using (global current_user ?= .author.id);
    }

In the above schema only the admin will see a non-empty ``author`` link,
because only the admin can see any user objects at all. This means that
instead of making ``BlogPost`` visible to its author, all non-admin authors
won't be able to see their own posts. The above issue can be remedied by
making the current user able to see their own ``User`` record.

.. _ref_datamodel_access_policies_nonrecursive:
.. _nonrecursive:

.. warning::

  Starting with the upcoming EdgeDB 3.0, access policy restrictions will
  **not**
  apply to any access policy expression. This means that when reasoning about
  access policies it is no longer necesary to take other policies into
  account. Instead, all data is visible for the purpose of *defining* an access
  policy.

  This change is being made to simplify reasoning about access
  policies and to allow certain patterns to be express
  efficiently. Since those who have access to modifying the schema can
  remove unwanted access policies, no additional security is provided
  by applying access policies to each other's expressions.

  It is possible (and recommended) to enable this :ref:`future
  <ref_eql_sdl_future>` behavior in EdgeDB 2.6 and later by adding the
  following to the schema: ``using future nonrecursive_access_policies;``


Examples
^^^^^^^^

Blog posts are publicly visible if ``published`` but only writable by the
author.

.. code-block:: sdl-diff

    global current_user -> uuid;

    type User {
      required property email -> str { constraint exclusive; };
    }

    type BlogPost {
      required property title -> str;
      required link author -> User;
  +   required property published -> bool { default := false }

      access policy author_has_full_access
        allow all
        using (global current_user ?= .author.id);
  +   access policy visible_if_published
  +     allow select
  +     using (.published);
    }

Blog posts are visible to friends but only modifiable by the author.

.. code-block:: sdl-diff

    global current_user -> uuid;

    type User {
      required property email -> str { constraint exclusive; };
  +   multi link friends -> User;
    }

    type BlogPost {
      required property title -> str;
      required link author -> User;

      access policy author_has_full_access
        allow all
        using (global current_user ?= .author.id);
  +   access policy friends_can_read
  +     allow select
  +     using ((global current_user in .author.friends.id) ?? false);
    }

Blog posts are publicly visible except to users that have been ``blocked`` by
the author.

.. code-block:: sdl-diff

    type User {
      required property email -> str { constraint exclusive; };
  +   multi link blocked -> User;
    }

    type BlogPost {
      required property title -> str;
      required link author -> User;

      access policy author_has_full_access
        allow all
        using (global current_user ?= .author.id);
  +   access policy anyone_can_read
  +     allow select;
  +   access policy exclude_blocked
  +     deny select
  +     using ((global current_user in .author.blocked.id) ?? false);
    }


"Disappearing" posts that become invisible after 24 hours.

Blog posts are publicly visible except to users that have been ``blocked`` by
the author.

.. code-block:: sdl-diff

    type User {
      required property email -> str { constraint exclusive; };
    }

    type BlogPost {
      required property title -> str;
      required link author -> User;
  +   required property created_at -> datetime {
  +     default := datetime_of_statement() # non-volatile
  +   }

      access policy author_has_full_access
        allow all
        using (global current_user ?= .author.id);
  +   access policy hide_after_24hrs
  +     allow select
  +     using (datetime_of_statement() - .created_at < <duration>'24 hours');
    }

Super constraints
*****************

Access policies support arbitrary EdgeQL and can be used to define "super
constraints". Policies on ``insert`` and ``update write`` can
be thought of as post-write "validity checks"; if the check fails, the write
will be rolled back.

.. note::

  Due to an underlying Postgres limitation, :ref:`constraints on object types
  <ref_datamodel_constraints_objects>` can only reference properties, not
  links.

Here's a policy that limits the number of blog posts a ``User`` can post.

.. code-block:: sdl-diff

    type User {
      required property email -> str { constraint exclusive; };
  +   multi link posts := .<author[is BlogPost]
    }

    type BlogPost {
      required property title -> str;
      required link author -> User;

      access policy author_has_full_access
        allow all
        using (global current_user ?= .author.id);
  +   access policy max_posts_limit
  +     deny insert
  +     using (count(.author.posts) > 500);
    }

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Access policies <ref_eql_sdl_access_policies>`
  * - :ref:`DDL > Access policies <ref_eql_ddl_access_policies>`
