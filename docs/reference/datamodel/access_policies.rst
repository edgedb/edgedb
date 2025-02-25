.. _ref_datamodel_access_policies:

===============
Access Policies
===============

.. index:: access policy, object-level security, row-level security, RLS,
           allow, deny, using

Object types in |Gel| can contain security policies that restrict the set of
objects that can be selected, inserted, updated, or deleted by a particular
query. This is known as *object-level security* and is similar in function
to SQL's row-level security.

When no access policies are defined, object-level security is not activated:
any properly authenticated client can carry out any operation on any object
in the database. Access policies allow you to ensure that the database itself
handles access control logic rather than having to implement it in every
application or service that connects to your database.

Access policies can greatly simplify your backend code, centralizing access
control logic in a single place. They can also be extremely useful for
implementing AI agentic flows, where you want to have guardrails around
your data that agents can't break.

We'll illustrate access policies in this document with this simple schema:

.. code-block:: sdl

    type User {
      required email: str { constraint exclusive; }
    }

    type BlogPost {
      required title: str;
      required author: User;
    }


.. warning::

  Once a policy is added to a particular object type, **all operations**
  (``select``, ``insert``, ``delete``, ``update``, etc.) on any object of
  that type are now *disallowed by default* unless specifically allowed by
  an access policy! See :ref:`resolution order <ref_datamodel_access_policies>`
  below for details.

Global variables
================

Global variables are a convenient way to set up the context for your access
policies.  Gel's global variables are tightly integrated with the Gel's
data model, client APIs, EdgeQL and SQL, and the tooling around them.

Global variables in Gel are not pre-defined. Users are free to define
as many globals in their schema as they want to represent the business
logic of their application.

A common scenario is storing a ``current_user`` global representing
the user executing queries. We'd like to have a slightly more complex example
showing that you can use more than one global variable. Let's do that:

* We'll use one *global* ``uuid`` to represent the identity of the user
  executing the query.
* We'll have the ``Country`` *enum* to represent the type of country
  that the user  is currently in. The enum represents three types of
  countries: those where the service has not been rolled out, those with
  read-only access, and those with full access.
* We'll use the ``current_country`` *global* to represent the user's
  current country. In our *example schema*, we want *country* to be
  context-specific: the same user who can access certain content in one
  country might not be able to in another country (let's imagine that's
  due to different country-specific legal frameworks).

Here is an illustration:

.. code-block:: sdl-diff

    +   scalar type Country extending enum<Full, ReadOnly, None>;
    +   global current_user: uuid;
    +   required global current_country: Country {
    +     default := Country.None
    +   }

        type User {
          required email: str { constraint exclusive; }
        }

        type BlogPost {
          required title: str;
          required author: User;
        }

You can set and reset these globals in Gel client libraries, for example:

.. tabs::

  .. code-tab:: typescript

    import createClient from 'gel';

    const client = createClient();

    // 'authedClient' will share the network connection with 'client',
    // but will have the 'current_user' global set.
    const authedClient = client.withGlobals({
      current_user: '2141a5b4-5634-4ccc-b835-437863534c51',
    });

    const result = await authedClient.query(
      `select global current_user;`);
    console.log(result);

  .. code-tab:: python

    from gel import create_client

    client = create_client().with_globals({
        'current_user': '580cc652-8ab8-4a20-8db9-4c79a4b1fd81'
    })

    result = client.query("""
        select global current_user;
    """)
    print(result)

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

    use gel_protocol::{
      model::Uuid,
      value::EnumValue
    };

    let client = gel_tokio::create_client()
        .await
        .expect("Client should init")
        .with_globals_fn(|c| {
            c.set(
                "current_user",
                Value::Uuid(
                    Uuid::parse_str("2141a5b4-5634-4ccc-b835-437863534c51")
                        .expect("Uuid should have parsed"),
                ),
            );
            c.set(
                "current_country",
                Value::Enum(EnumValue::from("Full"))
            );
        });
    client
        .query_required_single::<Uuid, _>("select global current_user;", &())
        .await
        .expect("Returning value");


Defining policies
=================

A policy example for our simple blog schema might look like:

.. code-block:: sdl-diff

      global current_user: uuid;
      required global current_country: Country {
        default := Country.None
      }
      scalar type Country extending enum<Full, ReadOnly, None>;

      type User {
        required email: str { constraint exclusive; }
      }

      type BlogPost {
        required title: str;
        required author: User;

    +   access policy author_has_full_access
    +     allow all
    +     using (global current_user    ?= .author.id
    +       and  global current_country ?= Country.Full) {
    +       errmessage := "User does not have full access";
    +     }

    +   access policy author_has_read_access
    +     allow select
    +     using (global current_user    ?= .author.id
    +       and  global current_country ?= Country.ReadOnly);
      }

Explanation:

- ``access policy <name>`` introduces a new policy in an object type.
- ``allow all`` grants ``select``, ``insert``, ``update``, and ``delete``
  access if the condition passes. We also used a separate policy to allow
  only ``select`` in some cases.
- ``using (<expr>)`` is a boolean filter restricting the set of objects to
  which the policy applies. (We used the coalescing operator ``?=`` to
  handle empty sets gracefully.)
- ``errmessage`` is an optional custom message to display in case of a write
  violation.

Let's run some experiments in the REPL:

.. code-block:: edgeql-repl

  db> insert User { email := "test@example.com" };
  {default::User {id: be44b326-03db-11ed-b346-7f1594474966}}
  db> set global current_user :=
  ...   <uuid>"be44b326-03db-11ed-b346-7f1594474966";
  OK: SET GLOBAL
  db> set global current_country := Country.Full;
  OK: SET GLOBAL
  db> insert BlogPost {
  ...    title := "My post",
  ...    author := (select User filter .id = global current_user)
  ...  };
  {default::BlogPost {id: e76afeae-03db-11ed-b346-fbb81f537ca6}}

Because the user is in a "full access" country and the current user ID
matches the author, the new blog post is permitted. When the same user sets
``global current_country := Country.ReadOnly;``:

.. code-block:: edgeql-repl

  db> set global current_country := Country.ReadOnly;
  OK: SET GLOBAL
  db> select BlogPost;
  {default::BlogPost {id: e76afeae-03db-11ed-b346-fbb81f537ca6}}
  db> insert BlogPost {
  ...    title := "My second post",
  ...    author := (select User filter .id = global current_user)
  ...  };
  gel error: AccessPolicyError: access policy violation on
  insert of default::BlogPost (User does not have full access)

Finally, let's unset ``current_user`` and see how many blog posts are returned
when we count them.

.. code-block:: edgeql-repl

  db> set global current_user := {};
  OK: SET GLOBAL
  db> select BlogPost;
  {}
  db> select count(BlogPost);
  {0}

``select BlogPost`` returns zero results in this case as well. We can only
``select`` the *posts* written by the *user* specified by ``current_user``.
When ``current_user`` has no value or has a different value from the
``.author.id`` of any existing ``BlogPost`` objects, we can't read any posts.
But thanks to ``Country`` being set to ``Country.Full``, this user will be
able to write a new blog post.

**The bottom line:** access policies use global variables to define a
"subgraph" of data that is visible to your queries.


Policy types
============

.. index:: access policy, select, insert, delete, update, update read,
           update write, all

The types of policy rules map to the statement type in EdgeQL:

- ``select``: Controls which objects are visible to any query.
- ``insert``: Post-insert check. If the inserted object violates the policy,
  the operation fails.
- ``delete``: Controls which objects can be deleted.
- ``update read``: Pre-update check on which objects can be updated at all.
- ``update write``: Post-update check for how objects can be updated.
- ``all``: Shorthand for granting or denying ``select, insert, update,
  delete``.

Resolution order
================

If multiple policies apply (some are ``allow`` and some are ``deny``), the
logic is:

1. If there are no policies, access is allowed.
2. All ``allow`` policies collectively form a *union* / *or* of allowed sets.
3. All ``deny`` policies *subtract* from that union, overriding allows!
4. The final set of objects is the intersection of the above logic for each
   operation: ``select, insert, update read, update write, delete``.

By default, once you define any policy on an object type, you must explicitly
allow the operations you need. This is a common **pitfall** when you are
starting out with access policies (but you will develop an intuition for this
quickly). Let's look at an example:

.. code-block:: sdl

    global current_user_id: uuid;
    global current_user := (
      select User filter .id = global current_user_id
    );

    type User {
      required email: str { constraint exclusive; }
      required is_admin: bool { default := false };

      access policy admin_only
        allow all
        using (global current_user.is_admin ?? false);
    }

    type BlogPost {
      required title: str;
      author: User;

      access policy author_has_full_access
        allow all
        using (global current_user ?= .author.id);
    }

In the above schema only admins will see a non-empty ``author`` link when
running ``select BlogPost { author }``. Why? Because only admins can see
``User`` objects at all: ``admin_only`` policy is the only one defined on
the ``User`` type!

This means that instead of making ``BlogPost`` visible to its author, all
non-admin authors won't be able to see their own posts. The above issue can be
remedied by making the current user able to see their own ``User`` record.


Interaction between policies
============================

Policy expressions themselves do not take other policies into account
(since |EdgeDB| 3). This makes it easier to reason about policies.

Custom error messages
=====================

.. index:: access policy, errmessage, using

When an ``insert`` or ``update write`` violates an access policy, Gel will
raise a generic ``AccessPolicyError``:

.. code-block::

    gel error: AccessPolicyError: access policy violation
    on insert of <type>

.. note::

    Restricted access is represented either as an error message or an empty
    set, depending on the filtering order of the operation. The operations
    ``select``, ``delete``, or ``update read`` filter up front, and thus you
    simply won't get the data that is being restricted. Other operations
    (``insert`` and ``update write``) will return an error message.

If multiple policies are in effect, it can be helpful to define a distinct
``errmessage`` in your policy:

.. code-block:: sdl-diff

      global current_user_id: uuid;
      global current_user := (
        select User filter .id = global current_user_id
      );

      type User {
        required email: str { constraint exclusive; };
        required is_admin: bool { default := false };

        access policy admin_only
          allow all
    +     using (global current_user.is_admin ?? false) {
    +       errmessage := 'Only admins may query Users'
    +     };
      }

      type BlogPost {
        required title: str;
        author: User;

        access policy author_has_full_access
          allow all
    +     using (global current_user ?= .author) {
    +       errmessage := 'BlogPosts may only be queried by their authors'
    +     };
      }

Now if you attempt, for example, a ``User`` insert as a non-admin user, you
will receive this error:

.. code-block::

    gel error: AccessPolicyError: access policy violation on insert of
    default::User (Only admins may query Users)


Disabling policies
==================

.. index:: apply_access_policies

You may disable all access policies by setting the ``apply_access_policies``
:ref:`configuration parameter <ref_std_cfg>` to ``false``.

You may also temporarily disable access policies using the Gel UI configuration
checkbox (or via :gelcmd:`ui`), which only applies to your UI session.

More examples
=============

Here are some additional patterns:

1. Publicly visible blog posts, only writable by the author:

   .. code-block:: sdl-diff

         global current_user: uuid;

         type User {
           required email: str { constraint exclusive; }
         }

         type BlogPost {
           required title: str;
           required author: User;
       +   required published: bool { default := false };

           access policy author_has_full_access
             allow all
             using (global current_user ?= .author.id);
       +   access policy visible_if_published
       +     allow select
       +     using (.published);
         }

2. Visible to friends, only modifiable by the author:

   .. code-block:: sdl-diff

         global current_user: uuid;

         type User {
           required email: str { constraint exclusive; }
       +   multi friends: User;
         }

         type BlogPost {
           required title: str;
           required author: User;

           access policy author_has_full_access
             allow all
             using (global current_user ?= .author.id);
       +   access policy friends_can_read
       +     allow select
       +     using ((global current_user in .author.friends.id) ?? false);
         }

3. Publicly visible except to those blocked by the author:

   .. code-block:: sdl-diff

         type User {
           required email: str { constraint exclusive; }
       +   multi blocked: User;
         }

         type BlogPost {
           required title: str;
           required author: User;

           access policy author_has_full_access
             allow all
             using (global current_user ?= .author.id);
       +   access policy anyone_can_read
       +     allow select;
       +   access policy exclude_blocked
       +     deny select
       +     using ((global current_user in .author.blocked.id) ?? false);
         }

4. "Disappearing" posts that become invisible after 24 hours:

   .. code-block:: sdl-diff

         type User {
           required email: str { constraint exclusive; }
         }

         type BlogPost {
           required title: str;
           required author: User;
       +   required created_at: datetime {
       +     default := datetime_of_statement() # non-volatile
       +   }

           access policy author_has_full_access
             allow all
             using (global current_user ?= .author.id);
       +   access policy hide_after_24hrs
       +     allow select
       +     using (
       +       datetime_of_statement() - .created_at < <duration>'24 hours'
       +     );
           }

Super constraints
=================

Access policies can act like "super constraints." For instance, a policy on
``insert`` or ``update write`` can do a post-write validity check, rejecting
the operation if a certain condition is not met.

E.g. here's a policy that limits the number of blog posts a
``User`` can post:

.. code-block:: sdl-diff

      type User {
        required email: str { constraint exclusive; }
    +   multi posts := .<author[is BlogPost]
      }

      type BlogPost {
        required title: str;
        required author: User;

        access policy author_has_full_access
          allow all
          using (global current_user ?= .author.id);
    +   access policy max_posts_limit
    +     deny insert
    +     using (count(.author.posts) > 500);
      }

.. _ref_eql_sdl_access_policies:
.. _ref_eql_sdl_access_policies_syntax:

Declaring access policies
=========================

This section describes the syntax to declare access policies in your schema.

Syntax
------

.. sdl:synopsis::

    access policy <name>
      [ when (<condition>) ]
      { allow | deny } <action> [, <action> ... ]
      [ using (<expr>) ]
      [ "{"
         [ errmessage := value ; ]
         [ <annotation-declarations> ]
        "}" ] ;

    # where <action> is one of
    all
    select
    insert
    delete
    update [{ read | write }]

Where:

:eql:synopsis:`<name>`
    The name of the access policy.

:eql:synopsis:`when (<condition>)`
    Specifies which objects this policy applies to. The
    :eql:synopsis:`<condition>` has to be a :eql:type:`bool` expression.

    When omitted, it is assumed that this policy applies to all objects of a
    given type.

:eql:synopsis:`allow`
    Indicates that qualifying objects should allow access under this policy.

:eql:synopsis:`deny`
    Indicates that qualifying objects should *not* allow access under this
    policy. This flavor supersedes any :eql:synopsis:`allow` policy and can
    be used to selectively deny access to a subset of objects that otherwise
    explicitly allows accessing them.

:eql:synopsis:`all`
    Apply the policy to all actions. It is exactly equivalent to listing
    :eql:synopsis:`select`, :eql:synopsis:`insert`, :eql:synopsis:`delete`,
    :eql:synopsis:`update` actions explicitly.

:eql:synopsis:`select`
    Apply the policy to all selection queries. Note that any object that
    cannot be selected, cannot be modified either. This makes
    :eql:synopsis:`select` the most basic "visibility" policy.

:eql:synopsis:`insert`
    Apply the policy to all inserted objects. If a newly inserted object would
    violate this policy, an error is produced instead.

:eql:synopsis:`delete`
    Apply the policy to all objects about to be deleted. If an object does not
    allow access under this kind of policy, it is not going to be considered
    by any :eql:stmt:`delete` command.

    Note that any object that cannot be selected, cannot be modified either.

:eql:synopsis:`update read`
    Apply the policy to all objects selected for an update. If an object does
    not allow access under this kind of policy, it is not visible cannot be
    updated.

    Note that any object that cannot be selected, cannot be modified either.

:eql:synopsis:`update write`
    Apply the policy to all objects at the end of an update. If an updated
    object violates this policy, an error is produced instead.

    Note that any object that cannot be selected, cannot be modified either.

:eql:synopsis:`update`
    This is just a shorthand for :eql:synopsis:`update read` and
    :eql:synopsis:`update write`.

    Note that any object that cannot be selected, cannot be modified either.

:eql:synopsis:`using <expr>`
    Specifies what the policy is with respect to a given eligible (based on
    :eql:synopsis:`when` clause) object. The :eql:synopsis:`<expr>` has to be
    a :eql:type:`bool` expression. The specific meaning of this value also
    depends on whether this policy flavor is :eql:synopsis:`allow` or
    :eql:synopsis:`deny`.

    The expression must be :ref:`Stable <ref_reference_volatility>`.

    When omitted, it is assumed that this policy applies to all eligible
    objects of a given type.

:eql:synopsis:`set errmessage := <value>`
    Set a custom error message of :eql:synopsis:`<value>` that is displayed
    when this access policy prevents a write action.

:sdl:synopsis:`<annotation-declarations>`
    Set access policy :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.

Any sub-type extending a type inherits all of its access policies.
You can define additional access policies on sub-types.


.. _ref_eql_ddl_access_policies:

DDL commands
============

This section describes the low-level DDL commands for creating, altering, and
dropping access policies. You typically don't need to use these commands
directly, but knowing about them is useful for reviewing migrations.

Create access policy
--------------------

:eql-statement:

Define a new object access policy on a type:

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    { create | alter } type <TypeName> "{"
      [ ... ]
      create access policy <name>
        [ when (<condition>) ; ]
        { allow | deny } action [, action ... ; ]
        [ using (<expr>) ; ]
        [ "{"
           [ set errmessage := value ; ]
           [ create annotation <annotation-name> := value ; ]
          "}" ]
    "}"

    # where <action> is one of
    all
    select
    insert
    delete
    update [{ read | write }]

See the meaning of each parameter in the `Declaring access policies`_ section.

The following subcommands are allowed in the ``create access policy`` block:

:eql:synopsis:`set errmessage := <value>`
    Set a custom error message of :eql:synopsis:`<value>` that is displayed
    when this access policy prevents a write action.

:eql:synopsis:`create annotation <annotation-name> := <value>`
    Set access policy annotation :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`create annotation` for details.


Alter access policy
-------------------

:eql-statement:

Modify an existing access policy:

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    alter type <TypeName> "{"
      [ ... ]
      alter access policy <name> "{"
        [ when (<condition>) ; ]
        [ reset when ; ]
        { allow | deny } <action> [, <action> ... ; ]
        [ using (<expr>) ; ]
        [ set errmessage := value ; ]
        [ reset expression ; ]
        [ create annotation <annotation-name> := <value> ; ]
        [ alter annotation <annotation-name> := <value> ; ]
        [ drop annotation <annotation-name>; ]
      "}"
    "}"

You can change the policy's condition, actions, or error message, or add/drop
annotations.

The parameters describing the action policy are identical to the parameters
used by ``create action policy``. There are a handful of additional
subcommands that are allowed in the ``alter access policy`` block:

:eql:synopsis:`reset when`
    Clear the :eql:synopsis:`when (<condition>)` so that the policy applies to
    all objects of a given type. This is equivalent to ``when (true)``.

:eql:synopsis:`reset expression`
    Clear the :eql:synopsis:`using (<condition>)` so that the policy always
    passes. This is equivalent to ``using (true)``.

:eql:synopsis:`alter annotation <annotation-name>;`
    Alter access policy annotation :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>;`
    Remove access policy annotation :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`drop annotation` for details.


All the subcommands allowed in the ``create access policy`` block are also
valid subcommands for ``alter access policy`` block.

Drop access policy
------------------

:eql-statement:

Remove an existing policy:

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    alter type <TypeName> "{"
      [ ... ]
      drop access policy <name> ;
    "}"
