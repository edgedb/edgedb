.. _ref_eql_paths:

=====
Paths
=====


A *path expression* (or simply a *path*) represents a set of values that are
reachable by traversing a given sequence of links or properties from some
source set of objects.

Consider the following schema:

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property email -> str;
      multi link friends -> User;
    }

    type BlogPost {
      required property title -> str;
      required link author -> User;
    }

    type Comment {
      required property text -> str;
      required link author -> User;
    }

.. code-block:: sdl

    type User {
      required email: str;
      multi friends: User;
    }

    type BlogPost {
      required title: str;
      required author: User;
    }

    type Comment {
      required text: str;
      required author: User;
    }

A few simple inserts will allow some experimentation with paths.

Start with a first user:

.. code-block:: edgeql-repl

    db> insert User {
    ... email := "user1@me.com",
    ... };

Along comes another user who adds the first user as a friend:

.. code-block:: edgeql-repl

    db> insert User {
    ... email := "user2@me.com",
    ... friends := (select detached User filter .email = "user1@me.com")
    ... };

The first user reciprocates, adding the new user as a friend:

.. code-block:: edgeql-repl

    db> update User filter .email = "user1@me.com" 
    ... set { 
    ... friends += (select detached User filter .email = "user2@me.com")
    ... };

The second user writes a blog post about how nice EdgeDB is:

.. code-block:: edgeql-repl

    db> insert BlogPost {
    ... title := "EdgeDB is awesome",
    ... author := assert_single((select User filter .email = "user2@me.com"))
    ... };

And the first user follows it up with a comment below the post:

.. code-block:: edgeql-repl

    db> insert Comment {
    ... text := "Nice post, user2!",
    ... author := assert_single((select User filter .email = "user1@me.com"))
    ... };

The simplest path is simply ``User``. This is a :ref:`set reference
<ref_eql_set_references>` that refers to all ``User`` objects in the database.

.. code-block:: edgeql

    select User;

Paths can traverse links. The path below refers to *all Users who are the
friend of another User*.

.. code-block:: edgeql

    select User.friends;

Paths can traverse to an arbitrary depth in a series of nested links.
Both ``select`` queries below end up showing the author of the ``BlogPost``.
The second query returns the friends of the friends of the author of the
``BlogPost``, which in this case is just the author.

.. code-block:: edgeql

    select BlogPost.author; # The author
    select BlogPost.author.friends.friends; # The author again

Paths can terminate with a property reference.

.. code-block:: edgeql

    select BlogPost.title; # all blog post titles
    select BlogPost.author.email; # all author emails
    select User.friends.email; # all friends' emails

.. _ref_eql_paths_backlinks:

Backlinks
---------

All examples thus far have traversed links in the *forward direction*, however
it's also possible to traverse links *backwards* with ``.<`` notation. These
are called **backlinks**.

Starting from each user, the path below traverses all *incoming* links labeled
``author`` and returns the union of their sources.

.. code-block:: edgeql

    select User.<author;

This query works, showing both the ``BlogPost`` and the ``Comment`` in the
database. However, we can't impose a shape on it:

.. code-block:: edgeql

    select User.<author { text };

As written, EdgeDB infers the *type* of this expression to be
:eql:type:`BaseObject`. Why? Because in theory, there may be
several links named ``author`` from different object types
that point to ``User``. And there is no guarantee that each 
of these types will have a property called ``text``.

.. note::
  ``BaseObject`` is the root ancestor of all object types and it only contains
  a single property, ``id``.

As such, commonly you'll want to narrow the results to a particular type.
To do so, use the :eql:op:`type intersection <isintersect>` operator: 
``[is Foo]``:

.. code-block:: edgeql
    
    # BlogPost objects that link to the user via a link named author
    select User.<author[is BlogPost];
    
    # Comment objects that link to the user via a link named author
    select User.<author[is Comment];

    # All objects that link to the user via a link named author
    select User.<author;

Or parsed one step at a time, the above queries can be read as follows:

================================ ===================================
Syntax                           Meaning
================================ ===================================
``User.<``                       Objects that link to the user
``author``                       via a link named author
================================ ===================================

================================ ===================================
Syntax                           Meaning
================================ ===================================
``User.<``                       Objects that link to the user
``author``                       via a link named author
``[is BlogPost]``                that are ``BlogPost`` objects
================================ ===================================

================================ ===================================
Syntax                           Meaning
================================ ===================================
``User.<``                       Objects that link to the user
``author``                       via a link named author
``[is Comment]``                 that are ``Comment`` objects
================================ ===================================

Backlinks can be inserted into a schema with the same format, except
that the type name (in this case ``User``) doesn't need to be specified.

.. code-block:: sdl-diff
    :version-lt: 3.0
    
      type User {
        required property email -> str;
        multi link friends -> User;
    +   link all_links := .<author;
    +   link blog_links := .<author[is BlogPost];
    +   link comment_links := .<author[is Comment];
      }

      type BlogPost {
        required property title -> str;
        required link author -> User;
      }
      type Comment {
        required property text -> str;
        required link author -> User;
      }

.. code-block:: sdl-diff
    :version-lt: 4.0

      type User {
        required email: str;
        multi friends: User;
    +   link all_links := .<author;
    +   link blog_links := .<author[is BlogPost];
    +   link comment_links := .<author[is Comment];
      }

      type BlogPost {
        required title: str;
        required author: User;
      }
      type Comment {
        required text: str;
        required author: User;
      }

.. code-block:: sdl-diff

      type User {
        required email: str;
        multi friends: User;
    +   all_links := .<author;
    +   blog_links := .<author[is BlogPost];
    +   comment_links := .<author[is Comment];
      }

      type BlogPost {
        required title: str;
        required author: User;
      }
      type Comment {
        required text: str;
        required author: User;
      }

.. _ref_eql_paths_link_props:

Link properties
---------------

Paths can also reference :ref:`link properties <ref_datamodel_link_properties>`
with ``@`` notation. To demonstrate this, let's add a property to the ``User.
friends`` link:

.. code-block:: sdl-diff
    :version-lt: 3.0

      type User {
        required property email -> str;
    -   multi link friends -> User;
    +   multi link friends -> User {
    +     property since -> cal::local_date;
    +   }
      }

.. code-block:: sdl-diff

      type User {
        required email: str;
    -   multi friends: User;
    +   multi friends: User {
    +     since: cal::local_date;
    +   }
      }

The following represents a set of all dates on which friendships were formed.

.. code-block:: edgeql

    select User.friends@since;

Path roots
----------

For simplicity, all examples above use set references like ``User`` as the root
of the path; however, the root can be *any expression* returning object types.
Below, the root of the path is a *subquery*.

.. code-block:: edgeql-repl

    db> with edgedb_lovers := (
    ...   select BlogPost filter .title ilike "EdgeDB is awesome"
    ... )
    ... select edgedb_lovers.author;

This expression returns a set of all ``Users`` who have written a blog post
titled "EdgeDB is awesome".

For a full syntax definition, see the :ref:`Reference > Paths
<ref_reference_paths>`.
