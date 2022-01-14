.. _ref_eql_paths:

=====
Paths
=====


A *path expression* (or simply a *path*) represents a set of values that are
reachable by traversing a given sequence of links or properties from some
source set of objects.

Consider the following schema:

.. code-block:: sdl

  type User {
    required property email -> str;
    multi link friends -> User;
  }

  type BlogPost {
    required property title -> str;
    required link author -> User;
  }

The simplest path is simply ``User``. This is a :ref:`set reference
<ref_eql_set_references>` that refers to all ``User`` objects in the database.

.. code-block:: edgeql

  select User;

Paths can traverse links. The path below refers to *all Users who are the
friend of another User*.

.. code-block:: edgeql

  select User.friends;

Paths can traverse arbitrarily many links.

.. code-block:: edgeql

  select BlogPost.author.friends.friends;

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

As written, EdgeDB infers the *type* of this expression to be
:eql:type:`BaseObject`, not ``BlogPost``. Why? Because in theory, there may be
several links named ``author`` that point to ``User``.

.. note::
  ``BaseObject`` is the root ancestor of all object types and it only contains
  a single property, ``id``.

Consider the following addition to the schema:

.. code-block:: sdl-diff

    type User {
      # as before
    }

    type BlogPost {
      required link author -> User;
    }

  + type Comment {
  +   required link author -> User;
  + }

With the above schema, the path ``User.<author`` would return a mixed set of
``BlogPost`` and ``Comment`` objects. This may be desirable in some cases, but
commonly you'll want to narrow the results to a particular type. To do so, use
the :eql:op:`type intersection <isintersect>` operator: ``[is Foo]``:

.. code-block:: edgeql

    select User.<author[is BlogPost]; # returns all blog posts
    select User.<author[is Comment]; # returns all comments


.. _ref_eql_paths_link_props:

Link properties
---------------

Paths can also reference :ref:`link properties <ref_datamodel_link_properties>`
with ``@`` notation. To demonstrate this, let's add a property to the ``User.
friends`` link:

.. code-block:: sdl-diff

    type User {
      required property email -> str;
  -   multi link friends -> User;
  +   multi link friends -> User {
  +     property since -> cal::local_date;
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
