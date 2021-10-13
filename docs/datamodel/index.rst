.. eql:section-intro-page:: datamodel
.. _ref_datamodel_intro:

.. _ref_datamodel_index:

===============
Schema Modeling
===============

One of EdgeDB's foundational features is **declarative schema modeling**.

.. toctree::
    :maxdepth: 3
    :hidden:


    objects/index
    scalars
    colltypes
    links
    props
    linkprops
    computables
    indexes
    constraints
    aliases
    functions
    inheritance
    annotations
    modules
    extensions
    comparison


With EdgeDB, you can define your schema in a readable, object-oriented way with
EdgeDB's schema definition language (usually referred to as "EdgeDB SDL" or
simply "SDL"). It's similar to defining models with an ORM library.

.. code-block:: sdl

  type Movie {
    required property title -> str;
    required link director -> Person;
  }

  type Person {
    required property name -> str;
  }


Properties of SDL
-----------------

SDL has two important properties. First, it's **declarative**; you can just
write your schema down exactly as you want it to be. It's easy to see the
current state of your schema at a glance.

Secondly, it's **object-oriented**. There are no foreign keys; instead,
relationships between types are represented with :ref:`Links
<ref_datamodel_links>`; this is part of what makes EdgeQL queries so concise
and powerful:

.. code-block:: edgeql

  SELECT Movie {
    title,
    director: {
      name
    }
  }

.. _ref_datamodel_terminology:

Terminology
-----------

.. important::

  Below is an overview of EdgeDB's terminology. Use it as a roadmap, but don't
  worry if it doesn't all make sense immediately. The following pages go into
  detail on each concept below.

A running EdgeDB process is known as an **instance**. Each instance can contain
several **databases**, each with a unique name. By default, an instance
contains a single database called ``edgedb``.

Databases can contain several **modules**. Modules have a unique name and can
be used to organize large schemas into logical units. Most users put their
entire schema inside a single module called ``default``.

The EdgeDB equivalent of SQL tables are **object types** (e.g. ``User``).
Object types contain **properties** and **links**. Both properties and links
are associated with a unique name (e.g. ``first_name``) and a cardinality,
which can be either **single** (the default) or **multi**.

Properties correspond to either a **scalar type** (e.g. ``str``, ``int64``) or
a **collection type** like an array or a tuple. Links represent relationships
between object types.

Links and properties can also be **computed**. Computed links and properties
are not physically stored in the database, but they can used in queries just
like non-computed ones. The value will be computed as needed.

Object types can also be **abstract** (e.g. ``HasEmailAddress``). Abstract
types can be *extended* by concrete object types of other abstract types, in
which case the extending type inherits all of its properties and links.

Object types can be augmented with **indexes** to speed up certain queries.
Object types, properties, and links can all be augmented with **annotations**
(readable notes for others) and **constraints**

You can define **expression aliases**, which lets you define additional
computed properties and links on non-abstract object types, without modifying
the original type. You can also define custom **functions**.


See also
--------

**Migrations**
  When you make changes to your schema file, you need to create and execute a
  *migration* to update your database. EdgeDB has a built-in migration system
  that intelligently determines *what has changed* since the last migration and
  how to update the schema to the new version. Read the :ref:`Migration docs
  <ref_docs_migrations>` to learn more.


**DDL**
  Under the hood, all schema modifications are the result of *data definition
  language* (DDL) commands. DDL is a set of lower-level, imperative commands analogous to SQL's ``CREATE TABLE``, etc. It is the foundation on which EdgeDB's migration system is built.

  We recommend that most users use SDL and migrations when building
  applications. However, if you prefer SQL-style imperative schema modeling,
  you are free to use DDL directly; reference the :ref:`DDL docs <ref_eql_ddl>`
  to learn more.
