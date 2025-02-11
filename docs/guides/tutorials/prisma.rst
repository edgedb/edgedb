.. _ref_guide_prisma:

======
Prisma
======

|Gel| supports SQL protocol for communicating with the database. This makes it possible to use it with Prisma by matching a Prisma schema to the |Gel| schema. You don't even need to figure out the conversion yourself because we have an automated tool for that.

This tool becomes available when you install ``gel`` JavaScript/TypeScript package. Once you have you |Gel| project setup, you can generate the Prisma schema with this command:

.. code-block:: bash

  $ npx @gel/generate prisma --file schema.prisma

The ``--file`` indicates the output file for the newly generated Prisma schema.

Even though Prisma and |Gel| both view the schema as a bunch of types with fields and interconnections, there are still some differences between what |Gel| and Prisma can represent.


Properties
==========

Property types must match the basic Postgres types supported by Prisma, so avoid any custom types as they will be skipped. Currently we support the following:

* :eql:type:`std::uuid` - ``String @db.Uuid``
* :eql:type:`std::bigint` - ``Decimal``
* :eql:type:`std::bool` - ``Boolean``
* :eql:type:`std::bytes` - ``Bytes``
* :eql:type:`std::decimal` - ``Decimal``
* :eql:type:`std::float32` - ``Float``
* :eql:type:`std::float64` - ``Float``
* :eql:type:`std::int16` - ``Int``
* :eql:type:`std::int32` - ``Int``
* :eql:type:`std::int64` - ``BigInt``
* :eql:type:`std::json` - ``Json``
* :eql:type:`std::str` - ``String``
* :eql:type:`std::datetime` - ``DateTime``

Array properties are supported for all of the above types as well.

Multi properties cannot be represented as they have no primary key and therefore rows cannot be uniquely identified. That means that the schema generator will omit them from the schema. If you needs to reflect multi properties, consider replacing them with a single array property.


Links
=====

Plain single links are reflected as a relation.

Multi links get represented as a many-to-many relationship with an implicit intermediary table.

Prisma is quite opinionated about the underlying SQL tables. It has a strict naming requirement for implicit link tables (they **must** start with an ``_``). This means that the way |Gel| exposes link tables is incompatible with the implicit naming requirement. So multi links and links with link properties have to reflected as explicit intermediate objects in a Prisma schema. These intermediary objects have ``source`` and ``target`` relations to the end points of the link. The link properties (if any) then become the fields of this link object.

All links automatically generate the backward relations as well. The name of these back-links takes the format of ``bk_linkname_SourceName``, which mimics the EdgeQL version of backlinks ``.<linkname[is SourceName]`` format.


Modules
=======

Currently multiple modules are not supported for reflection to Prisma. Only the ``default`` module will be reflected. This limitation comes from a very different way |Gel| and Prisma view multiple Postgres schemas.


Connection String
=================

Prisma requires a Postgres connection string in order to operate with |Gel|. One way to get that string is by using :gelcmd:`instance credentials --insecure-dsn` and replacing the protocol name with ``postgresql``.


Example
=======

Let's explore how conversion process works using a small example. Consider a project the following schema:

.. code-block:: sdl
  :caption: dbschema/default.gel

  module default {
    type UserGroup {
      required name: str;
      multi link users: User;
    }

    type User {
      required name: str;
    }

    type Post {
      required body: str;
      required link author: User;
    }
  }

This may be part of a system that has users who can belong to a bunch of groups and have the ability to post notes. This gives us some basic relationship types:

* many-to-one for ``Post`` link ``author``
* many-to-many for ``UserGroup`` multi link ``users``

Once the project is initialized and the schema has been applied to the |Gel| database we can run the conversion command:

.. code-block:: bash

  $ npx @gel/generate prisma --file schema.prisma

The command will produce the following file:

.. code-block::

    // Automatically generated from Gel schema.
    // Do not edit directly as re-generating this file will overwrite any changes.

    generator client {
      provider = "prisma-client-js"
    }

    datasource db {
      provider = "postgresql"
      url      = env("DATABASE_URL")
    }

    model Post {
      id    String    @id    @default(dbgenerated("uuid_generate_v4()"))    @db.Uuid
      gel_type_id    String    @default(dbgenerated("uuid_generate_v4()"))    @map("__type__")    @db.Uuid

      // properties
      author_id    String    @db.Uuid
      body    String?

      // links
      author    User    @relation("bk_author_Post", fields: [author_id], references: [id], onUpdate: NoAction, onDelete: NoAction)

      @@map("Post")
    }

    model User {
      id    String    @id    @default(dbgenerated("uuid_generate_v4()"))    @db.Uuid
      gel_type_id    String    @default(dbgenerated("uuid_generate_v4()"))    @map("__type__")    @db.Uuid

      // properties
      name    String?

      // multi-links
      bk_users_UserGroup    UserGroup_users[]    @relation("UserGroup_users")

      // backlinks
      bk_author_Post    Post[]    @relation("bk_author_Post")

      @@map("User")
    }

    model UserGroup {
      id    String    @id    @default(dbgenerated("uuid_generate_v4()"))    @db.Uuid
      gel_type_id    String    @default(dbgenerated("uuid_generate_v4()"))    @map("__type__")    @db.Uuid

      // properties
      name    String?

      // multi-links
      users    UserGroup_users[]    @relation("bk_users_UserGroup")

      @@map("UserGroup")
    }

    model UserGroup_users {

      // properties
      source_id    String    @map("source")    @db.Uuid
      target_id    String    @map("target")    @db.Uuid

      // links
      source    UserGroup    @relation("bk_users_UserGroup", fields: [source_id], references: [id], onUpdate: NoAction, onDelete: NoAction)
      target    User    @relation("UserGroup_users", fields: [target_id], references: [id], onUpdate: NoAction, onDelete: NoAction)

      @@id([source_id, target_id])
      @@map("UserGroup.users")
    }

We have the ``Post``, ``User``, and ``UserGroup`` models with their fields and relations. All models have two fields in common: ``id`` and ``gel_type_id``. They refer to the unique object ``id`` and to the ``__type__.id`` in the |Gel| schema. These two UUID fields are managed automatically by |Gel| and should not be directly modified. Effectively they are supposed to be treated as read-only fields.

At the end of the schema there's the model corresponding to the link table which represents the many-to-many relationship ``users`` between ``UserGroup`` and ``User``. All such intermediate tables will contain ``source`` and ``target`` relations as well as the corresponding ``source_id`` and ``target_id``. Both ``source_id`` and ``target_id`` are used as a composite ``@@id``. The name of the table is automatically generated as ``<Type>_<link>``.

Properties
----------

The |Gel| schema declares a few properties: ``name`` for ``User`` and ``UserGroup`` as well as ``body`` for ``Post``. These get reflected as ``String`` in the corresponding models. As long as a property has a valid corresponding Prisma field type it will be reflected in this manner.

Links
-----

Let's first look at the ``Post`` declaration in |Gel|. A ``Post`` has a link ``author`` pointing to a ``User``. So the reflected type ``Post`` has a field ``author_id`` and the corresponding relation ``author``.

Each reflected relation also automatically declares a back-link. In order to correctly map links and back-links every relation needs a name. We simply use the name of the back-link as the name of the relation. The naming format is ``bk_<link>_<source-Type>``. For the ``author`` link the name of the back-link is ``bk_author_Post`` and so is the name of the relation.

We can look at the ``User`` model and find ``bk_author_Post`` relation used as a back-link of the same name. This relation is pointing back to ``Post[]``.

``User`` model also has a many-to-many relationship with ``UserGroup``. Both ``User`` and ``UserGroup`` are connected by the ``UserGroup_users`` model. The relation names for ``UserGroup`` is the same as in the original |Gel| schema - ``users``. On the other hand the ``User`` model follows the back-link naming convention for this relation - ``bk_users_UserGroup``.

Finally, ``UserGroup_users`` model has the last part of the many-to-many relationship declaration. The ``source`` relation pointing to ``UserGroup`` and the ``target`` relation pointing to ``User``.

Connection
----------

In order to use these generated models in your Prisma app you need to setup the ``DATABASE_URL``. Typically this is done in the ``.env`` file.

Running :gelcmd:`instance credentials --insecure-dsn` command produces something like this:

.. code-block:: bash

    $ gel instance credentials --insecure-dsn
    gel://admin:h632hKRuss6i9uQeMgEvRsuQ@localhost:10715/main

All we have to do is replace the protocol with ``postgresql`` and add the following to ``.env``:

.. code-block::

    DATABASE_URL="postgresql://admin:h632hKRuss6i9uQeMgEvRsuQ@localhost:10715/main"
