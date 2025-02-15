.. _ref_datamodel_indexes:

=======
Indexes
=======

.. index::
   index on, performance, postgres query planner

An index is a data structure used internally to speed up filtering, ordering,
and grouping operations in |Gel|. Indexes help accomplish this in two key ways:

- They are pre-sorted, which saves time on costly sort operations on rows.
- They can be used by the query planner to filter out irrelevant rows.

.. note::

   The Postgres query planner decides when to use indexes for a query. In some
   cases—e.g. when tables are small—it may be faster to scan the whole table
   rather than use an index. In such scenarios, the index might be ignored.

   For more information on how the planner decides this, see
   `the Postgres query planner documentation
   <https://www.postgresql.org/docs/current/planner-optimizer.html>`_.


Tradeoffs
=========

While improving query performance, indexes also increase disk and memory usage
and can slow down insertions and updates. Creating too many indexes may be
detrimental; only index properties you often filter, order, or group by.

.. important::
   **Foreign and primary keys**

   In SQL databases, indexes are commonly used to index *primary keys* and
   *foreign keys*. Gel's analog to a SQL primary key is the ``id`` field
   automatically created for each object, while a link in Gel is the analog
   to a SQL foreign key. Both of these are automatically indexed.

   Moreover, any property with an :eql:constraint:`exclusive` constraint
   is also automatically indexed.


Index on a property
===================

Most commonly, indexes are declared within object type declarations and
reference a particular property. The index can be used to speed up queries
that reference that property in a filter, order by, or group by clause:

.. code-block:: sdl

   type User {
     required name: str;
     index on (.name);
   }

By indexing on ``User.name``, the query planner will have access to that index
when planning queries using the ``name`` property. This may result in better
performance as the database can look up a name in the index instead of scanning
through all ``User`` objects sequentially—though ultimately it's up to the
Postgres query planner whether to use the index.

To see if an index helps, compare query plans by adding
:ref:`analyze <ref_cli_gel_analyze>` to your queries.

.. note::

   Even if your database is small now, you may benefit from an index as it grows.


Index on an expression
======================

Indexes may be defined using an arbitrary *singleton* expression that
references multiple properties of the enclosing object type.

.. important::
   A singleton expression is an expression that's guaranteed to return
   *at most one* element. As such, you can't index on a ``multi`` property.

Example:

.. code-block:: sdl

   type User {
     required first_name: str;
     required last_name: str;
     index on (str_lower(.first_name + ' ' + .last_name));
   }


Index on multiple properties
============================

.. index:: tuple

A *composite index* references multiple properties. This can speed up queries
that filter, order, or group on multiple properties at once.

.. note::

   An index on multiple properties may also be used in queries where only a
   single property in the index is referenced. In many traditional database
   systems, placing the most frequently used columns first in the composite
   index can improve the likelihood of its use.

   Read `the Postgres documentation on multicolumn indexes
   <https://www.postgresql.org/docs/current/indexes-multicolumn.html>`_ to learn
   more about how the query planner uses these indexes.

In |Gel|, a composite index is created by indexing on a ``tuple`` of properties:

.. code-block:: sdl

   type User {
     required name: str;
     required email: str;
     index on ((.name, .email));
   }


Index on a link property
========================

.. index:: __subject__, linkprops

Link properties can also be indexed. The special placeholder
``__subject__`` refers to the source object in a link property expression:

.. code-block:: sdl

   abstract link friendship {
     strength: float64;
     index on (__subject__@strength);
   }

   type User {
     multi friends: User {
       extending friendship;
     };
   }


Specify a Postgres index type
=============================

.. index:: pg::hash, pg::btree, pg::gin, pg::gist, pg::spgist, pg::brin

.. versionadded:: 3.0

Gel exposes Postgres index types that can be used directly in schemas via
the ``pg`` module:

- ``pg::hash`` : Index based on a 32-bit hash of the value
- ``pg::btree`` : B-tree index (can help with sorted data retrieval)
- ``pg::gin`` : Inverted index for multi-element data (arrays, JSON)
- ``pg::gist`` : Generalized Search Tree for range and geometric searches
- ``pg::spgist`` : Space-partitioned GiST
- ``pg::brin`` : Block Range INdex

Example:

.. code-block:: sdl

   type User {
     required name: str;
     index pg::spgist on (.name);
   }


Annotate an index
=================

.. index:: annotation

Indexes can include annotations:

.. code-block:: sdl

   type User {
     name: str;
     index on (.name) {
       annotation description := 'Indexing all users by name.';
     };
   }


.. _ref_eql_sdl_indexes:

SDL declarations
================

|Gel's| Schema Definition Language (SDL) supports inline index declarations,
as seen above. The following syntax details how to declare an index in SDL.

Syntax
------

.. sdl:synopsis::

   index on ( <index-expr> )
   [ except ( <except-expr> ) ]
   [ "{" <annotation-declarations> "}" ] ;

.. rubric:: Description

- :sdl:synopsis:`on ( <index-expr> )`

  The expression to index. It must be :ref:`Immutable <ref_reference_volatility>`
  but may refer to the indexed object's properties/links. The expression itself
  must be parenthesized.

- :eql:synopsis:`except ( <except-expr> )`

  An optional condition. If ``<except-expr>`` evaluates to ``true``, the object
  is omitted from the index; if ``false`` or empty, it is included.

- :sdl:synopsis:`<annotation-declarations>`

  Allows setting index :ref:`annotation <ref_eql_sdl_annotations>` to a given
  value.


.. _ref_eql_ddl_indexes:

DDL commands
============

This section describes the DDL statements for creating, altering, and
dropping indexes.

Create index
------------

:eql-statement:

.. eql:synopsis::

   create index on ( <index-expr> )
   [ except ( <except-expr> ) ]
   [ "{" <subcommand>; [...] "}" ] ;

   # where <subcommand> is one of

     create annotation <annotation-name> := <value>

Creates a new index for a given object type or link using *index-expr*.

- Most parameters/options match those in
  :ref:`SDL > Indexes <ref_eql_sdl_indexes>`.

- Allowed subcommand:

  :eql:synopsis:`create annotation <annotation-name> := <value>`
     Assign an annotation to this index.
     See :eql:stmt:`create annotation` for details.

Example:

.. code-block:: edgeql

   create type User {
     create property name -> str {
       set default := '';
     };

     create index on (.name);
   };


Alter index
-----------

:eql-statement:

Alter the definition of an index.

.. eql:synopsis::

   alter index on ( <index-expr> ) [ except ( <except-expr> ) ]
   [ "{" <subcommand>; [...] "}" ] ;

   # where <subcommand> is one of

     create annotation <annotation-name> := <value>
     alter annotation <annotation-name> := <value>
     drop annotation <annotation-name>

The command ``alter index`` is used to change the :ref:`annotations
<ref_datamodel_annotations>` of an index. The *index-expr* is used to
identify the index to be altered.

:sdl:synopsis:`on ( <index-expr> )`
    The specific expression for which the index is made.  Note also
    that ``<index-expr>`` itself has to be parenthesized.

The following subcommands are allowed in the ``alter index`` block:

:eql:synopsis:`create annotation <annotation-name> := <value>`
    Set index :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.
    See :eql:stmt:`create annotation` for details.

:eql:synopsis:`alter annotation <annotation-name>;`
    Alter index :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>;`
    Remove constraint :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`drop annotation` for details.


Example:

.. code-block:: edgeql

   alter type User {
     alter index on (.name) {
       create annotation title := 'User name index';
     };
   };


Drop index
----------

:eql-statement:

Remove an index from a given schema item.

.. eql:synopsis::

   drop index on ( <index-expr> ) [ except ( <except-expr> ) ] ;

Removes an index from a schema item.

- :sdl:synopsis:`on ( <index-expr> )` identifies the indexed expression.

This statement can only be used as a subdefinition in another DDL statement.

Example:

.. code-block:: edgeql

   alter type User {
     drop index on (.name);
   };


.. list-table::
   :class: seealso

   * - **See also**
     - :ref:`Introspection > Indexes <ref_datamodel_introspection_indexes>`
