.. _ref_datamodel_indexes:

=======
Indexes
=======

An index is a data structure used internally to speed up filtering, ordering,
and grouping operations. Most commonly, indexes are declared within object
type declarations and reference a particular property; this will speed up
any query that references that property in a ``filter``, ``order by``, or
``group`` clause.

.. note::

  While improving query performance, indexes also increase disk and memory
  usage and slow down insertions and updates. Creating too many indexes may be
  detrimental; only index properties you often filter, order, or group by.

Index on a property
-------------------

Below, we are referencing the ``User.name`` property with the :ref:`dot
notation shorthand <ref_dot_notation>`: ``.name``.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property name -> str;
      index on (.name);
    }

.. code-block:: sdl

    type User {
      required name: str;
      index on (.name);
    }

By indexing on ``User.name``, queries that filter, order, or group by the 
``name`` property will be faster, as the database can look up a name in 
the index instead of scanning through all ``User`` objects sequentially.

To see the difference for yourself, try adding the :ref:`analyze
<ref_cli_edgedb_analyze>` keyword before a query with an index compared
to one without.

Index on an expression
----------------------

Indexes may be defined using an arbitrary *singleton* expression that
references multiple properties of the enclosing object type.

.. important::

  A singleton expression is an expression that's guaranteed to return *at most
  one* element. As such, you can't index on a ``multi`` property.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property first_name -> str;
      required property last_name -> str;
      index on (str_lower(.firstname + ' ' + .lastname));
    }

.. code-block:: sdl

    type User {
      required first_name: str;
      required last_name: str;
      index on (str_lower(.firstname + ' ' + .lastname));
    }

Index on multiple properties
----------------------------

A *composite index* is an index that references multiple properties. This will
speed up queries that filter, order, or group on *both properties*. In EdgeDB,
this is accomplished by indexing on a ``tuple`` of properties.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property name -> str;
      required property email -> str;
      index on ((.name, .email));
    }

.. code-block:: sdl

    type User {
      required name: str;
      required email: str;
      index on ((.name, .email));
    }

Index on a link property
------------------------

Link properties can also be indexed.

.. code-block:: sdl
    :version-lt: 3.0

    abstract link friendship {
      property strength -> float64;
      index on (__subject__@strength);
    }

    type User {
      multi link friends extending friendship -> User;
    }

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
-----------------------------

.. versionadded:: 3.0

EdgeDB exposes Postgres indexes that you can use in your schemas. These are
exposed through the ``pg`` module.

* ``pg::hash``- Index based on a 32-bit hash derived from the indexed value

* ``pg::btree``- B-tree index can be used to retrieve data in sorted order

* ``pg::gin``- GIN is an "inverted index" appropriate for data values that
  contain multiple elements, such as arrays and JSON

* ``pg::gist``- GIST index can be used to optimize searches involving ranges

* ``pg::spgist``- SP-GIST index can be used to optimize searches involving
  ranges and strings

* ``pg::brin``- BRIN (Block Range INdex) index works with summaries about the
  values stored in consecutive physical block ranges in the database

You can use them like this:

.. code-block:: sdl

    type User {
      required property name -> str;
      index pg::spgist on (.name);
    };

Annotate an index
-----------------

Indexes can be augmented with annotations.

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      property name -> str;
      index on (.name) {
        annotation description := 'Indexing all users by name.';
      };
    }

.. code-block:: sdl

    type User {
      name: str;
      index on (.name) {
        annotation description := 'Indexing all users by name.';
      };
    }

.. important::

  **Foreign and primary keys**

  In SQL databases, indexes are commonly used to index *primary keys* and
  *foreign keys*. EdgeDB's analog to SQL's primary key is the ``id`` field
  that gets automatically created for each object, while a link in EdgeDB
  is the analog to SQL's foreign key. Both of these are automatically indexed.
  Moreover, any property with an :eql:constraint:`exclusive` constraint
  is also automatically indexed.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Indexes <ref_eql_sdl_indexes>`
  * - :ref:`DDL > Indexes <ref_eql_ddl_indexes>`
  * - :ref:`Introspection > Indexes <ref_datamodel_introspection_indexes>`
