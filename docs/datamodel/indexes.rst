.. _ref_datamodel_indexes:

=======
Indexes
=======

An index is a data structure used internally by a database to speed up
filtering and sorting operations. Most commonly, indexes are declared within
object type declarations and reference a particular property; this will speed
up any query that references that property in a ``filter`` or ``order by``
clause.

.. note::

  While improving query performance, indexes also increase disk and memory
  usage and slow down insertions and updates. Creating too many indexes may be
  detrimental; only index properties you often filter or order by.

Index on a property
-------------------

Below, we are referencing the ``User.name`` property with the :ref:`dot
notation shorthand <ref_dot_notation>`: ``.name``.

.. code-block:: sdl

  type User {
    required property name -> str;
    index on (.name);
  }

By indexing on ``User.name``, queries that filter by the ``name`` property will
be faster, as the database can lookup a name in the index instead of scanning
through all Users sequentially.

Index on an expression
----------------------

Indexes may be defined using an arbitrary *singleton* expression that
references multiple properties of the enclosing object type.

.. important::

  A singleton expression is an expression that's guaranteed to return *at most
  one* element. As such, you can't index on a ``multi`` property.

.. code-block:: sdl

  type User {
    required property first_name -> str;
    required property last_name -> str;
    index on (str_lower(.firstname + ' ' + .lastname));
  }

Index on multiple properties
----------------------------

A *composite index* is an index that references multiple properties. This will
speed up queries that filter or sort on *both properties*. In EdgeDB, this is
accomplished by indexing on a ``tuple`` of properties.

.. code-block:: sdl

  type User {
    required property name -> str;
    required property email -> str;
    index on ((.name, .email));
  }

Index on a link property
------------------------

Link properties can also be indexed.

.. code-block:: sdl

  abstract link friendship {
    property strength -> float64;
    index on (__subject__@strength);
  }

  type User {
    multi link friends extending friendship -> User;
  }

Annotate an index
-----------------

Indexes can be augmented with annotations.

.. code-block:: sdl

  type User {
    property name -> str;
    index on (.name) {
      annotation description := 'Indexing all users by name.';
    };
  }

.. important::

  **Foreign and primary keys**

  In SQL databases, indexes are commonly used to index *primary keys* and
  *foreign keys*. In EdgeDB, these fields are automatically indexed; there's no
  need to manually declare them. Moreover, any property with an
  :eql:constraint:`exclusive` constraint is also automatically indexed.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Indexes <ref_eql_sdl_indexes>`
  * - :ref:`DDL > Indexes <ref_eql_ddl_indexes>`
  * - :ref:`Introspection > Indexes <ref_eql_introspection_indexes>`
