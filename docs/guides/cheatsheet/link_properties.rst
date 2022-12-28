.. _ref_guide_linkprops:

=====================
Using link properties
=====================

:index: property


Links can contain **properties**. These are distinct from links themselves
(which we refer to as simply "links") and are used to store metadata about a
link. Due to how they're persisted under the hood, link properties have a few
additional constraints: they're always *single* and *optional*.

.. note::

  In practice, link properties are best used with many-to-many relationships
  (``multi`` links without any exclusive constraints). For one-to-one,
  one-to-many, and many-to-one relationships the same data should be stored in
  object properties instead.


Declaration
-----------

Let's a create a ``Person.friends`` link with a ``strength`` property
corresponding to the strength of the friendship.

.. code-block:: sdl

  type Person {
    required property name -> str { constraint exclusive };

    multi link friends -> Person {
      property strength -> float64;
    }
  }

Constraints
-----------

.. code-block:: sdl

  type Person {
    required property name -> str { constraint exclusive };

    multi link friends -> Person {
      property strength -> float64;
      constraint expression on (
        __subject__@strength >= 0
      );
    }
  }

Indexes
-------

To index on a link property, you must declare an abstract link and extend it.

.. code-block:: sdl

  abstract link friendship {
    property strength -> float64;
    index on (__subject__@strength);
  }

  type Person {
    required property name -> str { constraint exclusive };
    multi link friends extending friendship -> Person;
  }


Inserting
---------

The ``@strength`` property is specified in the *shape* of the ``select``
subquery. This is only valid in a subquery *inside* an ``insert`` statement.

.. code-block:: edgeql

  insert Person {
    name := "Bob",
    friends := (
      select detached Person {
        @strength := 3.14
      }
      filter .name = "Alice"
    )
  }


.. note::

  We are using the :eql:op:`detached` operator to unbind the
  ``Person`` reference from the scope of the ``insert`` query.


When doing a nested insert, link properties can be directly included in the
inner ``insert`` subquery.

.. code-block:: edgeql

  insert Person {
    name := "Bob",
    friends := (
      insert Person {
        name := "Jane",
        @strength := 3.14
      }
    )
  }

Updating
--------

.. code-block:: edgeql

  update Person
  filter .name = "Bob"
  set {
    friends += (
      select .friends {
        @strength := 3.7
      }
      filter .name = "Alice"
    )
  };

The example updates the ``@strength`` property of Bob's friends link to
Alice to 3.7.

In the context of multi links the += operator works like an an insert/update
operator.

To update one or more links in a multi link, you can select from the current
linked objects, as the example does. Use a ``detached`` selection if you
want to insert/update a wider selection of linked objects instead.


Querying
--------

.. code-block:: edgeql-repl

  edgedb> select Person {
  .......   friends: {
  .......     name,
  .......     @strength
  .......   }
  ....... };
  {
    default::Person {name: 'Alice', friends: {}},
    default::Person {
      name: 'Bob',
      friends: {
        default::Person {name: 'Alice', @strength: 3.7}
      }
    },
  }

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Data Model > Links > Link properties
      <ref_datamodel_link_properties>`
  * - :ref:`SDL > Properties <ref_eql_sdl_props>`
  * - :ref:`DDL > Properties <ref_eql_ddl_props>`
  * - :ref:`Introspection > Object Types <ref_eql_introspection_object_types>`
