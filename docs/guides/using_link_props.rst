.. _ref_guide_linkprops:

=====================
Using link properties
=====================

:index: property


Links can contain **properties**. Due to how they're persisted under the hood,
link properties have a few additional constraints: they're always *single* and
*optional*.

Declaration
-----------

.. code-block:: sdl

  type Person {
    required property name -> str { constraint exclusive; }

    multi link friends -> Person {
      property strength -> float64;
    }
  }


Insertion
---------

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

Updating
--------

.. code-block:: edgeql

  update Person
  filter .name = "Bob"
  set {
    friends += (
      select detached Person {
        @strength := 3.7
      }
      filter .name = "Alice"
    )
  };

Querying
--------

.. code-block:: edgeql-repl

  edgedb> select Person {
  .......   friends: {
  .......     name,
  .......     @strength
  .......   }
  ....... }
  {
    default::Person {name: 'Alice', friends: {}},
    default::Person {
      name: 'Bob',
      friends: {
        default::Person {name: 'Alice', @strength: 3.7}
      }
    },
  }


See Also
--------

Property
:ref:`Data Model <ref_datamodel_linkprops>`, :ref:`SDL <ref_eql_sdl_props>`,
:ref:`DDL <ref_eql_ddl_props>`,
and :ref:`introspection <ref_eql_introspection_object_types>`
(as part of overall object introspection).
