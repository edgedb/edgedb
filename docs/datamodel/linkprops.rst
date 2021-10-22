.. _ref_datamodel_linkprops:

===============
Link Properties
===============

:index: property

.. important::

  For a full guide on modeling, inserting, updating, and querying link
  properties, see the :ref:`Using Link Properties <ref_guide_linkprops>` guide.

Like object types, links can contain **properties**. Link properties can be
used to store metadata about links, such as *when* it was created or the
*nature/strength* of the relationship.

.. code-block:: sdl

  type Person {
    multi link friends -> Person {
      property strength -> float64;
    };
  }

Due to how they're persisted under the hood, link properties have a couple
additional constraints: they're always ``single`` and ``optional``.

Example
-------

The schema below represents a family tree using a single object type,
``Person``. Instead of a separate ``Marriage`` type, we can directly store
relevant metadata about a marriage inside the ``Person.married_to`` link.

.. code-block:: sdl

  type Person {
    required property name -> str;

    multi link married_to -> Person {
      property marriage_date -> cal::local_date;
      property divorce_date -> cal::local_date;
    }

    multi link children -> Person {
      property adopted -> bool;
    }
  }

See Also
--------

For a full guide on modeling, inserting, updating, and querying link
properties, see the :ref:`Using link properties <ref_guide_linkprops>` guide.

Property
:ref:`SDL <ref_eql_sdl_props>`,
:ref:`DDL <ref_eql_ddl_props>`,
and :ref:`introspection <ref_eql_introspection_object_types>`
(as part of overall object introspection).
