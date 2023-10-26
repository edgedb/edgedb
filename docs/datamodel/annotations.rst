.. _ref_datamodel_annotations:

===========
Annotations
===========

*Annotations* are named values associated with schema items and
are designed to hold arbitrary schema-level metadata represented as a
:eql:type:`str`.


Standard annotations
--------------------

There are a number of annotations defined in the standard library.
The following are the annotations which can be set on any schema item:

- ``title``
- ``description``
- ``deprecated``

For example, consider the following declaration:

.. code-block:: sdl
    :version-lt: 3.0

    type Status {
        annotation title := 'Activity status';
        annotation description := 'All possible user activities';

        required property name -> str {
            constraint exclusive
        }
    }

.. code-block:: sdl

    type Status {
        annotation title := 'Activity status';
        annotation description := 'All possible user activities';

        required name: str {
            constraint exclusive
        }
    }

The ``deprecated`` annotation is used to mark deprecated items (e.g.
:eql:func:`str_rpad`) and to provide some information such as what
should be used instead.


User-defined annotations
------------------------

To declare a custom annotation type beyond the three built-ins, add an abstract
annotation type to your schema. A custom annotation could be used to attach
arbitrary JSON-encoded data to your schema—potentially useful for introspection
and code generation.

.. code-block:: sdl

  abstract annotation admin_note;

  type Status {
    annotation admin_note := 'system-critical';
  }


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Annotations <ref_eql_sdl_annotations>`
  * - :ref:`DDL > Annotations <ref_eql_ddl_annotations>`
  * - :ref:`Cheatsheets > Annotations <ref_cheatsheet_annotations>`
  * - :ref:`Introspection > Object types
      <ref_datamodel_introspection_object_types>`
