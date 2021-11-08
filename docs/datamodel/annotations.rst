.. _ref_datamodel_annotations:

===========
Annotations
===========

*Annotations* are named values associated with schema items and
are designed to hold arbitrary schema-level metadata represented as a
:eql:type:`str`.


Standard annotations
--------------------

There is a number of annotations defined in the standard library.
The following are the annotations which can be set on any schema item:

- ``title``
- ``description``
- ``deprecated``

For example, consider the following declaration:

.. code-block:: sdl

    type Status {
        annotation title := 'Activity status';
        annotation description := 'All possible user activities';

        required property name -> str {
            constraint exclusive
        }
    }

The ``deprecated`` annotation is used to mark deprecated items (e.g.
:eql:func:`str_rpad`) and to provide some information such as what
should be used instead.


User-defined annotations
------------------------

To declare a custom constraint type beyond the three built-ins, add an abstract
annotation type to your schema.

.. code-block:: sdl

  abstract annotation admin_note;

  type Status {
    annotation admin_note := 'system-critical';
  }


See Also
--------

Annotation
:ref:`SDL <ref_eql_sdl_annotations>`,
:ref:`DDL <ref_eql_ddl_annotations>`,
and :ref:`introspection <ref_eql_introspection>`.
