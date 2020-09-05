.. _ref_datamodel_annotations:

===========
Annotations
===========

*Annotations* are named values associated with schema items and
are designed to hold arbitrary schema-level metadata represented as a
:eql:type:`str`.



Standard Annotations
====================

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

The above annotations can be extracted via schema introspection queries
and used to create a descriptive UI for an admin tool:

.. code-block:: edgeql-repl

    db> WITH MODULE schema
    ... SELECT ObjectType {
    ...     name,
    ...     annotations: {
    ...         name,
    ...         @value
    ...     }
    ... }
    ... FILTER .name = 'default::Status';
    {
        Object {
            name: 'default::Status',
            annotations: {
                Object {
                    name: 'std::description',
                    @value: 'All possible user activities'
                },
                Object {
                    name: 'std::title',
                    @value: 'Activity status'
                }
            }
        }
    }

The ``deprecated`` annotation is used to mark deprecated items (e.g.
:eql:func:`str_rpad`) and to provide some information such as what
should be used instead.


See Also
--------

Annotation
:ref:`SDL <ref_eql_sdl_annotations>`,
:ref:`DDL <ref_eql_ddl_annotations>`,
and :ref:`introspection <ref_eql_introspection>`.
