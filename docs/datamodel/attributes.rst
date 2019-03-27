.. _ref_datamodel_attributes:

==========
Attributes
==========

*Attributes* are named values associated with schema items and
are designed to hold arbitrary schema-level metadata represented as a
:eql:type:`str`.


Standard Attributes
===================

There is a number of attributes defined in the standard library.  The following
are the attributes which can be set on any schema item:

- ``title``
- ``description``

For example, consider the following declaration:

.. code-block:: sdl

    type Status {
        attribute title := 'Activity status';
        attribute description := 'All possible user activities';

        required property name -> str {
            constraint exclusive
        }
    }

The above attributes can be extracted via schema introspection queries
and used to create a descriptive UI for an admin tool:

.. code-block:: edgeql-repl

    db> WITH MODULE schema
    ... SELECT ObjectType {
    ...     name,
    ...     attributes: {
    ...         name,
    ...         @value
    ...     }
    ... }
    ... FILTER .name = 'default::Status';
    {
        Object {
            name: 'default::Status',
            attributes: {
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
