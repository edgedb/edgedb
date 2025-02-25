.. _ref_datamodel_introspection_scalar_types:

============
Scalar types
============

This section describes introspection of :ref:`scalar types
<ref_datamodel_scalar_types>`.

Introspection of the ``schema::ScalarType``:

.. code-block:: edgeql-repl

    db> with module schema
    ... select ObjectType {
    ...     name,
    ...     links: {
    ...         name,
    ...     },
    ...     properties: {
    ...         name,
    ...     }
    ... }
    ... filter .name = 'schema::ScalarType';
    {
        Object {
            name: 'schema::ScalarType',
            links: {
                Object { name: '__type__' },
                Object { name: 'annotations' },
                Object { name: 'bases' },
                Object { name: 'constraints' },
                Object { name: 'ancestors' }
            },
            properties: {
                Object { name: 'default' },
                Object { name: 'enum_values' },
                Object { name: 'id' },
                Object { name: 'abstract' },
                Object { name: 'name' }
            }
        }
    }

Introspection of the built-in scalar :eql:type:`str`:

.. code-block:: edgeql-repl

    db> with module schema
    ... select ScalarType {
    ...     name,
    ...     default,
    ...     enum_values,
    ...     abstract,
    ...     bases: { name },
    ...     ancestors: { name },
    ...     annotations: { name, @value },
    ...     constraints: { name },
    ... }
    ... filter .name = 'std::str';
    {
        Object {
            name: 'std::str',
            default: {},
            enum_values: {},
            abstract: {},
            bases: {Object { name: 'std::anyscalar' }},
            ancestors: {Object { name: 'std::anyscalar' }},
            annotations: {},
            constraints: {}
        }
    }

For an :ref:`enumerated scalar type <ref_std_enum>`,
consider the following:

.. code-block:: sdl

    scalar type Color extending enum<Red, Green, Blue>;

Introspection of the enum scalar ``Color``:

.. code-block:: edgeql-repl

    db> with module schema
    ... select ScalarType {
    ...     name,
    ...     default,
    ...     enum_values,
    ...     abstract,
    ...     bases: { name },
    ...     ancestors: { name },
    ...     annotations: { name, @value },
    ...     constraints: { name },
    ... }
    ... filter .name = 'default::Color';
    {
        Object {
            name: 'default::Color',
            default: {},
            enum_values: ['Red', 'Green', 'Blue'],
            abstract: {},
            bases: {Object { name: 'std::anyenum' }},
            ancestors: {
                Object { name: 'std::anyscalar' },
                Object { name: 'std::anyenum' }
            },
            annotations: {},
            constraints: {}
        }
    }
