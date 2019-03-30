.. _ref_eql_introspection_scalar_types:

============
Scalar Types
============

This section describes introspection of :ref:`scalar types
<ref_datamodel_scalar_types>`.

Introspection of the ``schema::ScalarType``:

.. code-block:: edgeql-repl

    db> WITH MODULE schema
    ... SELECT ObjectType {
    ...     name,
    ...     links: {
    ...         name,
    ...     },
    ...     properties: {
    ...         name,
    ...     }
    ... }
    ... FILTER .name = 'schema::ScalarType';
    {
        Object {
            name: 'schema::ScalarType',
            links: {
                Object { name: '__type__' },
                Object { name: 'attributes' },
                Object { name: 'bases' },
                Object { name: 'constraints' },
                Object { name: 'ancestors' }
            },
            properties: {
                Object { name: 'default' },
                Object { name: 'enum_values' },
                Object { name: 'id' },
                Object { name: 'is_abstract' },
                Object { name: 'is_final' },
                Object { name: 'name' }
            }
        }
    }

Introspection of the built-in scalar :eql:type:`str`:

.. code-block:: edgeql-repl

    db> WITH MODULE schema
    ... SELECT ScalarType {
    ...     name,
    ...     default,
    ...     enum_values,
    ...     is_abstract,
    ...     is_final,
    ...     bases: { name },
    ...     ancestors: { name },
    ...     attributes: { name, @value },
    ...     constraints: { name },
    ... }
    ... FILTER .name = 'std::str';
    {
        Object {
            name: 'std::str',
            default: {},
            enum_values: {},
            is_abstract: {},
            is_final: {},
            bases: {Object { name: 'std::anyscalar' }},
            ancestors: {Object { name: 'std::anyscalar' }},
            attributes: {},
            constraints: {}
        }
    }

For an :ref:`enumerated scalar type <ref_datamodel_scalars_enum>`,
consider the following:

.. code-block:: sdl

    scalar type color_enum_t extending enum<'red', 'green', 'blue'>;

Introspection of the enum scalar ``color_enum_t``:

.. code-block:: edgeql-repl

    db> WITH MODULE schema
    ... SELECT ScalarType {
    ...     name,
    ...     default,
    ...     enum_values,
    ...     is_abstract,
    ...     is_final,
    ...     bases: { name },
    ...     ancestors: { name },
    ...     attributes: { name, @value },
    ...     constraints: { name },
    ... }
    ... FILTER .name = 'default::color_enum_t';
    {
        Object {
            name: 'default::color_enum_t',
            default: {},
            enum_values: ['red', 'green', 'blue'],
            is_abstract: {},
            is_final: true,
            bases: {Object { name: 'std::anyenum' }},
            ancestors: {
                Object { name: 'std::anyscalar' },
                Object { name: 'std::anyenum' }
            },
            attributes: {},
            constraints: {}
        }
    }
