.. _ref_eql_introspection_collection_types:

================
Collection Types
================

This section describes introspection of :ref:`collection types
<ref_datamodel_collection_types>`.


Array
-----

Introspection of the ``schema::Array``:

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
    ... FILTER .name = 'schema::Array';
    {
        Object {
            name: 'schema::Array',
            links: {
                Object { name: '__type__' },
                Object { name: 'element_type' }
            },
            properties: {
                Object { name: 'id' },
                Object { name: 'name' }
            }
        }
    }

For a type with an :eql:type:`array` property, consider the following:

.. code-block:: sdl

    type User {
        required property name -> str;
        property favorites -> array<str>;
    }

Introspection of the ``User`` with emphasis on properties:

.. code-block:: edgeql-repl

    db> WITH MODULE schema
    ... SELECT ObjectType {
    ...     name,
    ...     properties: {
    ...         name,
    ...         target: {
    ...             name,
    ...             [IS Array].element_type: { name },
    ...         },
    ...     },
    ... }
    ... FILTER .name = 'default::User';
    {
        Object {
            name: 'default::User',
            properties: {
                Object {
                    name: 'favorites',
                    target: Object {
                        name: 'array',
                        element_type: Object { name: 'std::str' }
                    }
                },
                ...
            }
        }
    }


Tuple
-----

Introspection of the ``schema::Tuple``:

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
    ... FILTER .name = 'schema::Tuple';
    {
        Object {
            name: 'schema::Tuple',
            links: {
                Object { name: '__type__' },
                Object { name: 'element_types' }
            },
            properties: {
                Object { name: 'id' },
                Object { name: 'name' }
            }
        }
    }

Introspection of the specific :eql:type:`tuple` types is not yet implemented.
