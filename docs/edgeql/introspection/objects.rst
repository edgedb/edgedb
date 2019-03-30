.. _ref_eql_introspection_object_types:

============
Object Types
============

This section describes introspection of :ref:`object types
<ref_datamodel_object_types>`.

Introspection of the ``schema::ObjectType``:

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
    ... FILTER .name = 'schema::ObjectType';
    {
        Object {
            name: 'schema::ObjectType',
            links: {
                Object { name: '__type__' },
                Object { name: 'attributes' },
                Object { name: 'bases' },
                Object { name: 'constraints' },
                Object { name: 'indexes' },
                Object { name: 'links' },
                Object { name: 'ancestors' },
                Object { name: 'pointers' },
                Object { name: 'properties' }
            },
            properties: {
                Object { name: 'id' },
                Object { name: 'is_abstract' },
                Object { name: 'is_final' },
                Object { name: 'name' }
            }
        }
    }

Consider the following schema:

.. code-block:: sdl

    type Addressable {
        property address -> str;
    }

    type User extending Addressable {
        # define some properties and a link
        required property name -> str;

        multi link friends -> User;

        # define an index for User based on name
        index user_name_idx on (__subject__.name);
    }

Introspection of ``User``:

.. code-block:: edgeql-repl

    db> WITH MODULE schema
    ... SELECT ObjectType {
    ...     name,
    ...     is_abstract,
    ...     is_final,
    ...     bases: { name },
    ...     ancestors: { name },
    ...     attributes: { name, @value },
    ...     links: {
    ...         name,
    ...         cardinality,
    ...         required,
    ...         target: { name },
    ...     },
    ...     properties: {
    ...         name,
    ...         cardinality,
    ...         required,
    ...         target: { name },
    ...     },
    ...     constraints: { name },
    ...     indexes: { name, expr },
    ... }
    ... FILTER .name = 'default::User';
    {
        Object {
            name: 'default::User',
            is_abstract: false,
            is_final: false,
            bases: {Object { name: 'default::Addressable' }},
            ancestors: {
                Object { name: 'std::Object' },
                Object { name: 'default::Addressable' }
            },
            attributes: {},
            links: {
                Object {
                    name: '__type__',
                    cardinality: 'ONE',
                    required: {},
                    target: Object { name: 'schema::Type' }
                },
                Object {
                    name: 'friends',
                    cardinality: 'MANY',
                    required: false,
                    target: Object { name: 'default::User' }
                }
            },
            properties: {
                Object {
                    name: 'address',
                    cardinality: 'ONE',
                    required: false,
                    target: Object { name: 'std::str' }
                },
                Object {
                    name: 'id',
                    cardinality: 'ONE',
                    required: true,
                    target: Object { name: 'std::uuid' }
                },
                Object {
                    name: 'name',
                    cardinality: 'ONE',
                    required: true,
                    target: Object { name: 'std::str' }
                }
            },
            constraints: {},
            indexes: {
                Object {
                    name: 'default::User.user_name_idx',
                    expr: 'default::User.name'
                }
            }
        }
    }
