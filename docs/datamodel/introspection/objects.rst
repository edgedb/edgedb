.. _ref_datamodel_introspection_object_types:

============
Object types
============

This section describes introspection of :ref:`object types
<ref_datamodel_object_types>`.

Introspection of the ``schema::ObjectType``:

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
    ... filter .name = 'schema::ObjectType';
    {
        Object {
            name: 'schema::ObjectType',
            links: {
                Object { name: '__type__' },
                Object { name: 'annotations' },
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
                Object { name: 'abstract' },
                Object { name: 'name' }
            }
        }
    }

Consider the following schema:

.. code-block:: sdl
    :version-lt: 3.0

    abstract type Addressable {
        property address -> str;
    }

    type User extending Addressable {
        # define some properties and a link
        required property name -> str;

        multi link friends -> User;

        # define an index for User based on name
        index on (.name);
    }

.. code-block:: sdl

    abstract type Addressable {
        address: str;
    }

    type User extending Addressable {
        # define some properties and a link
        required name: str;

        multi friends: User;

        # define an index for User based on name
        index on (.name);
    }

Introspection of ``User``:

.. code-block:: edgeql-repl

    db> with module schema
    ... select ObjectType {
    ...     name,
    ...     abstract,
    ...     bases: { name },
    ...     ancestors: { name },
    ...     annotations: { name, @value },
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
    ...     indexes: { expr },
    ... }
    ... filter .name = 'default::User';
    {
        Object {
            name: 'default::User',
            abstract: false,
            bases: {Object { name: 'default::Addressable' }},
            ancestors: {
                Object { name: 'std::BaseObject' },
                Object { name: 'std::Object' },
                Object { name: 'default::Addressable' }
            },
            annotations: {},
            links: {
                Object {
                    name: '__type__',
                    cardinality: 'One',
                    required: {},
                    target: Object { name: 'schema::Type' }
                },
                Object {
                    name: 'friends',
                    cardinality: 'Many',
                    required: false,
                    target: Object { name: 'default::User' }
                }
            },
            properties: {
                Object {
                    name: 'address',
                    cardinality: 'One',
                    required: false,
                    target: Object { name: 'std::str' }
                },
                Object {
                    name: 'id',
                    cardinality: 'One',
                    required: true,
                    target: Object { name: 'std::uuid' }
                },
                Object {
                    name: 'name',
                    cardinality: 'One',
                    required: true,
                    target: Object { name: 'std::str' }
                }
            },
            constraints: {},
            indexes: {
                Object {
                    expr: '.name'
                }
            }
        }
    }


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Object types <ref_datamodel_object_types>`
  * - :ref:`SDL > Object types <ref_eql_sdl_object_types>`
  * - :ref:`DDL > Object types <ref_eql_ddl_object_types>`
  * - :ref:`Cheatsheets > Object types <ref_cheatsheet_object_types>`

