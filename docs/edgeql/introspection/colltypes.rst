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

For example, below is an introspection of the return type of
the :eql:func:`sys::get_version` function:

.. code-block:: edgeql-repl

    db> WITH MODULE schema
    ... SELECT `Function` {
    ...     return_type[IS Tuple]: {
    ...         element_types: {
    ...             name,
    ...             type: { name }
    ...         } ORDER BY .num
    ...     }
    ... }
    ... FILTER .name = 'sys::get_version';
    {
        Object {
            return_type: Object {
                element_types: {
                    Object {
                        name: 'major',
                        type: Object {
                            name: 'std::int64'
                        }
                    },
                    Object {
                        name: 'minor',
                        type: Object {
                            name: 'std::int64'
                        }
                    },
                    Object {
                        name: 'stage',
                        type: Object {
                            name: 'sys::version_stage'
                        }
                    },
                    Object {
                        name: 'stage_no',
                        type: Object {
                            name: 'std::int64'
                        }
                    },
                    Object {
                        name: 'local',
                        type: Object { name: 'array' }
                    }
                }
            }
        }
    }
