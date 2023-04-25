.. _ref_datamodel_introspection_operators:

=========
Operators
=========

This section describes introspection of EdgeDB operators. Much like
functions, operators have parameters and return types as well as a few
other features.

Introspection of the ``schema::Operator``:

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
    ... filter .name = 'schema::Operator';
    {
        Object {
            name: 'schema::Operator',
            links: {
                Object { name: '__type__' },
                Object { name: 'annotations' },
                Object { name: 'params' },
                Object { name: 'return_type' }
            },
            properties: {
                Object { name: 'id' },
                Object { name: 'name' },
                Object { name: 'operator_kind' },
                Object { name: 'return_typemod' }
            }
        }
    }

Since ``params`` are quite important to operators, here's their structure:

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
    ... filter .name = 'schema::Parameter';
    {
        Object {
            name: 'schema::Parameter',
            links: {
                Object { name: '__type__' },
                Object { name: 'type' }
            },
            properties: {
                Object { name: 'default' },
                Object { name: 'id' },
                Object { name: 'kind' },
                Object { name: 'name' },
                Object { name: 'num' },
                Object { name: 'typemod' }
            }
        }
    }

Introspection of the :eql:op:`and` operator:

.. code-block:: edgeql-repl

    db> with module schema
    ... select Operator {
    ...     name,
    ...     operator_kind,
    ...     annotations: { name, @value },
    ...     params: {
    ...         kind,
    ...         name,
    ...         num,
    ...         typemod,
    ...         type: { name },
    ...         default,
    ...     },
    ...     return_typemod,
    ...     return_type: { name },
    ... }
    ... filter .name = 'std::AND';
    {
        Object {
            name: 'std::AND',
            operator_kind: 'Infix',
            annotations: {},
            params: {
                Object {
                    kind: 'PositionalParam',
                    name: 'a',
                    num: 0,
                    typemod: 'SingletonType',
                    type: Object { name: 'std::bool' },
                    default: {}
                },
                Object {
                    kind: 'PositionalParam',
                    name: 'b',
                    num: 1,
                    typemod: 'SingletonType',
                    type: Object { name: 'std::bool' },
                    default: {}
                }
            },
            return_typemod: 'SingletonType',
            return_type: Object { name: 'std::bool' }
        }
    }
