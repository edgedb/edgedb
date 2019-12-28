.. _ref_eql_introspection_operators:

=========
Operators
=========

This section describes introspection of EdgeDB operators. Much like
functions, operators have parameters and return types as well as a few
other features.

Introspection of the ``schema::Operator``:

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
    ... FILTER .name = 'schema::Operator';
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
    ... FILTER .name = 'schema::Parameter';
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

Introspection of the :eql:op:`AND` operator:

.. code-block:: edgeql-repl

    db> WITH MODULE schema
    ... SELECT Operator {
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
    ... FILTER .name = 'std::AND';
    {
        Object {
            name: 'std::AND',
            operator_kind: 'INFIX',
            annotations: {},
            params: {
                Object {
                    kind: 'POSITIONAL',
                    name: 'a',
                    num: 0,
                    typemod: 'SINGLETON',
                    type: Object { name: 'std::bool' },
                    default: {}
                },
                Object {
                    kind: 'POSITIONAL',
                    name: 'b',
                    num: 1,
                    typemod: 'SINGLETON',
                    type: Object { name: 'std::bool' },
                    default: {}
                }
            },
            return_typemod: 'SINGLETON',
            return_type: Object { name: 'std::bool' }
        }
    }
