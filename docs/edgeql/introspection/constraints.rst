.. _ref_eql_introspection_constraints:

===========
Constraints
===========

This section describes introspection of :ref:`constraints
<ref_datamodel_constraints>`.

Introspection of the ``schema::Constraint``:

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
    ... FILTER .name = 'schema::Constraint';
    {
        Object {
            name: 'schema::Constraint',
            links: {
                Object { name: '__type__' },
                Object { name: 'args' },
                Object { name: 'attributes' },
                Object { name: 'bases' },
                Object { name: 'mro' },
                Object { name: 'params' },
                Object { name: 'return_type' },
                Object { name: 'subject' }
            },
            properties: {
                Object { name: 'errmessage' },
                Object { name: 'expr' },
                Object { name: 'finalexpr' },
                Object { name: 'id' },
                Object { name: 'is_abstract' },
                Object { name: 'is_final' },
                Object { name: 'name' },
                Object { name: 'return_typemod' },
                Object { name: 'subjectexpr' }
            }
        }
    }

Consider the following schema:

.. code-block:: sdl

    scalar type maxex_100 extending int64 {
        constraint max_ex_value(100);
    }

Introspection of the scalar ``maxex_100`` with focus on the constraint:

.. code-block:: edgeql-repl

    db> WITH MODULE schema
    ... SELECT ScalarType {
    ...     name,
    ...     constraints: {
    ...         name,
    ...         expr,
    ...         attributes: { name, @value },
    ...         subject: { name },
    ...         args: { name, @value, type: { name } },
    ...         return_typemod,
    ...         return_type: { name },
    ...         errmessage,
    ...     },
    ... }
    ... FILTER .name = 'default::maxex_100';
    {
        Object {
            name: 'default::maxex_100',
            constraints: {
                Object {
                    name: 'std::max_ex_value',
                    expr: '(__subject__ <= max)',
                    attributes: {},
                    subject: Object { name: 'default::maxex_100' },
                    args: {
                        Object {
                            name: 'max',
                            type: Object { name: 'anytype' },
                            @value: '100'
                        }
                    },
                    return_typemod: 'SINGLETON',
                    return_type: Object { name: 'std::bool' }
                    errmessage: '{__subject__} must be less ...',
                }
            }
        }
    }
