.. _ref_datamodel_introspection_constraints:

===========
Constraints
===========

This section describes introspection of :ref:`constraints
<ref_datamodel_constraints>`.

Introspection of the ``schema::Constraint``:

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
    ... filter .name = 'schema::Constraint';
    {
        Object {
            name: 'schema::Constraint',
            links: {
                Object { name: '__type__' },
                Object { name: 'args' },
                Object { name: 'annotations' },
                Object { name: 'bases' },
                Object { name: 'ancestors' },
                Object { name: 'params' },
                Object { name: 'return_type' },
                Object { name: 'subject' }
            },
            properties: {
                Object { name: 'errmessage' },
                Object { name: 'expr' },
                Object { name: 'finalexpr' },
                Object { name: 'id' },
                Object { name: 'abstract' },
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

    db> with module schema
    ... select ScalarType {
    ...     name,
    ...     constraints: {
    ...         name,
    ...         expr,
    ...         annotations: { name, @value },
    ...         subject: { name },
    ...         params: { name, @value, type: { name } },
    ...         return_typemod,
    ...         return_type: { name },
    ...         errmessage,
    ...     },
    ... }
    ... filter .name = 'default::maxex_100';
    {
        Object {
            name: 'default::maxex_100',
            constraints: {
                Object {
                    name: 'std::max_ex_value',
                    expr: '(__subject__ <= max)',
                    annotations: {},
                    subject: Object { name: 'default::maxex_100' },
                    params: {
                        Object {
                            name: 'max',
                            type: Object { name: 'anytype' },
                            @value: '100'
                        }
                    },
                    return_typemod: 'SingletonType',
                    return_type: Object { name: 'std::bool' }
                    errmessage: '{__subject__} must be less ...',
                }
            }
        }
    }


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Constraints <ref_datamodel_constraints>`
  * - :ref:`SDL > Constraints <ref_eql_sdl_constraints>`
  * - :ref:`DDL > Constraints <ref_eql_ddl_constraints>`
  * - :ref:`Standard Library > Constraints <ref_std_constraints>`
