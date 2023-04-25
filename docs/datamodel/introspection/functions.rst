.. _ref_datamodel_introspection_functions:

=========
Functions
=========

This section describes introspection of :ref:`functions
<ref_datamodel_functions>`.

Introspection of the ``schema::Function``:

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
    ... filter .name = 'schema::Function';
    {
        Object {
            name: 'schema::Function',
            links: {
                Object { name: '__type__' },
                Object { name: 'annotations' },
                Object { name: 'params' },
                Object { name: 'return_type' }
            },
            properties: {
                Object { name: 'id' },
                Object { name: 'name' },
                Object { name: 'return_typemod' }
            }
        }
    }

Since ``params`` are quite important to functions, here's their structure:

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

Introspection of the built-in :eql:func:`count`:

.. code-block:: edgeql-repl

    db> with module schema
    ... select `Function` {
    ...     name,
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
    ... filter .name = 'std::count';
    {
        Object {
            name: 'std::count',
            annotations: {},
            params: {
                Object {
                    kind: 'PositionalParam',
                    name: 's',
                    num: 0,
                    typemod: 'SetOfType',
                    type: Object { name: 'anytype' },
                    default: {}
                }
            },
            return_typemod: 'SingletonType',
            return_type: Object { name: 'std::int64' }
        }
    }


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Functions <ref_datamodel_functions>`
  * - :ref:`SDL > Functions <ref_eql_sdl_functions>`
  * - :ref:`DDL > Functions <ref_eql_ddl_functions>`
  * - :ref:`Reference > Function calls <ref_reference_function_call>`
  * - :ref:`Cheatsheets > Functions <ref_cheatsheet_functions>`

