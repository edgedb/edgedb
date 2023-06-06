.. _ref_datamodel_introspection_indexes:

=======
Indexes
=======

This section describes introspection of :ref:`indexes
<ref_datamodel_indexes>`.

Introspection of the ``schema::Index``:

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
    ... filter .name = 'schema::Index';
    {
        Object {
            name: 'schema::Index',
            links: {Object { name: '__type__' }},
            properties: {
                Object { name: 'expr' },
                Object { name: 'id' },
                Object { name: 'name' }
            }
        }
    }

Consider the following schema:

.. code-block:: sdl

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

Introspection of ``User.name`` index:

.. code-block:: edgeql-repl

    db> with module schema
    ... select Index {
    ...     expr,
    ... }
    ... filter .expr like '%.name';
    {
        Object {
            expr: '.name'
        }
    }

For introspection of the index within the context of its host type see
:ref:`object type introspection <ref_datamodel_introspection_object_types>`.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Indexes <ref_datamodel_indexes>`
  * - :ref:`SDL > Indexes <ref_eql_sdl_indexes>`
  * - :ref:`DDL > Indexes <ref_eql_ddl_indexes>`
