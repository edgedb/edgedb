.. _ref_eql_introspection_indexes:

=======
Indexes
=======

This section describes introspection of :ref:`indexes
<ref_datamodel_indexes>`.

Introspection of the ``schema::Index``:

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
    ... FILTER .name = 'schema::Index';
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

    type Addressable {
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

    db> WITH MODULE schema
    ... SELECT Index {
    ...     expr,
    ... }
    ... FILTER .expr LIKE '%.name';
    {
        Object {
            expr: '.name'
        }
    }

For introspection of the index within the context of its host type see
:ref:`object type introspection <ref_eql_introspection_object_types>`.
