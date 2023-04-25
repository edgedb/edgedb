.. _ref_datamodel_introspection_casts:

=====
Casts
=====

This section describes introspection of EdgeDB :eql:op:`type casts
<cast>`. Features like whether the casts are implicit can be
discovered by introspecting ``schema::Cast``.

Introspection of the ``schema::Cast``:

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
    ... filter .name = 'schema::Cast';
    {
        Object {
            name: 'schema::Cast',
            links: {
                Object { name: '__type__' },
                Object { name: 'from_type' },
                Object { name: 'to_type' }
            },
            properties: {
                Object { name: 'allow_assignment' },
                Object { name: 'allow_implicit' },
                Object { name: 'id' },
                Object { name: 'name' }
            }
        }
    }

Introspection of the possible casts from ``std::int64`` to other
types:

.. code-block:: edgeql-repl

    db> with module schema
    ... select Cast {
    ...     allow_assignment,
    ...     allow_implicit,
    ...     to_type: { name },
    ... }
    ... filter .from_type.name = 'std::int64'
    ... order by .to_type.name;
    {
        Object {
            allow_assignment: false,
            allow_implicit: true,
            to_type: Object { name: 'std::bigint' }
        },
        Object {
            allow_assignment: false,
            allow_implicit: true,
            to_type: Object { name: 'std::decimal' }
        },
        Object {
            allow_assignment: true,
            allow_implicit: false,
            to_type: Object { name: 'std::float32' }
        },
        Object {
            allow_assignment: false,
            allow_implicit: true,
            to_type: Object { name: 'std::float64' }
        },
        Object {
            allow_assignment: true,
            allow_implicit: false,
            to_type: Object { name: 'std::int16' }
        },
        Object {
            allow_assignment: true,
            allow_implicit: false,
            to_type: Object { name: 'std::int32' }
        },
        Object {
            allow_assignment: false,
            allow_implicit: false,
            to_type: Object { name: 'std::json' }
        },
        Object {
            allow_assignment: false,
            allow_implicit: false,
            to_type: Object { name: 'std::str' }
        }
    }

The ``allow_implicit`` property tells whether this is an *implicit cast*
in all contexts (such as when determining the type of a set of mixed
literals or resolving the argument types of functions or operators if
there's no exact match). For example, a literal ``1`` is an
:eql:type:`int64` and it is implicitly cast into a :eql:type:`bigint`
or :eql:type:`float64` if it is added to a set containing either one
of those types:

.. code-block:: edgeql-repl

    db> select {1, 2n};
    {1n, 2n}
    db> select {1, 2.0};
    {1.0, 2.0}

What happens if there's no implicit cast between a couple of scalars
in this type of example? EdgeDB checks whether there's a scalar type
such that all of the set elements can be implicitly cast into that:

.. code-block:: edgeql-repl

    db> select introspect (typeof {<int64>1, <float32>2}).name;
    {'std::float64'}

The scalar types :eql:type:`int64` and :eql:type:`float32` cannot be
implicitly cast into each other, but they both can be implicitly cast
into :eql:type:`float64`.

The ``allow_assignment`` property tells whether this is an implicit
cast during assignment if a more general *implicit cast* is not
allowed. For example, consider the following type:

.. code-block:: sdl

    type Example {
        property p_int16 -> int16;
        property p_float32 -> float32;
        property p_json -> json;
    }

.. code-block:: edgeql-repl

    db> insert Example {
    ...     p_int16 := 1,
    ...     p_float32 := 2
    ... };
    {Object { id: <uuid>'...' }}
    db> insert Example {
    ...     p_json := 3  # assignment cast to json not allowed
    ... };
    InvalidPropertyTargetError: invalid target for property
    'p_json' of object type 'default::Example': 'std::int64'
    (expecting 'std::json')
