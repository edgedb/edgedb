.. _ref_graphql_introspection:


Introspection
=============

GraphQL introspection can be used to explore the exposed EdgeDB types
and expresssion aliases. Note that there are certain types like
:eql:type:`tuple` that cannot be expressed in terms of GraphQL type
system (a ``tuple`` can be like a heterogeneous "List").

Consider the following GraphQL introspection query:

.. code-block:: graphql

    {
        __type(name: "Query") {
            name
            fields {
                name
                args {
                    name
                    type {
                        kind
                        name
                    }
                }
            }
        }
    }

Produces:

.. code-block:: json

    {
        "__type": {
            "name": "Query",
            "fields": [
                {
                    "name": "Author",
                    "args": [
                        {
                            "name": "id",
                            "type": {
                                "kind": "SCALAR",
                                "name": "ID"
                            }
                        },
                        {
                            "name": "name",
                            "type": {
                                "kind": "SCALAR",
                                "name": "String"
                            }
                        }
                    ]
                },
                {
                    "name": "Book",
                    "args": [
                        {
                            "name": "id",
                            "type": {
                                "kind": "SCALAR",
                                "name": "ID"
                            }
                        },
                        {
                            "name": "isbn",
                            "type": {
                                "kind": "SCALAR",
                                "name": "String"
                            }
                        },
                        {
                            "name": "synopsis",
                            "type": {
                                "kind": "SCALAR",
                                "name": "String"
                            }
                        },
                        {
                            "name": "title",
                            "type": {
                                "kind": "SCALAR",
                                "name": "String"
                            }
                        }
                    ]
                }
            ]
        }
    }

The above example shows what has been exposed for querying with GraphQL.
