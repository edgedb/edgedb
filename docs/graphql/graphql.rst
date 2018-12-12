.. _ref_graphql_overview:


Basics
======

For the purposes of this section we will consider ``default`` module
containing the following schema:

.. code-block:: eschema

    type Author:
        property name -> str

    type Book:
        # to make the examples simpler only the title is
        # a required property
        required property title -> str
        property synopsis -> str
        link author -> Author
        property isbn -> str:
            constraint maxlength(10)

From the schema above EdgeDB will expose to GraphQL:

* Object types: ``Author`` and ``Book``
* scalars ``String`` and ``ID``

In addition to this the ``Query`` will have 2 fields: ``Author``, and
``Book`` to query these types respectively.


Queries
+++++++

Consider this example:

.. table::
    :class: codeblocks

    +----------------------------------+--------------------------------+
    | GraphQL                          | EdgeQL equivalent              |
    +==================================+================================+
    | .. code-block:: graphql          | .. code-block:: edgeql         |
    |                                  |                                |
    |     {                            |     SELECT                     |
    |         Book {                   |         Book {                 |
    |             title                |             title,             |
    |             synopsis             |             synopsis,          |
    |             author {             |             author: {          |
    |                 name             |                 name           |
    |             }                    |             }                  |
    |         }                        |         };                     |
    |     }                            |                                |
    +----------------------------------+--------------------------------+

The top-level field of the GraphQL query must be a valid
``ObjectType`` or a ``View``. Nested fields must be valid links or
properties.


Arguments
---------

There are some specific conventions as to how ``arguments`` in GraphQL
queries are treated. Filtering the retrieved data is done by
specifying ``arguments`` for the GraphQL query. Consider the following
document:

.. table::
    :class: codeblocks

    +---------------------------------+---------------------------------+
    | GraphQL                         | EdgeQL equivalent               |
    +=================================+=================================+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     {                           |     SELECT                      |
    |         Book(title: "Spam") {   |         Book {                  |
    |             title               |             title,              |
    |             synopsis            |             synopsis,           |
    |         }                       |         }                       |
    |     }                           |     FILTER                      |
    |                                 |         Book.title = 'Spam';    |
    +---------------------------------+---------------------------------+

Any of the scalar fields of the return type are also valid arguments.
All of the arguments are optional.

Variables
---------

It is possible to use variables within GraphQL queries. They are
mapped to variables in EdgeQL.

.. table::
    :class: codeblocks

    +---------------------------------+---------------------------------+
    | GraphQL                         | EdgeQL equivalent               |
    +=================================+=================================+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     query ($title: String!) {   |     SELECT                      |
    |         Book(title: $title) {   |         Book {                  |
    |             title               |             title,              |
    |             synopsis            |             synopsis,           |
    |         }                       |         }                       |
    |     }                           |     FILTER                      |
    |                                 |         Book.title = $title;    |
    +---------------------------------+---------------------------------+


Mutations
+++++++++

By default EdgeDB does not provide GraphQL mutations.
