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
            constraint max_len(10)

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

    +---------------------------------+---------------------------------+
    | GraphQL                         | EdgeQL equivalent               |
    +=================================+=================================+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     {                           |     SELECT                      |
    |         Book {                  |         Book {                  |
    |             title               |             title,              |
    |             synopsis            |             synopsis,           |
    |             author {            |             author: {           |
    |                 name            |                 name            |
    |             }                   |             }                   |
    |         }                       |         };                      |
    |     }                           |                                 |
    +---------------------------------+---------------------------------+

The top-level field of the GraphQL query must be a valid
``ObjectType`` or a ``View``. Nested fields must be valid links or
properties.

There are some specific conventions as to how *arguments* in GraphQL
queries are used to allow filtering, ordering, and paginating data.


Filtering
---------

Filtering the retrieved data is done by specifying a ``filter``
argument. The ``filter`` argument is customized to each specific type
based on the available fields. In case of the sample schema, here's
what the specification for the available filter arguments:

.. code-block:: graphql-schema

    # this is Author-specific
    input FilterAuthor {
        # basic boolean operators that combine conditions
        and: [FilterAuthor!]
        or: [FilterAuthor!]
        not: FilterAuthor

        # fields available for filtering (properties in EdgeQL)
        name: FilterString
    }

    # this is Book-specific
    input FilterBook {
        # basic boolean operators that combine conditions
        and: [FilterBook!]
        or: [FilterBook!]
        not: FilterBook

        # fields available for filtering (properties in EdgeQL)
        title: FilterString
        synopsis: FilterString
        isbn: FilterString
        author: FilterAuthor
    }

    # this is generic
    input FilterString {
        # equality
        eq: String
        neq: String

        # lexicographical comparison
        gt: String
        gte: String
        lt: String
        lte: String

        # other useful operations
        like: String
        ilike: String
    }

Here are some examples of using a filter:

.. table::
    :class: codeblocks

    +---------------------------------+---------------------------------+
    | GraphQL                         | EdgeQL equivalent               |
    +=================================+=================================+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     {                           |     SELECT                      |
    |         Book(                   |         Book {                  |
    |             filter: {           |             title,              |
    |                 title: {        |             synopsis            |
    |                     eq: "Spam"  |         }                       |
    |                 }               |     FILTER                      |
    |             }                   |         Book.title = 'Spam';    |
    |         ) {                     |                                 |
    |             title               |                                 |
    |             synopsis            |                                 |
    |         }                       |                                 |
    |     }                           |                                 |
    +---------------------------------+---------------------------------+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     {                           |     SELECT                      |
    |         Book(                   |         Book {                  |
    |             filter: {           |             title,              |
    |                 author: {       |             synopsis            |
    |                     name: {     |         }                       |
    |                         eq:     |     FILTER                      |
    |                 "Lewis Carroll" |         Book.author.name =      |
    |                     }           |             'Lewis Carroll';    |
    |                 }               |                                 |
    |             }                   |                                 |
    |         ) {                     |                                 |
    |             title               |                                 |
    |             synopsis            |                                 |
    |         }                       |                                 |
    |     }                           |                                 |
    +---------------------------------+---------------------------------+

It is legal to provide multiple input fields in the same input object.
They are all implicitly combined using a logical conjunction. For
example:

.. table::
    :class: codeblocks

    +---------------------------------+---------------------------------+
    | GraphQL                         | EdgeQL equivalent               |
    +=================================+=================================+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     {                           |     SELECT                      |
    |         Book(                   |         Book {                  |
    |             filter: {           |             title,              |
    |                 title: {        |         }                       |
    |                     gte: "m",   |     FILTER                      |
    |                     lt: "o"     |         Book.title >= 'm'       |
    |                 }               |         AND                     |
    |             }                   |         Book.title < 'o';       |
    |         ) {                     |                                 |
    |             title               |                                 |
    |         }                       |                                 |
    |     }                           |                                 |
    +---------------------------------+---------------------------------+


Ordering
--------

Ordering the retrieved data is done by specifying an ``order``
argument. The ``order`` argument is customized to each specific type
based on the available fields, much like the ``filter``. In case of
the sample schema, here's what the specification for the available
filter arguments:

.. code-block:: graphql-schema

    # this is Author-specific
    input OrderAuthor {
        # fields available for ordering (properties in EdgeQL)
        name: Ordering
    }

    # this is Book-specific
    input OrderBook {
        # fields available for ordering (properties in EdgeQL)
        title: Ordering
        synopsis: Ordering
        isbn: Ordering
    }

    # this is generic
    input Ordering {
        dir: directionEnum
        nulls: nullsOrderingEnum
    }

    enum directionEnum {
        ASC
        DESC
    }

    enum nullsOrderingEnum {
        SMALLEST    # null < any other value
        BIGGEST     # null > any other value
    }

If the value of ``nulls`` is not specified it is assumed to be
``SMALLEST``.

.. table::
    :class: codeblocks

    +------------------------------------+------------------------------+
    | GraphQL                            | EdgeQL equivalent            |
    +====================================+==============================+
    | .. code-block:: graphql            | .. code-block:: edgeql       |
    |                                    |                              |
    |     {                              |     SELECT                   |
    |         Author(                    |         Author {             |
    |             order: {               |             name,            |
    |                 name: {            |         }                    |
    |                     dir: ASC,      |     ORDER BY                 |
    |                     nulls: BIGGEST |         Author.name ASC      |
    |                 }                  |             EMPTY LAST;      |
    |             }                      |                              |
    |         ) {                        |                              |
    |             name                   |                              |
    |         }                          |                              |
    |     }                              |                              |
    +------------------------------------+------------------------------+


Paginating
----------

Paginating the retrieved data is done by providing one or more of the
following arguments: ``first``, ``last``, ``before``, and ``after``.
The pagination works in a similar way to Relay Connections. In case of
the sample schema, here's what the specification for the available
filter arguments:

.. code-block:: graphql-schema

    # a relevant Query definition snippet
    type Query {
        Author(
            filter: FilterAuthor,
            order: OrderAuthor,

            after: String,
            before: String,
            first: Int,
            last: Int,
        ): [Author!]

        # ... other Query fields
    }

The ``after`` and ``before`` strings are, in fact, string
representations of numeric indices under the particular filter and
ordering (starting at "0"). This makes the usage fairly intuitive even
without having Relay Connection edges and cursors.

The objects corresponding to the indices specified by ``before`` or
``after`` are not included.

.. table::
    :class: codeblocks

    +---------------------------------+---------------------------------+
    | GraphQL                         | EdgeQL equivalent               |
    +=================================+=================================+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     {                           |     SELECT                      |
    |         Author(                 |         Author {                |
    |             order: {            |             name,               |
    |                 name: {         |         }                       |
    |                     dir: ASC    |     ORDER BY                    |
    |                 }               |         Author.name ASC         |
    |             },                  |     LIMIT 10;                   |
    |             first: 10           |                                 |
    |         ) {                     |                                 |
    |             name                |                                 |
    |         }                       |                                 |
    |     }                           |                                 |
    +---------------------------------+---------------------------------+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     {                           |     SELECT                      |
    |         Author(                 |         Author {                |
    |             order: {            |             name,               |
    |                 name: {         |         }                       |
    |                     dir: ASC    |     ORDER BY                    |
    |                 }               |         Author.name ASC         |
    |             },                  |     OFFSET 20 LIMIT 10;         |
    |             after: "19",        |                                 |
    |             first: 10           |                                 |
    |         ) {                     |                                 |
    |             name                |                                 |
    |         }                       |                                 |
    |     }                           |                                 |
    +---------------------------------+---------------------------------+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     {                           |     SELECT                      |
    |         Author(                 |         Author {                |
    |             order: {            |             name,               |
    |                 name: {         |         }                       |
    |                     dir: ASC    |     ORDER BY                    |
    |                 }               |         Author.name ASC         |
    |             },                  |     OFFSET 20 LIMIT 10;         |
    |             after: "19",        |                                 |
    |             before: "30"        |                                 |
    |         ) {                     |                                 |
    |             name                |                                 |
    |         }                       |                                 |
    |     }                           |                                 |
    +---------------------------------+---------------------------------+


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
