.. _ref_graphql_mutations:


Mutations
=========

EdgeDB provides GraphQL mutations to perform ``delete``, ``insert``
and ``update`` operations.


Delete
------

The "delete" mutation is very similar in structure to a query.
Basically, it works the same way as a query, using the
:ref:`filter <ref_graphql_overview_filter>`,
:ref:`order <ref_graphql_overview_order>`, and various
:ref:`pagination parameters <ref_graphql_overview_pagination>` to
define a set of objects to be deleted. These objects are also
returned as the result of the delete mutation. Each object type
has a corresponding ``delete_<type>`` mutation:

.. table::
    :class: codeblocks

    +---------------------------------+---------------------------------+
    | GraphQL                         | EdgeQL equivalent               |
    +=================================+=================================+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     mutation delete_all_books { |     select (                    |
    |         delete_Book {           |         delete Book             |
    |             title               |     ) {                         |
    |             synopsis            |         title,                  |
    |             author {            |         synopsis,               |
    |                 name            |         author: {               |
    |             }                   |             name                |
    |         }                       |         }                       |
    |     }                           |     };                          |
    +---------------------------------+---------------------------------+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     mutation delete_book_spam { |     select (                    |
    |         delete_Book(            |         delete Book             |
    |             filter: {           |         filter                  |
    |                 title: {        |             Book.title = 'Spam' |
    |                     eq: "Spam"  |     ) {                         |
    |                 }               |         title,                  |
    |             }                   |         synopsis                |
    |         ) {                     |     };                          |
    |             title               |                                 |
    |             synopsis            |                                 |
    |         }                       |                                 |
    |     }                           |                                 |
    +---------------------------------+---------------------------------+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     mutation delete_one_book {  |     select (                    |
    |         delete_Book(            |         delete Book             |
    |             filter: {           |         filter                  |
    |                 author: {       |             Book.author.name =  |
    |                     name: {     |                 'Lewis Carroll' |
    |                         eq:     |         order by                |
    |                 "Lewis Carroll" |             Book.title ASC      |
    |                     }           |         limit 1                 |
    |                 }               |     ) {                         |
    |             },                  |         title,                  |
    |             order: {            |         synopsis                |
    |                 title: {        |     };                          |
    |                     dir: ASC    |                                 |
    |                 }               |                                 |
    |             },                  |                                 |
    |             first: 1            |                                 |
    |         ) {                     |                                 |
    |             title               |                                 |
    |             synopsis            |                                 |
    |         }                       |                                 |
    |     }                           |                                 |
    +---------------------------------+---------------------------------+

Insert
------

The "insert" mutation exists for every object type. It allows creating
new objects and supports nested insertions, too. The objects to be
inserted are specified via the ``data`` parameter, which takes a list
of specifications. Each such specification has the same structure as
the object being inserted with required and optional fields (although
if a field is required in the object but has a default, it's optional
in the insert specification):

.. table::
    :class: codeblocks

    +---------------------------------+---------------------------------+
    | GraphQL                         | EdgeQL equivalent               |
    +=================================+=================================+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     mutation insert_books {     |     select {                    |
    |         insert_Book(            |         (insert Book {          |
    |             data: [{            |             title := "One"      |
    |                 title: "One"    |         }),                     |
    |             }, {                |         (insert Book {          |
    |                 title: "Two"    |             title := "Two"      |
    |             }]                  |         })                      |
    |         ) {                     |     } {                         |
    |             id                  |         id,                     |
    |             title               |         title                   |
    |         }                       |     };                          |
    |     }                           |                                 |
    +---------------------------------+---------------------------------+

It's possible to insert a nested structure all at once (e.g., a new
book and a new author):

.. table::
    :class: codeblocks

    +---------------------------------+---------------------------------+
    | GraphQL                         | EdgeQL equivalent               |
    +=================================+=================================+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     mutation insert_books {     |     select (                    |
    |         insert_Book(            |         insert Book {           |
    |             data: [{            |             title := "Three",   |
    |                 title: "Three", |             author := (         |
    |                 author: {       |                 insert Author { |
    |                     data: {     |                     name :=     |
    |                         name:   |                     "Unknown"   |
    |                     "Unknown"   |                 }               |
    |                     }           |             )                   |
    |                 }               |        }                        |
    |             }]                  |     ) {                         |
    |         ) {                     |         id,                     |
    |             id                  |         title                   |
    |             title               |     };                          |
    |         }                       |                                 |
    |     }                           |                                 |
    +---------------------------------+---------------------------------+

It's also possible to insert a new object that's connected to an
existing object (e.g. a new book by an existing author). In this case
the nested object is specified using :ref:`filter
<ref_graphql_overview_filter>`,
:ref:`order <ref_graphql_overview_order>`, and various
:ref:`pagination parameters <ref_graphql_overview_pagination>` to
define a set of objects to be connected:

.. table::
    :class: codeblocks

    +---------------------------------+---------------------------------+
    | GraphQL                         | EdgeQL equivalent               |
    +=================================+=================================+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     mutation insert_book {      |     select (                    |
    |         insert_Book(            |         insert Book {           |
    |             data: [{            |             title := "Four",    |
    |                 title: "Four",  |             author := (         |
    |                 author: {       |                 select Author   |
    |                     filter: {   |                 filter          |
    |         name: {eq: "Unknown"}   |                 Author.name =   |
    |                     }           |                     "Unknown"   |
    |                 }               |             )                   |
    |             }]                  |         }                       |
    |         ) {                     |     ) {                         |
    |             id                  |         id,                     |
    |             title               |         title                   |
    |         }                       |     };                          |
    |     }                           |                                 |
    +---------------------------------+---------------------------------+

Update
------

The "update" mutation has features that are similar to both an
"insert" mutation and a query. On one hand, the mutation takes
:ref:`filter <ref_graphql_overview_filter>`,
:ref:`order <ref_graphql_overview_order>`, and various
:ref:`pagination parameters <ref_graphql_overview_pagination>` to
define a set of objects to be updated. On the other hand, the ``data``
parameter is used to specify what and how should be updated.

The ``data`` parameter contains the fields that should be altered as
well as what type of update operation must be performed (``set``,
``increment``, ``append``, etc.). The particular operations available
depend on the type of field being updated.

.. table::
    :class: codeblocks

    +---------------------------------+---------------------------------+
    | GraphQL                         | EdgeQL equivalent               |
    +=================================+=================================+
    | .. code-block:: graphql         | .. code-block:: edgeql          |
    |                                 |                                 |
    |     mutation update_book {      |     with                        |
    |         update_Book(            |         Upd := (                |
    |             filter: {           |             update Book         |
    |                 title: {        |             filter              |
    |                     eq: "One"   |                 Book.title =    |
    |                 }               |                     "One"       |
    |             }                   |             set {               |
    |             data: {             |                 synopsis :=     |
    |                 synopsis: {     |                     "TBD",      |
    |                     set: "TBD"  |                 author := (     |
    |                 }               |                 select Author   |
    |                 author: {       |                 filter          |
    |                     set: {      |                 Author.name =   |
    |             filter: {           |                     "Unknown"   |
    |                 name: {         |                 )               |
    |                     eq:         |             }                   |
    |                     "Unknown"   |         )                       |
    |                 }               |     select Upd {                |
    |             }                   |         id,                     |
    |                     }           |         title                   |
    |                 }               |     };                          |
    |             }                   |                                 |
    |         ) {                     |                                 |
    |             id                  |                                 |
    |             title               |                                 |
    |         }                       |                                 |
    |     }                           |                                 |
    +---------------------------------+---------------------------------+
