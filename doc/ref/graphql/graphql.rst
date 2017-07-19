.. _ref_graphql_overview:


Basics
======

For the purposes of this section we will use the following schema:

.. code-block:: eschema

    concept Person:
        link name to str

    concept Author extending Person

    concept Book:
        # to make our examples simpler only the title is a required
        # link
        required link title to str
        link synopsis to str
        link author to Author:
            mapping: *1
        link isbn to str:
            constraint maxlength(10)
        link pub_date to datetime
        link price to float


EdgeDB supports all of the GraphQL syntactical constructs
(``queries``, ``fragments``, ``directives``, etc.). There's a special
``@edgedb`` directive that essentially maps onto ``WITH`` clause in
EdgeQL.


Queries
+++++++

Consider this example:

+--------------------------------------+-----------------------+
| GraphQL                              | EdgeQL equivalent     |
+======================================+=======================+
| .. code-block:: graphql              | .. code-block:: eql   |
|                                      |                       |
|   query @edgedb(module: "example") { |   WITH MODULE example |
|       Book {                         |   SELECT Book {       |
|           title                      |       title,          |
|           synopsis                   |       synopsis,       |
|           author {                   |       author: {       |
|               name                   |           name        |
|           }                          |       }               |
|       }                              |   };                  |
|   }                                  |                       |
+--------------------------------------+-----------------------+

The directive ``@edgedb`` takes a required argument of ``module`` that
specifies the module for the concept used as the top-level field of
the query. The top-level field of the GraphQL query must be a valid
concept. Nested fields must be valid links. If a link is to a
different concept, then it must have at least one nested field
specified.


Arguments
---------

There are some specific conventions as to how ``arguments`` in GraphQL
queries are treated. Filtering the retrieved data is done by
specifying ``arguments`` for the GraphQL query. Consider the following
document:

+--------------------------------------+----------------------------+
| GraphQL                              | EdgeQL equivalent          |
+======================================+============================+
| .. code-block:: graphql              | .. code-block:: eql        |
|                                      |                            |
|   query @edgedb(module: "example") { |   WITH MODULE example      |
|       Book(title: "Spam") {          |   SELECT Book {            |
|           title                      |       title,               |
|           synopsis                   |       synopsis,            |
|       }                              |   } FILTER                 |
|   }                                  |       Book.title = 'Spam'; |
+--------------------------------------+----------------------------+

To specify a filter based on deeper nesting the following convention
for the argument name should be used (``__`` where ``.`` would be
expected in EdgeQL):

+---------------------------------------+---------------------------+
| GraphQL                               | EdgeQL equivalent         |
+=======================================+===========================+
| .. code-block:: graphql               | .. code-block:: eql       |
|                                       |                           |
|   query @edgedb(module: "example") {  |   WITH MODULE example     |
|       Book(author__name:              |   SELECT Book {           |
|               "Alice Smith") {        |       title,              |
|           title                       |       synopsis,           |
|           synopsis                    |   } FILTER                |
|       }                               |       Book.author.name =  |
|   }                                   |           'Alice Smith';  |
+---------------------------------------+---------------------------+


Variables
---------

It is possible to use variables within GraphQL queries. They are
mapped to variables in EdgeQL.

+-----------------------------------+---------------------------------+
| GraphQL                           | EdgeQL equivalent               |
+===================================+=================================+
| .. code-block:: graphql           | .. code-block:: eql             |
|                                   |                                 |
|   query ($name: String!)          |   WITH MODULE example           |
|   @edgedb(module: "example") {    |   SELECT Book {                 |
|       Book(author__name: $name) { |       title,                    |
|           title                   |       synopsis,                 |
|           synopsis                |   } FILTER                      |
|       }                           |       Book.author.name = $name; |
|   }                               |                                 |
+-----------------------------------+---------------------------------+


Mutations
+++++++++

EdgeDB also has GraphQL mutations set up to *insert*, *delete* and
*update* data. To tell EdgeDB which mutation is being applied prefix
the concept name with one of ``insert__``, ``delete__`` or
``update__``. Otherwise the structure of the document is similar to
that of GraphQL queries with the fields and arguments defining the
shape to be returned and possible filters.

Delete
------

The simplest type of mutation to understand is *delete*. The syntax
for it is almost exactly identical to query syntax. The semantics are
that all the objects described by the query will be deleted from the
DB and returned as the result of this operation.

+----------------------------------+----------------------------------+
| GraphQL                          | EdgeQL equivalent                |
+==================================+==================================+
| .. code-block:: graphql          | .. code-block:: eql              |
|                                  |                                  |
|   mutation ($name: String!)      |   WITH MODULE example            |
|   @edgedb(module: "example") {   |   SELECT (                       |
|       delete__Book(author__name: |       DELETE (                   |
|                       $name) {   |           SELECT Book            |
|           title                  |           FILTER                 |
|           synopsis               |               Book.author.name = |
|       }                          |                   $name          |
|   }                              |       )                          |
|                                  |   ) {                            |
|                                  |       title,                     |
|                                  |       synopsis,                  |
|                                  |   };                             |
+----------------------------------+----------------------------------+


Insert
------

*Insert* mutations can be used to add new objects to the DB.
Typically, the only argument that should appear in an *insert*
mutation is the special ``__data`` argument. It is an *InputObject*
that describes the object to be created. Since all other arguments act
as filters, they make no sense as part of *insert* mutation.

+----------------------------------------+--------------------------+
| GraphQL                                | EdgeQL equivalent        |
+========================================+==========================+
| .. code-block:: graphql                | .. code-block:: eql      |
|                                        |                          |
|   mutation ($name: String!)            |   WITH MODULE example    |
|   @edgedb(module: "example") {         |   SELECT (               |
|       insert__Person(__data:           |       INSERT Author {    |
|                       {name: $name}) { |           name := $name  |
|           id                           |       }                  |
|           name                         |   ) {                    |
|       }                                |       id,                |
|   }                                    |       name               |
|                                        |   };                     |
+----------------------------------------+--------------------------+

Notice that it is possible to insert nested objects just like in
EdgeQL in the following manner:

+----------------------------------+--------------------------------+
| GraphQL                          | EdgeQL equivalent              |
+==================================+================================+
| .. code-block:: graphql          | .. code-block:: eql            |
|                                  |                                |
|   mutation ($title: String!,     |   WITH MODULE example          |
|             $name: String!)      |   SELECT (                     |
|   @edgedb(module: "example") {   |       INSERT Book {            |
|       insert__Book(__data: {     |           title := $title,     |
|           title: $title,         |           author: {            |
|           author: {              |               name := $name    |
|               name: $name        |           }                    |
|           }                      |       }                        |
|       }) {                       |   ) {                          |
|           id                     |       id,                      |
|           title                  |       title,                   |
|           author {               |       author: {                |
|               id                 |           id,                  |
|               name               |           name                 |
|           }                      |       }                        |
|       }                          |   };                           |
|   }                              |                                |
+----------------------------------+--------------------------------+

However, sometimes it's necessary to link existing objects to a newly
created one. This is done by using a convention of adding ``__id``
postfix after the field name and providing the *UUID* of the existing
object to be linked.

+----------------------------------+------------------------------------+
| GraphQL                          | EdgeQL equivalent                  |
+==================================+====================================+
| .. code-block:: graphql          | .. code-block:: eql                |
|                                  |                                    |
|     mutation ($title: String!,   |     WITH MODULE example            |
|               $authid: String!)  |     SELECT (                       |
|     @edgedb(module: "example") { |         INSERT Book {              |
|         insert__Book(__data: {   |             title := $title,       |
|             title: $title,       |             author := (            |
|             author__id: $authid  |                 SELECT Object      |
|         }) {                     |                 FILTER Object.id = |
|             id                   |                     $authid        |
|             title                |             )                      |
|             author {             |         }                          |
|                 id               |     ) {                            |
|                 name             |         id,                        |
|             }                    |         title,                     |
|         }                        |         author: {                  |
|     }                            |             id,                    |
|                                  |             name                   |
|                                  |         }                          |
|                                  |     };                             |
+----------------------------------+------------------------------------+



Update
------

*Update* mutations do not create new EdgeDB objects, but update the
data or connections on existing ones. Because they operate on new data
and existing objects *update* mutations make use of both the special
``__data`` arguments and the regular arguments used for filtering. The
filters specify the objects the update should be applied to, whereas
the ``__data`` *InputObject* specifies what the new data is.

The following mutation will update the prices to ``7.99`` for all of
the books of a specified author.

+----------------------------------+-----------------------------------+
| GraphQL                          | EdgeQL equivalent                 |
+==================================+===================================+
| .. code-block:: graphql          | .. code-block:: eql               |
|                                  |                                   |
|     mutation ($name: String!)    |     WITH MODULE example           |
|     @edgedb(module: "example") { |     SELECT (                      |
|         update__Book(            |         UPDATE Book               |
|             __data: {            |         FILTER Book.author.name = |
|                 price: 7.99,     |             $name                 |
|             },                   |         SET {                     |
|             author__name: $name  |             price := 7.99         |
|         ) {                      |         }                         |
|             id                   |     ) {                           |
|             title                |         id,                       |
|             price                |         title,                    |
|         }                        |         price                     |
|     }                            |     };                            |
+----------------------------------+-----------------------------------+

Unlike in EdgeQL there is no generalized way to refer to existing
values in GraphQL *update* mutations. The main premise is that largely
the purpose of updates is to set values entered via some kind of a
form or a user-dialog, therefore the final values are fully known and
do not need to be dynamically computed on the server.
