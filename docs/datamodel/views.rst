.. _ref_datamodel_views:

=====
Views
=====

A *view* is a named subset of an existing type defined by a query.  A
view can be referred to like a regular type in other expressions.
Views over queries that return objects essentially define a *view
subtype* of the original object type, which may have different
properties and links as may be specified by a *shape* in the view
expression.

Consider the following:

.. code-block:: sdl

    type User {
        required property name -> str;
        multi link friends -> User;
    }

    view UserView := User {
        # declare a computable link
        friend_of := User.<friends[IS User]
    };

One benefit that the ``UserView`` provides is making EdgeQL queries
more legible:

.. code-block:: edgeql

    SELECT
        User.<friends[IS User].name
    FILTER
        .name = 'Alice';

    # vs

    SELECT
        UserView.friend_of.name
    FILTER
        .name = 'Alice';

Another benefit is that this ``UserView`` can now be exposed via
:ref:`GraphQL <ref_graphql_index>` providing access to the computable
link ``friend_of``, that would otherwise be inexpressible in GrapQL:

.. code-block:: graphql

    {
        UserView(
            filter: {name: {eq: "Alice"}}
        ) {
            friend_of {
                name
            }
        }
    }
