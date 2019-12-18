.. _ref_datamodel_aliases:

==================
Expression Aliases
==================

An *alias* is a named set of values produced by an expression.

Aliases can be referred to like regular type in other expressions.
Aliases over queries that return objects essentially define a dynamic
*subtype* of the original object type, which may have additional
properties and links as may be specified by a *shape* in the alias
expression.

Consider the following:

.. code-block:: sdl

    type User {
        required property name -> str;
        multi link friends -> User;
    }

    alias UserAlias := User {
        # declare a computable link
        friend_of := User.<friends[IS User]
    };

One benefit that the ``UserAlias`` provides is making EdgeQL queries
more legible:

.. code-block:: edgeql

    SELECT
        User.<friends[IS User].name
    FILTER
        .name = 'Alice';

    # vs

    SELECT
        UserAlias.friend_of.name
    FILTER
        .name = 'Alice';

Another benefit is that this ``UserAlias`` can now be exposed via
:ref:`GraphQL <ref_graphql_index>` providing access to the computable
link ``friend_of``, that would otherwise be inexpressible in GraphQL:

.. code-block:: graphql

    {
        UserAlias(
            filter: {name: {eq: "Alice"}}
        ) {
            friend_of {
                name
            }
        }
    }



See Also
--------

Alias
:ref:`SDL <ref_eql_sdl_aliases>`,
and :ref:`DDL <ref_eql_ddl_aliases>`.
