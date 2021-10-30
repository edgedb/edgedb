.. _ref_eql_select:

Select
======

- Basic examples
- Object types
- Filter
- Order by
- Limit
- Computed fields
- Polymorphic queries
- Backlinks
- Subqueries

.. _ref_eql_select_backlinks:

.. _ref_eql_polymorphic_queries:

Polymorphic Queries
-------------------

:index: poly polymorphism nested shapes

A link target can be an abstract type, thus allowing objects of
different extending types to be referenced.  This necessitates writing
*polymorphic queries* that could fetch different data depending on the
type of the actual objects.  Consider the following schema:

.. code-block:: sdl

    abstract type Named {
        required property name -> str {
            delegated constraint exclusive;
        }
    }

    type User extending Named {
        property avatar -> str;
        multi link favorites -> Named;
    }

    type Game extending Named {
        property price -> int64;
    }

    type Article extending Named {
        property url -> str;
    }

Every ``User`` can have its ``favorites`` link point to either other
``User``, ``Game``, or ``Article``.  To fetch data related to
different types of objects in the ``favorites`` link the following
syntax can be used:

.. code-block:: edgeql

    SELECT User {
        name,
        avatar,
        favorites: {
            # common to all Named
            name,

            # specific to Games
            [IS Game].price,

            # specific to Article
            [IS Article].url,

            # specific to User
            [IS User].avatar,

            # a computed property tracking how many favorites
            # does my favorite User have?
            favorites_count := count(
                # start the path at the root of the shape
                User.favorites[IS User].favorites)
        }
    }

The :eql:op:`[IS TypeName] <ISINTERSECT>` construct can be used in
:ref:`paths <ref_eql_expr_paths>` to restrict the target to a specific
type.  When it is used in :ref:`shapes <ref_eql_expr_shapes>` it
allows to create polymorphic nested queries.

Another scenario where polymorphic queries may be useful is when a
link target is a :eql:op:`union type <TYPEOR>`.

It is also possible to fetch data that contains only one of the
possible types of ``favorites`` even if a particular ``User`` has a
mix of everything:

.. code-block:: edgeql

    # User + favorite Articles only
    SELECT User {
        name,
        favorites[IS Article]: {
            name,
            url
        }
    }
