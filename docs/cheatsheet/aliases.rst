.. _ref_cheatsheet_aliases:

Aliases
=======

.. note::

    Aliases are commonly defined by :ref:`migrations
    <ref_cheatsheet_migrations>` using :ref:`SDL <ref_eql_sdl>`.

Define an alias that merges some information from links as computable
properties, this is a way of flattening a nested structure:

.. code-block:: sdl

    alias ReviewAlias := Review {
        # It will already have all the Review
        # properties and links.
        author_name := .author.name,
        movie_title := .movie.title,
    }

Define an alias for traversing a link backwards, this is especially
useful for GraphQL access:

.. code-block:: sdl

    alias MovieAlias := Movie {
        # A computable link for accessing all the
        # reviews for this movie.
        reviews := .<movie[IS Review]
    }

.. note::

    Aliases allow to use the full power of EdgeQL (expressions, aggregate
    functions, backwards link navigation) from :ref:`GraphQL
    <ref_graphql_index>`.

The aliases defined above allow you to query ``MovieAlias`` with
:ref:`GraphQL <ref_cheatsheet_graphql>`.
