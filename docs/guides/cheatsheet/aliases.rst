.. _ref_cheatsheet_aliases:

Declaring aliases
=================


Define an alias that merges some information from links as computed
properties, this is a way of flattening a nested structure:

.. code-block:: sdl

    alias ReviewAlias := Review {
        # It will already have all the Review
        # properties and links.
        author_name := .author.name,
        movie_title := .movie.title,
    }


----------


Define an alias for traversing a :ref:`backlink
<ref_datamodel_links>`, this is especially useful for GraphQL access:

.. code-block:: sdl

    alias MovieAlias := Movie {
        # A computed link for accessing all the
        # reviews for this movie.
        reviews := .<movie[is Review]
    }

.. note::

    Aliases allow to use the full power of EdgeQL (expressions,
    aggregate functions, :ref:`backlink <ref_datamodel_links>`
    navigation) from :ref:`GraphQL <ref_graphql_index>`.

The aliases defined above allow you to query ``MovieAlias`` with
:ref:`GraphQL <ref_cheatsheet_graphql>`.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Aliases <ref_datamodel_aliases>`
  * - :ref:`SDL > Aliases <ref_eql_sdl_aliases>`
  * - :ref:`DDL > Aliases <ref_eql_ddl_aliases>`
