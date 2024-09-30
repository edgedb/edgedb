.. versionadded:: 4.0

.. _ref_ext_pgunaccent:

================
ext::pg_unaccent
================

This extension provides a dictionary for removing accents (diacritic signs) from
text.

.. code-block:: edgeql-repl

  db> select ext::pg_unaccent::unaccent('Hôtel de la Mer');
  {'Hotel de la Mer'}

To activate this functionality you can use the :ref:`extension
<ref_datamodel_extensions>` mechanism:

.. code-block:: sdl

    using extension pg_unaccent;

That will give you access to the ``ext::pg_unaccent`` module where you may find
the function ``unaccent``.

PostgreSQL extension ``unaccent`` also supports creating dictionaries that are
used by other PostgreSQL text search, such as ``to_tsvector``. EdgeDB extension
currently does not support creating such dictionaries, but one can use the 
``unaccent`` function to achieve the same effect:

.. code-block:: sdl

    type Post {
        title: str;

        index fts::index on ((
            fts::with_options(
                ext::pg_unaccent::unaccent(.title),
                language := fts::Language.fra
            ),
        ));
    };

.. code-block:: edgeql-repl

  db> select fts::search(
  ...   Post,
  ...   ext::pg_unaccent::unaccent('Hôtel'),
  ...   language := 'eng',
  ... ).object.title;
  {'Hôtel de la Mer'}
