.. _ref_std_fts:

.. versionadded:: 4.0

================
Full-text Search
================

The ``fts`` built-in module contains various tools that enable full-text
search functionality in EdgeDB.

.. note::

    Since full-text search is a natural language search, it may not be ideal
    for your use case, particularly if you want to find partial matches. In
    that case, you may want to look instead at :ref:`ref_ext_pgtrgm`.

.. list-table::
    :class: funcoptable

    * - :eql:type:`fts::Language`
      - Common languages :eql:type:`enum`

    * - :eql:type:`fts::PGLanguage`
      - Postgres languages :eql:type:`enum`

    * - :eql:type:`fts::Weight`
      - Weight category :eql:type:`enum`

    * - :eql:type:`fts::document`
      - Opaque document type

    * - :eql:func:`fts::search`
      - :eql:func-desc:`fts::search`

    * - :eql:func:`fts::with_options`
      - :eql:func-desc:`fts::with_options`

When considering FTS functionality our goal was to come up with an interface
that could support different backend FTS providers. To achieve that we've
identified the following components to the FTS functionality:

1) Valid FTS targets must be indexed.
2) The expected language should be specified at the time of creating an index.
3) It should be possible to mark document parts as having different relevance.
4) It should be possible to assign custom weights at runtime so as to make
   searching more flexible.
5) The search query should be close to what people are already used to.

To address (1) we introduce a special ``fts::index``. The presence of this
index in a type declaration indicates that the type in question can be subject
to full-text search. This is an unusual index as it actually affects the
results of :eql:func:`fts::search` function. This is unlike most indexes which
only affect the performance and not the actual results. Another special
feature of ``fts::index`` is that at most one such index can be declared per
type. If a type inherits this index from a parent and also declares its own,
only the new index applies and fully overrides the ``fts::index`` inherited
from the parent type. This means that when dealing with a hierarchy of
full-text-searchable types, each type can customize what gets searched as
needed.

The language (2) is defined as part of the ``fts::index on`` expression. A
special function :eql:func:`fts::with_options` is used for that purpose:

.. code-block:: sdl

    type Item {
      required available: bool {
        default := false;
      };
      required name: str;
      description: str;

      index fts::index on (
        fts::with_options(
          .name,
          language := fts::Language.eng
        )
      );
    }

The above declaration specifies that ``Item`` is full-text-searchable,
specifically by examining the ``name`` property (and ignoring ``description``)
and assuming that the contents of that property are in English.

Marking different parts of the document as having different relevance (3) can
also be done by the :eql:func:`fts::with_options` function:

.. code-block:: sdl

    type Item {
      required available: bool {
        default := false;
      };
      required name: str;
      description: str;

      index fts::index on ((
        fts::with_options(
          .name,
          language := fts::Language.eng,
          weight_category := fts::Weight.A,
        ),
        fts::with_options(
          .description,
          language := fts::Language.eng,
          weight_category := fts::Weight.B,
        )
      ));
    }

The schema now indicates that both ``name`` and ``description`` properties of
``Item`` are full-text-searchable. Additionally, the ``name`` and
``description`` have potentially different relevance.

By default :eql:func:`fts::search` assumes that the weight categories ``A``,
``B``, ``C``, and ``D`` have the following weights: ``[1, 0.5, 0.25, 0.125]``.
This makes each successive category relevance score halved.

Consider the following:

.. code-block:: edgeql-repl

    edgedb> select Item{name, description};
    {
      default::Item {name: 'Canned corn', description: {}},
      default::Item {
        name: 'Candy corn',
        description: 'A great Halloween treat',
      },
      default::Item {
        name: 'Sweet',
        description: 'Treat made with corn sugar',
      },
    }

    edgedb> with res := (
    .......   select fts::search(Item, 'corn treat', language := 'eng')
    ....... )
    ....... select res.object {name, description, score := res.score}
    ....... order by res.score desc;
    {
      default::Item {
        name: 'Candy corn',
        description: 'A great Halloween treat',
        score: 0.4559453,
      },
      default::Item {
        name: 'Canned corn',
        description: {},
        score: 0.30396354,
      },
      default::Item {
        name: 'Sweet',
        description: 'Treat made with corn sugar',
        score: 0.30396354,
      },
    }

As you can see, the highest scoring match came from an ``Item`` that had the
search terms appear in both ``name`` and ``description``. It is also apparent
that matching a single term from the search query in the ``name`` property
scores the same as matching two terms in ``description`` as we would expect
based on their weight categories. We can, however, customize the weights (4)
to change this trend:

.. code-block:: edgeql-repl

    edgedb> with res := (
    .......   select fts::search(
    .......     Item, 'corn treat',
    .......     language := 'eng',
    .......     weights := [0.2, 1],
    .......   )
    ....... )
    ....... select res.object {name, description, score := res.score}
    ....... order by res.score desc;
    {
      default::Item {
        name: 'Sweet',
        description: 'Treat made with corn sugar',
        score: 0.6079271,
      },
      default::Item {
        name: 'Candy corn',
        description: 'A great Halloween treat',
        score: 0.36475626,
      },
      default::Item {
        name: 'Canned corn',
        description: {},
        score: 0.06079271,
      },
    }

We can even use custom weights to completely ignore one of the properties
(e.g. ``name``) in our search, although we also need to add a filter based on
the score to make this work properly:

.. code-block:: edgeql-repl

    edgedb> with res := (
    .......   select fts::search(
    .......     Item, 'corn treat',
    .......     language := 'eng',
    .......     weights := [0, 1],
    .......   )
    ....... )
    ....... select res.object {name, description, score := res.score}
    ....... filter res.score > 0
    ....... order by res.score desc;
    {
      default::Item {
        name: 'Sweet',
        description: 'Treat made with corn sugar',
        score: 0.6079271,
      },
      default::Item {
        name: 'Candy corn',
        description: 'A great Halloween treat',
        score: 0.30396354,
      },
    }

Finally, the search query supports features for fine-tuning (5). By default,
all search terms are desirable, but ultimately optional. You can enclose a
term or even a phrase in ``"..."`` to indicate that this specific term is of
increased importance and should appear in all matches:

.. code-block:: edgeql-repl

    edgedb> with res := (
    .......   select fts::search(
    .......     Item, '"corn sugar"',
    .......     language := 'eng',
    .......   )
    ....... )
    ....... select res.object {name, description, score := res.score}
    ....... order by res.score desc;
    {
      default::Item {
        name: 'Sweet',
        description: 'Treat made with corn sugar',
        score: 0.4955161,
      },
    }

Only one ``Item`` contains the phrase "corn sugar" and incomplete matches are
omitted.

The search query can also use ``AND`` (using upper-case to indicate that it is
a query modifier and not part of the query) to indicate whether terms are
required or optional:

.. code-block:: edgeql-repl

    edgedb> with res := (
    .......   select fts::search(
    .......     Item, 'sweet AND treat',
    .......     language := 'eng',
    .......   )
    ....... )
    ....... select res.object {name, description, score := res.score}
    ....... order by res.score desc;
    {
      default::Item {
        name: 'Sweet',
        description: 'Treat made with corn sugar',
        score: 0.70076555,
      },
    }

Adding a ``!`` in front of a search term marks it as something that
the matching object *must not* contain:

.. code-block:: edgeql-repl

    edgedb> with res := (
    .......   select fts::search(
    .......     Item, '!treat',
    .......     language := 'eng',
    .......   )
    ....... )
    ....... select res.object {name, description, score := res.score}
    ....... order by res.score desc;
    {
      default::Item {
        name: 'Canned corn',
        description: {},
        score: 0,
      },
    }

.. note::

    EdgeDB 4.0 only supports Postgres full-text search backend. Support for
    other backends is still in development.

----------


.. eql:type:: fts::Language

    An :eql:type:`enum` for representing commonly supported languages.

    When indexing an object for full-text search it is important to specify
    the expected language by :eql:func:`fts::with_options` function. This
    particular :eql:type:`enum` represents languages that are common across
    several possible [future] backend implementations and thus are "safe" even
    if the backend implementation switches from one of the options to another.
    This generic enum is the recommended way of specifying the language.

    The following `ISO 639-3 <iso639_>`_ language identifiers are available:
    ``ara``, ``hye``, ``eus``, ``cat``, ``dan``, ``nld``, ``eng``, ``fin``,
    ``fra``, ``deu``, ``ell``, ``hin``, ``hun``, ``ind``, ``gle``, ``ita``,
    ``nor``, ``por``, ``ron``, ``rus``, ``spa``, ``swe``, ``tur``.

----------


.. eql:type:: fts::PGLanguage

    An :eql:type:`enum` for representing languages supported by PostgreSQL.

    When indexing an object for full-text search it is important to specify
    the expected language by :eql:func:`fts::with_options` function. This
    particular :eql:type:`enum` represents languages that are available in
    PostgreSQL implementation of full-text search.

    The following `ISO 639-3 <iso639_>`_ language identifiers are available:
    ``ara``, ``hye``, ``eus``, ``cat``, ``dan``, ``nld``, ``eng``, ``fin``,
    ``fra``, ``deu``, ``ell``, ``hin``, ``hun``, ``ind``, ``gle``, ``ita``,
    ``lit``, ``npi``, ``nor``, ``por``, ``ron``, ``rus``, ``srp``, ``spa``,
    ``swe``, ``tam``, ``tur``, ``yid``.

    Additionally, the ``xxx_simple`` identifier is also available to represent
    the ``pg_catalog.simple`` language setting.

    Unless you need some particular language setting that is not available in
    the :eql:type:`fts::Language`, it is recommended that you use the more
    general lanuguage enum instead.


----------


.. eql:type:: fts::Weight

    An :eql:type:`enum` for representing weight categories.

    When indexing an object for full-text search different properties of this
    object may have different significance. To account for that, they can be
    assigned different weight categories by using
    :eql:func:`fts::with_options` function. There are four available weight
    categories: ``A``, ``B``, ``C``, or ``D``.


----------


.. eql:type:: fts::document

    An opaque transient type used in ``fts::index``.

    This type is technically what the ``fts::index`` expects as a valid ``on``
    expression. It cannot be directly instantiated and can only be produced as
    the result of applying the special :eql:func:`fts::with_options` function.
    Thus this type only appears in full-text search index definitions and
    cannot appear as either a property type or anywhere in regular queries.


------------


.. eql:function:: fts::search( \
                    object: anyobject, \
                    query: str, \
                    named only language: str = <str>fts::Language.eng, \
                    named only weights: optional array<float64> = {}, \
                  ) -> optional tuple<object: anyobject, score: float32>

    Perform full-text search on a target object.

    This function applies the search ``query`` to the specified object. If a
    match is found, the result will consist of a tuple with the matched
    ``object`` and the corresponding ``score``. A higher ``score`` indicates a
    better match. In case no match is found, the function will return an empty
    set ``{}``. Likewise, ``{}`` is returned if the ``object`` has no
    ``fts::index`` defined for it.

    The ``language`` parameter can be specified in order to match the expected
    indexed language. In case of mismatch there is a big chance that the query
    will not produce the expected results.

    The optional ``weights`` parameter can be passed in order to provide
    custom weights to the different weight groups. By default, the weights are
    ``[1, 0.5, 0.25, 0.125]`` representing groups of diminishing significance.


------------


.. eql:function:: fts::with_options( \
                    text: str, \
                    NAMED ONLY language: anyenum, \
                    NAMED ONLY weight_category: optional fts::Weight = \
                    fts::Weight.A, \
                  ) -> fts::document

    Assign language and weight category to a document portion.

    This is a special function that can only appear inside ``fts::index``
    expressions.

    The ``text`` expression specifies the portion of the document that will be
    indexed and available for full-text search.

    The ``language`` parameter specifies the expected language of the ``text``
    expression. This affects how the index accounts for grammatical variants
    of a given word (e.g. how plural and singular forms are determined, etc.).

    The ``weight_category`` parameter assigns one of four available weight
    categories to the ``text`` expression: ``A``, ``B``, ``C``, or ``D``. By
    themselves, the categories simply group together portions of the document
    so that these groups can be ascribed different significance by the
    :eql:func:`fts::search` function. By default it is assumed that each
    successive category is half as significant as the previous, starting with
    ``A`` as the most significant. However, these default weights can be
    overridden when making a call to :eql:func:`fts::search`.


.. _iso639: https://iso639-3.sil.org/code_tables/639/data