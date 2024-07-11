.. versionadded:: 4.0

.. _ref_ext_pgtrgm:

============
ext::pg_trgm
============

This extension provides tools for determining similarity of text based on
trigram matching.

Word similarity tools can often supplement :ref:`full-text search
<ref_std_fts>`. Full-text search concentrates on matching words and phrases
typically trying to account for some grammatical variations, while trigram
matching analyzes similarity between words. Thus trigram matching can account
for misspelling or words that aren't dictionary words:

.. code-block:: edgeql-repl

  db> select fts::search(Doc, 'thaco').object{text};
  {}

  db> select Doc{text} filter ext::pg_trgm::word_similar('thaco', Doc.text);
  {
    default::Doc {
      text: 'THAC0 is used in AD&D 2 to determine likelihood of hitting',
    },
  }

The first search attempt fails to produce results because "THAC0" is an
obscure acronym that is misspelled in the query. However, using similarity
search produces a hit because the acronym is not too badly misspelled and is
close enough.

The Postgres that comes packaged with the EdgeDB 4.0+ server includes
``pg_trgm``, as does EdgeDB Cloud. It you are using a separate
Postgres backend, you will need to arrange for it to be installed.

To activate this functionality you can use the :ref:`extension
<ref_datamodel_extensions>` mechanism:

.. code-block:: sdl

    using extension pg_trgm;

That will give you access to the ``ext::pg_trgm`` module where you may find
the following functions:

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::pg_trgm::similarity`
      - :eql:func-desc:`ext::pg_trgm::similarity`

    * - :eql:func:`ext::pg_trgm::similarity_dist`
      - :eql:func-desc:`ext::pg_trgm::similarity_dist`

    * - :eql:func:`ext::pg_trgm::similar`
      - :eql:func-desc:`ext::pg_trgm::similar`

    * - :eql:func:`ext::pg_trgm::word_similarity`
      - :eql:func-desc:`ext::pg_trgm::word_similarity`

    * - :eql:func:`ext::pg_trgm::word_similarity_dist`
      - :eql:func-desc:`ext::pg_trgm::word_similarity_dist`

    * - :eql:func:`ext::pg_trgm::word_similar`
      - :eql:func-desc:`ext::pg_trgm::word_similar`

    * - :eql:func:`ext::pg_trgm::strict_word_similarity`
      - :eql:func-desc:`ext::pg_trgm::strict_word_similarity`

    * - :eql:func:`ext::pg_trgm::strict_word_similarity_dist`
      - :eql:func-desc:`ext::pg_trgm::strict_word_similarity_dist`

    * - :eql:func:`ext::pg_trgm::strict_word_similar`
      - :eql:func-desc:`ext::pg_trgm::strict_word_similar`


In addition to the functions this extension has two indexes that speed up
queries that involve similarity searches: ``ext::pg_trgm::gin`` and
``ext::pg_trgm::gist``.


.. _ref_ext_pgtrgm_config:

Configuration
^^^^^^^^^^^^^

This extension also adds a few configuration options to control some of the
similarity search behavior:

.. code-block:: sdl

    type Config extending cfg::ConfigObject {
      required similarity_threshold: float32;
      required word_similarity_threshold: float32;
      required strict_word_similarity_threshold: float32;
    }

All of the configuration parameters have to take values between 0 and 1.

The ``similarity_threshold`` sets the current similarity threshold that is
used by :eql:func:`ext::pg_trgm::similar` (default is 0.3).

The ``word_similarity_threshold`` sets the current word similarity threshold
that is used by :eql:func:`ext::pg_trgm::word_similar` (default is 0.6).

The ``strict_word_similarity_threshold`` sets the current strict word
similarity threshold that is used by
:eql:func:`ext::pg_trgm::strict_word_similar` (default is 0.5).


------------


.. eql:function:: ext::pg_trgm::similarity(a: str, b: str) -> float32

    Computes how similar two strings are.

    The result is always a value between 0 and 1, where 0 indicates no
    similarity and 1 indicates the strings are identical.

    .. code-block:: edgeql-repl

      db> select ext::pg_trgm::similarity('cat', 'dog');
      {0}
      db> select ext::pg_trgm::similarity('cat', 'cart');
      {0.28571427}
      db> select ext::pg_trgm::similarity('cat', 'car');
      {0.33333337}
      db> select ext::pg_trgm::similarity('cat', 'cat');
      {1}


------------


.. eql:function:: ext::pg_trgm::similarity_dist(a: str, b: str) -> float32

    Computes how distant two strings are.

    The distance between *a* and *b* is simply defined as ``1 -
    ext::pg_trgm::similarity(a, b)``.

    .. code-block:: edgeql-repl

      db> select ext::pg_trgm::similarity_dist('cat', 'dog');
      {1}
      db> select ext::pg_trgm::similarity_dist('cat', 'cart');
      {0.71428573}
      db> select ext::pg_trgm::similarity_dist('cat', 'car');
      {0.6666666}
      db> select ext::pg_trgm::similarity_dist('cat', 'cat');
      {0}


------------


.. eql:function:: ext::pg_trgm::similar(a: str, b: str) -> bool

    Returns whether two strings are similar.

    The result is ``true`` if the :eql:func:`ext::pg_trgm::similarity` between
    the two strings is greater than the currently configured
    :ref:`similarity_threshold <ref_ext_pgtrgm_config>`.

    .. code-block:: edgeql-repl

      db> select ext::pg_trgm::similar('cat', 'dog');
      {false}
      db> select ext::pg_trgm::similar('cat', 'cart');
      {false}
      db> select ext::pg_trgm::similar('cat', 'car');
      {true}
      db> select ext::pg_trgm::similar('cat', 'cat');
      {true}


------------


.. eql:function:: ext::pg_trgm::word_similarity(a: str, b: str) -> float32

    Returns similarity between the first and any part of the second string.

    The result is the greatest similarity between the set of
    trigrams in *a* and any continuous extent of an ordered set
    of trigrams in *b*.

    .. code-block:: edgeql-repl

      db> select ext::pg_trgm::word_similarity('cat', 'Lazy dog');
      {0}
      db> select ext::pg_trgm::word_similarity('cat', 'Dog in a car');
      {0.5}
      db> select ext::pg_trgm::word_similarity('cat', 'Dog catastrophe');
      {0.75}
      db> select ext::pg_trgm::word_similarity('cat', 'Lazy dog and cat');
      {1}


------------


.. eql:function:: ext::pg_trgm::word_similarity_dist(a: str, b: str) \
                    -> float32

    Returns distance between the first and any part of the second string.

    The distance between *a* and *b* is simply defined as ``1 -
    ext::pg_trgm::word_similarity(a, b)``.

    .. code-block:: edgeql-repl

      db> select ext::pg_trgm::word_similarity_dist('cat', 'Lazy dog');
      {1}
      db> select ext::pg_trgm::word_similarity_dist('cat', 'Dog in a car');
      {0.5}
      db> select ext::pg_trgm::word_similarity_dist('cat', 'Dog catastrophe');
      {0.25}
      db> select ext::pg_trgm::word_similarity_dist('cat', 'Lazy dog and cat');
      {0}


------------


.. eql:function:: ext::pg_trgm::word_similar(a: str, b: str) -> bool

    Returns whether the first string is similar to any part of the second.

    The result is ``true`` if the :eql:func:`ext::pg_trgm::word_similarity`
    between the two strings is greater than the currently configured
    :ref:`word_similarity_threshold <ref_ext_pgtrgm_config>`.

    .. code-block:: edgeql-repl

      db> select ext::pg_trgm::word_similar('cat', 'Lazy dog');
      {false}
      db> select ext::pg_trgm::word_similar('cat', 'Dog in a car');
      {false}
      db> select ext::pg_trgm::word_similar('cat', 'Dog catastrophe');
      {true}
      db> select ext::pg_trgm::word_similar('cat', 'Lazy dog and cat');
      {true}


------------


.. eql:function:: ext::pg_trgm::strict_word_similarity(a: str, b: str) \
                    -> float32

    Same as ``word_similarity``, but with stricter boundaries.

    This works much like :eql:func:`ext::pg_trgm::word_similarity`, but also
    forces the match within *b* to happen at word boundaries.

    .. code-block:: edgeql-repl

      db> select ext::pg_trgm::strict_word_similarity('cat', 'Lazy dog');
      {0}
      db> select ext::pg_trgm::strict_word_similarity('cat', 'Dog in a car');
      {0.5}
      db> select ext::pg_trgm::strict_word_similarity(
      ...   'cat', 'Dog catastrophy');
      {0.23076922}
      db> select ext::pg_trgm::strict_word_similarity(
      ...   'cat', 'Lazy dog and cat');
      {1}


------------


.. eql:function:: ext::pg_trgm::strict_word_similarity_dist(a: str, b: str) \
                    -> float32

    Same as ``word_similarity_dist``, but with stricter boundaries.

    This works much like :eql:func:`ext::pg_trgm::word_similarity_dist`, but
    also forces the match within *b* to happen at word boundaries.

    .. code-block:: edgeql-repl

      db> select ext::pg_trgm::strict_word_similarity_dist(
      ...   'cat', 'Lazy dog');
      {1}
      db> select ext::pg_trgm::strict_word_similarity_dist(
      ...   'cat', 'Dog in a car');
      {0.5}
      db> select ext::pg_trgm::strict_word_similarity_dist(
      ...   'cat', 'Dog catastrophy');
      {0.7692308}
      db> select ext::pg_trgm::strict_word_similarity_dist(
      ...   'cat', 'Lazy dog and cat');
      {0}


------------


.. eql:function:: ext::pg_trgm::strict_word_similar(a: str, b: str) -> bool

    Same as ``word_similar``, but with stricter boundaries.

    This works much like :eql:func:`ext::pg_trgm::word_similar`, but
    also forces the match within *b* to happen at word boundaries.

    .. code-block:: edgeql-repl

      db> select ext::pg_trgm::strict_word_similar(
      ...   'cat', 'Lazy dog');
      {false}
      db> select ext::pg_trgm::strict_word_similar(
      ...   'cat', 'Lazy dog');
      {false}
      db> select ext::pg_trgm::strict_word_similar(
      ...   'cat', 'Dog catastrophy');
      {false}
      db> select ext::pg_trgm::strict_word_similar(
      ...   'cat', 'Lazy dog and cat');
      {true}
