.. _ref_eql_analyze:

.. versionadded:: 3.0

Analyze
=======

:index: performance

Prefix an EdgeQL query with ``analyze`` to run a performance analysis of that
query.

.. code-block:: edgeql-repl

  db> analyze select Hero {
  ...   name,
  ...   secret_identity,
  ...   villains: {
  ...     name,
  ...     nemesis: {
  ...       name
  ...     }
  ...   }
  ... };
  ──────────────────────────────────────── Query ────────────────────────────────────────
  analyze select ➊  Hero {name, secret_identity, ➋  villains: {name, ➌  nemesis: {name}}};

  ──────────────────────── Coarse-grained Query Plan ────────────────────────
                    │ Time     Cost Loops Rows Width │ Relations
  ➊ root            │  0.0 69709.48   1.0  0.0    32 │ Hero
  ╰──➋ .villains    │  0.0     92.9   0.0  0.0    32 │ Villain, Hero.villains
  ╰──➌ .nemesis     │  0.0     8.18   0.0  0.0    32 │ Hero


.. note::

    In addition to using the ``analyze`` statement in the CLI or UI's REPL, you
    may also run performance analysis via our CLI's :ref:`analyze command
    <ref_cli_edgedb_analyze>` and the UI's query builder (accessible by running
    :ref:`ref_cli_edgedb_ui` to invoke your instance's UI) by prepending your
    query with ``analyze``. This method offers helpful visualizations to to
    make it easy to understand your query's performance.

After analyzing a query, you may run the ``\expand`` command in the REPL to see
more fine-grained performance metrics on the previously analyzed query.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`CLI > edgedb analyze <ref_cli_edgedb_analyze>`
  * - :ref:`Reference > EdgeQL > analyze <ref_eql_statements_analyze>`
