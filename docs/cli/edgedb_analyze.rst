.. _ref_cli_edgedb_analyze:


==============
edgedb analyze
==============

.. note::

    This CLI feature is compatible with EdgeDB server 3.0 and above.

.. note::

    Performance analysis is also available in our :ref:`CLI REPL
    <ref_cli_edgedb>` and the UI's REPL and query builder (both accessible by
    running :ref:`ref_cli_edgedb_ui` to invoke your instance's UI). Use it by
    prepending your query with ``analyze``.

Run a query performance analysis on the given query.

.. cli:synopsis::

	edgedb analyze [<options>] <query>

An example of ``analyze`` output from a simple query:

.. lint-off

.. code-block::

    ──────────────────────────────────────── Query ────────────────────────────────────────
    analyze select ➊  Hero {name, secret_identity, ➋  villains: {name, ➌  nemesis: {name}}};

    ──────────────────────── Coarse-grained Query Plan ────────────────────────
                       │ Time     Cost Loops Rows Width │ Relations
    ➊ root            │  0.0 69709.48   1.0  0.0    32 │ Hero
    ╰──➋ .villains    │  0.0     92.9   0.0  0.0    32 │ Villain, Hero.villains
    ╰──➌ .nemesis     │  0.0     8.18   0.0  0.0    32 │ Hero

.. lint-on


Options
=======

The ``analyze`` command runs on the database it is connected to. For specifying
the connection target see :ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`<query>`
    The query to analyze. Be sure to wrap the query in quotes.

:cli:synopsis:`--expand`
    Print expanded output of the query analysis

:cli:synopsis:`--debug-output-file <debug_output_file>`
    Write analysis into the JSON file specified instead of formatting

:cli:synopsis:`--read-json <read_json>`
    Read JSON file instead of executing a query

Media
=====

.. edb:youtube-embed:: WoHJu0nq5z0