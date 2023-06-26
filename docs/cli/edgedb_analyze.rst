.. _ref_cli_edgedb_analyze:


==============
edgedb analyze
==============

.. note::

    This CLI feature is compatible with EdgeDB server 3.0 and above.

Run a query performance analysis on the given query.

.. cli:synopsis::

	edgedb analyze [<options>] <query>

Here's example ``analyze`` output from a simple query:

.. lint-off

.. code-block::

    Contexts
    analyze select ➊ Hero {name, secret_identity, ➋ villains: {name, nemesis: {name}}}
    Shape
    ╰──➊ default::Hero (cost=20430.96)
       ╰──➋ .villains: default::Villain, default::Hero (cost=35.81)

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
