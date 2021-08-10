.. _ref_cli_edgedb_query:


============
edgedb query
============

Execute one or more EdgeQL queries.

.. cli:synopsis::

    edgedb query [OPTIONS] <edgeql-query>...


Description
===========

``edgedb query`` is a terminal command used to execute EdgeQL queries
provided as space-separated strings.


Options
=======

:cli:synopsis:`-F, --output-format=<output_format>`
    Output format: ``json``, ``json-pretty``, ``json-lines``,
    ``tab-separated``. Default is ``json-pretty``.

:cli:synopsis:`-f, --file=<file>`
    Filename to execute queries from. Pass ``--file -`` to execute
    queries from stdin.

:cli:synopsis:`<edgeql-query>`
    Any valid EdgeQL query to be executed.
