.. _ref_cli_edgedb_query:


============
edgedb query
============

Execute one or more EdgeQL queries.

.. cli:synopsis::

    edgedb [<connection-option>...] query <edgeql-query>...


Description
===========

``edgedb query`` is a terminal command used to execute EdgeQL queries
provided as space-separated strings. An alternative way to access this
functionality is by using ``edgedb -c``.


Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The ``query`` command
    runs in the database it is connected to.

:cli:synopsis:`<edgeql-query>`
    Any valid EdgeQL query to be executed.
