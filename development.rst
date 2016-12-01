Development
===========

Debug Flags
-----------

There are few debug flags (environment variables) that can improve EdgeDB
debug and development:

1. ``EDGEDB_DEBUG_SERVER``: print detailed tracebacks;

2. ``EDGEDB_DEBUG_EDGEQL_COMPILE``: dump AST of SQL and EdgeQL;

3. ``EDEGDB_DEBUG_DELTA_EXECUTE``: dump SQL queries when migrations are being
applied.

4. ``EDGEDB_DEBUG_DELTA_PLAN``: dump migrations ASTs.

Example: ``$ EDEGDB_DEBUG_EDGEQL_COMPILE=1 edgedb-server`` will launch an
EdgeDB server which will dump details of EdgeQL to SQL translation to the
standard output.
