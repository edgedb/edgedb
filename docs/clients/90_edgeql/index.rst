.. _ref_edgeql_index:

================
EdgeQL over HTTP
================

EdgeDB can expose an HTTP endpoint for EdgeQL queries. Since HTTP is a
stateless protocol, no :ref:`DDL <ref_eql_ddl>` or functions that
require a session (such as :eql:func:`sys::advisory_lock`,
:eql:func:`sys::advisory_unlock`, :eql:func:`sys::advisory_unlock_all`, and
:eql:func:`sys::sleep`) can be executed using this endpoint. Only one
query per request can be executed.

Here's an example of configuration that will set up EdgeQL over HTTP
access to the database:

.. code-block:: edgeql-repl

    tutorial> CONFIGURE SYSTEM INSERT Port {
    .........     protocol := "edgeql+http",
    .........     database := "your_database_name",
    .........     address := "127.0.0.1",
    .........     port := 8889,
    .........     user := "http",
    .........     concurrency := 4,
    ......... };
    CONFIGURE SYSTEM

This will expose EdgeQL API on port 8889 (or any other port that was
specified).

.. toctree::
    :maxdepth: 2
    :hidden:

    protocol
