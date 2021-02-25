.. _ref_edgeql_index:

================
EdgeQL over HTTP
================

EdgeDB can expose an HTTP endpoint for EdgeQL queries. Since HTTP is a
stateless protocol, no :ref:`DDL <ref_eql_ddl>`,
:ref:`transaction commands <ref_eql_statements_start_tx>`,
can be executed using this endpoint.  Only one query per request can be
executed.

In order to set up HTTP access to the database add the following to
the schema:

.. code-block:: sdl

    using extension edgeql_http;

Then create a new migration and apply it using :ref:`edgedb
create-migration <ref_cli_edgedb_create_migration>` and :ref:`edgedb
migrate <ref_cli_edgedb_migrate>`, respectively.

``http://127.0.0.1:<instance-port>/db/<database-name>/edgeql`` will
expose GraphQL API. Check the credentials file for your instance at
``$HOME/.edgedb/credentials`` to find out which port the instance is
using.

.. toctree::
    :maxdepth: 2
    :hidden:

    protocol
