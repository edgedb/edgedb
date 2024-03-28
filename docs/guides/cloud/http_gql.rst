.. _ref_guide_cloud_http_gql:

===================
HTTP & GraphQL APIs
===================

:edb-alt-title: Querying EdgeDB Cloud over HTTP and GraphQL

Using EdgeDB Cloud via HTTP and GraphQL works the same as :ref:`using any other
EdgeDB instance <ref_edgeql_http>`. The two differences are in **how to
discover your instance's URL** and **authentication**.


Enabling
========

EdgeDB Cloud can expose an HTTP endpoint for EdgeQL queries. Since HTTP is a
stateless protocol, no :ref:`DDL <ref_eql_ddl>` or :ref:`transaction commands
<ref_eql_statements_start_tx>`, can be executed using this endpoint.  Only one
query per request can be executed.

In order to set up HTTP access to the database add the following to
the schema:

.. code-block:: sdl

    using extension edgeql_http;

Then create a new migration and apply it using
:ref:`ref_cli_edgedb_migration_create` and
:ref:`ref_cli_edgedb_migrate`, respectively.

Your instance can now receive EdgeQL queries over HTTP at
``https://<host>:<port>/branch/<branch-name>/edgeql``.


Instance URL
============

To determine the URL of an EdgeDB Cloud instance, find the host by running
``edgedb instance credentials -I <org-name>/<instance-name>``. Use the
``host`` and ``port`` from that table in the URL format above this note.
Change the protocol to ``https`` since EdgeDB Cloud instances are secured
with TLS.

Your instance can now receive EdgeQL queries over HTTP at
``https://<hostname>:<port>/branch/<branch-name>/edgeql``.


Authentication
==============


To authenticate to your EdgeDB Cloud instance, first create a secret key using
the EdgeDB Cloud UI or :ref:`ref_cli_edgedb_cloud_secretkey_create`. Use the
secret key as your token with the bearer authentication method. Here is an
example showing how you might send the query ``select Person {*};`` using cURL:

.. lint-off

.. code-block:: bash

    $ curl -G https://<cloud-instance-host>:<cloud-instance-port>/branch/main/edgeql \
       -H "Authorization: Bearer <secret-key> \
       --data-urlencode "query=select Person {*};"

.. lint-on


Usage
=====

Usage of the HTTP and GraphQL APIs is identical on an EdgeDB Cloud instance.
Reference the HTTP and GraphQL documentation for more information.


HTTP
----

- :ref:`Overview <ref_edgeql_http>`
- :ref:`ref_edgeqlql_protocol`
- :ref:`ref_edgeql_http_health_checks`


GraphQL
-------

- :ref:`Overview <ref_graphql_index>`
- :ref:`ref_graphql_overview`
- :ref:`ref_graphql_mutations`
- :ref:`ref_graphql_introspection`
- :ref:`ref_cheatsheet_graphql`
