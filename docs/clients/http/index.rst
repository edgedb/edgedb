.. _ref_edgeql_http:

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

Then create a new migration and apply it using
:ref:`ref_cli_edgedb_migration_create` and
:ref:`ref_cli_edgedb_migrate`, respectively.

Your instance can now receive EdgeQL queries over HTTP at
``https://<hostname>:<port>/db/<database-name>/edgeql``.

.. note::

    Here's how to determine your local EdgeDB instance's HTTP server URL:

    - The ``hostname`` will be ``localhost``
    - Find the ``port`` by running ``edgedb instance list``. This will print a
      table of all EdgeDB instances on your machine, including their associated
      port number.
    - In most cases, ``database_name`` will be ``edgedb``. An EdgeDB *instance*
      can contain multiple databases. On initialization, a default database
      called ``edgedb`` is created; all queries are executed against this
      database unless otherwise specified.

    To determine the URL of a remote instance you have linked with the CLI, you
    can get both the hostname and port of the instance from the "Port" column
    of the ``edgedb instance list`` table (formatted as ``<hostname>:<port>``).
    The same guidance on local database names applies here.


.. _ref_http_auth:

Authentication
--------------

.. versionadded:: 4.0

.. lint-off

By default, the HTTP endpoint uses :eql:type:`cfg::Password` based
authentication, in which
`HTTP Basic Authentication
<https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme>`_
is used to provide an edgedb username and password.

.. lint-on

This is configurable, however: the HTTP endpoint's authentication
mechanism can be configured by adjusting which
:eql:type:`cfg::AuthMethod` applies to the ``SIMPLE_HTTP``
:eql:type:`cfg::ConnectionTransport`.

If :eql:type:`cfg::JWT` is used, the requests should contain these headers:

* ``X-EdgeDB-User``: The EdgeDB username.

* ``Authorization``: The JWT authorization token prefixed by ``Bearer``.


If :eql:type:`cfg::Trust` is used, no authentication is done at all. This
is not generally recommended, but can be used to recover the pre-4.0
behavior::

    db> configure instance insert cfg::Auth {
    ...     priority := -1,
    ...     method := (insert cfg::Trust { transports := "SIMPLE_HTTP" }),
    ... };
    OK: CONFIGURE INSTANCE


.. toctree::
    :maxdepth: 2
    :hidden:

    protocol
    health-checks
