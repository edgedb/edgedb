.. _ref_edgeql_http:

================
EdgeQL over HTTP
================

.. toctree::
    :maxdepth: 2
    :hidden:

    protocol
    health-checks

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
``https://<hostname>:<port>/branch/<branch-name>/edgeql``.

.. note::

    Here's how to determine your local EdgeDB instance's HTTP server URL:

    - The ``hostname`` will be ``localhost``
    - Find the ``port`` by running ``edgedb instance list``. This will print a
      table of all EdgeDB instances on your machine, including their associated
      port number.
    - The default ``branch-name`` will be ``main``, and after initializing
      your database, all queries are executed against it by default. If you
      want to query another branch instead, simply use that branch name
      in the URL.

    To determine the URL of an EdgeDB Cloud instance, find the host by running
    ``edgedb instance credentials -I <org-name>/<instance-name>``. Use the
    ``host`` and ``port`` from that table in the URL format above this note.
    Change the protocol to ``https`` since EdgeDB Cloud instances are secured
    with TLS.

    To determine the URL of a self-hosted remote instance you have linked with
    the CLI, you can get both the hostname and port of the instance from the
    "Port" column of the ``edgedb instance list`` table (formatted as
    ``<hostname>:<port>``). The same guidance on local branch names applies
    here.


.. _ref_http_auth:

Authentication
--------------

.. versionadded:: 4.0

.. lint-off

By default, the HTTP endpoint uses :eql:type:`cfg::Password` based
authentication, in which
`HTTP Basic Authentication
<https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme>`_
is used to provide an EdgeDB username and password.

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
