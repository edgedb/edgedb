.. eql:section-intro-page:: graphql

.. _ref_graphql_index:

=======
GraphQL
=======


.. toctree::
    :maxdepth: 2
    :hidden:

    graphql
    mutations
    introspection
    cheatsheet


EdgeDB supports `GraphQL queries`__ via the built-in ``graphql`` extension. A
full CRUD API for all object types, their properties (both material and
computed), their links, and all :ref:`aliases <ref_datamodel_aliases>` is
reflected in the GraphQL schema.

Setting up the extension
------------------------

In order to set up GraphQL access to the database, add the following to the
schema:

.. code-block:: sdl

    using extension graphql;

Then create a new migration and apply it.

.. code-block:: bash

  $ edgedb migration create
  $ edgedb migrate

Refer to the :ref:`connection docs <edgedb_client_connection>` for various
methods of running these commands against remotely-hosted instances.

Connection
----------

Once you've activated the extension, your instance will listen for incoming
GraphQL queries via HTTP at the following URL.

``http://<instance-hostname><instance-port>/branch/<branch-name>/graphql``

The default ``branch-name`` will be ``main``, and after initializing your
database, all queries are executed against it by default. If you want to query
another branch instead, simply use that branch name in the URL.

To find the port number associated with a local instance, run ``edgedb
instance list``.

.. code-block:: bash

  $ edgedb instance list
  ┌────────┬──────────────┬──────────┬───────────────┬─────────────┐
  │ Kind   │ Name         │ Port     │ Version       │ Status      │
  ├────────┼──────────────┼──────────┼───────────────┼─────────────┤
  │ local  │ inst1        │ 10700    │ 2.x           │ running     │
  │ local  │ inst2        │ 10702    │ 2.x           │ running     │
  │ local  │ inst3        │ 10703    │ 2.x           │ running     │
  └────────┴──────────────┴──────────┴───────────────┴─────────────┘

To execute a GraphQL query against the branch ``main`` on the instance
named ``inst2``, we would send an HTTP request to
``http://localhost:10702/branch/edgedb/main``.

To determine the URL of an EdgeDB Cloud instance, find the host by running
``edgedb instance credentials -I <org-name>/<instance-name>``. Use the
``host`` and ``port`` from that table in the URL format at the top of this
section. Change the protocol to ``https`` since EdgeDB Cloud instances are
secured with TLS.

.. note::

  The endpoint also provides a `GraphiQL`_ interface to explore the GraphQL
  schema and write queries. Take the GraphQL query endpoint, append
  ``/explore``, and visit that URL in the browser. Under the above example,
  the GraphiQL endpoint is available at
  ``http://localhost:10702/branch/main/graphql/explore``.

Authentication
--------------

.. versionadded:: 4.0

Authentication for the GraphQL endpoint is identical to that for the
:ref:`EdgeQL HTTP endpoint <ref_http_auth>`.

.. _ref_graphql_protocol:

The protocol
------------

EdgeDB can recieve GraphQL queries via both ``GET`` and ``POST`` requests.
Requests can contain the following fields:

- ``query`` - the GraphQL query string
- ``variables`` - a JSON object containing a set of variables. **Optional**
  If the GraphQL query string contains variables, the ``variables`` object is
  required.
- ``globals`` - a JSON object containing global variables. **Optional**. The
  keys must be the fully qualified names of the globals to set (e.g.,
  ``default::current_user`` for the global ``current_user`` in the ``default``
  module).
- ``operationName`` - the name of the operation that must be
  executed. **Optional** If the GraphQL query contains several named
  operations, it is required.

.. note::

  The protocol implementations conform to the official GraphQL `HTTP protocol
  <https://graphql.org/learn/serving-over-http/>`_. The protocol supports
  ``HTTP Keep-Alive``.


POST request (recommended)
^^^^^^^^^^^^^^^^^^^^^^^^^^

The POST request should use ``application/json`` content type and
submit the following JSON-encoded form with the necessary fields.

.. lint-off

.. code-block:: bash

  $ curl \
      -H "Content-Type: application/json" \
      -X POST http://localhost:10787/branch/main/graphql \
      -d '{ "query": "query getMovie($title: String!) { Movie(filter: {title:{eq: $title}}) { id title }}", "variables": { "title": "The Batman" }, "globals": {"default::current_user": "04e52807-6835-4eaa-999b-952804ab40a5"}}'
  {"data": {...}}

.. lint-on


GET request
^^^^^^^^^^^

When using ``GET`` requests, any values for ``query``, ``variables``,
``globals``, or ``operationName`` should be passed as query parameters in the
URL.

.. lint-off

.. code-block:: bash

  $ curl \
      -H application/x-www-form-urlencoded \
      -X GET http://localhost:10787/branch/main/graphql \
      -G \
      --data-urlencode 'query=query getMovie($title: String!) { Movie(filter: {title:{eq: $title}}) { id title }}' \
      --data-urlencode 'variables={ "title": "The Batman" }'
      --data-urlencode 'globals={ "default::current_user": "04e52807-6835-4eaa-999b-952804ab40a5" }'
  {"data": {...}}

.. lint-on


Response format
^^^^^^^^^^^^^^^

The body of the response is JSON in the following format:

.. code-block::

  {
    "data": { ... },
    "errors": [
      { "message": "Error message"}, ...
    ]
  }

Note that the ``errors`` field will only be present if some errors
actually occurred.

.. note::

    Caution is advised when reading ``decimal`` or ``bigint`` values
    (mapped onto ``Decimal`` and ``Bigint`` GraphQL custom scalar
    types) using HTTP protocol because the results are provides in
    JSON format. The JSON specification does not have a limit on
    significant digits, so a ``decimal`` or a ``bigint`` number can be
    losslessly represented in JSON. However, JSON decoders in many
    languages will read all such numbers as some kind of of 32- or
    64-bit number type, which may result in errors or precision loss.
    If such loss is unacceptable, then consider creating a computed
    property which casts the value into ``str`` and decoding it on the
    client side into a more appropriate type.

.. _ref_graphql_limitations:

Known limitations
-----------------

We provide this GraphQL extension to support users who are accustomed to
writing queries in GraphQL. That said, GraphQL is quite limited and verbose
relative to EdgeQL.

There are also some additional limitations:

- Variables can only correspond to *scalar types*; you can't use
  GraphQL ``input`` types. Under the hood, query variables are mapped onto
  EdgeQL parameters, which only support scalar types.

  As a consequence of this, you must declare top-level variables for each
  property for a GraphQL insertion mutation, which can make queries more
  verbose.

- Due to the differences between EdgeQL and GraphQL syntax,
  :eql:type:`enum <std::enum>` types which have values that cannot be
  represented as GraphQL identifiers (e.g. ```N/A``` or ```NOT
  APPLICABLE```) cannot be properly reflected into GraphQL enums.

- Inserting or updating tuple properties is not yet supported.

- :ref:`Link properties<ref_datamodel_link_properties>` are not reflected, as
  GraphQL has no such concept.

- Every non-abstract EdgeDB object type is simultaneously an interface
  and an object in terms of the GraphQL type system, which means that, for
  every one object type name, two names are needed in reflected
  GraphQL. This potentially results in name clashes if the convention
  of using camel-case names for user types is not followed in EdgeDB.


.. __: http://graphql.org/docs/queries/

.. _`GraphiQL`: https://github.com/graphql/graphiql
