.. _ref_graphql_protocol:


Protocol
========

EdgeDB supports GET and POST methods for handling GraphQL over HTTP
protocol. Both GET and POST methods use the following fields:

- ``query`` - contains the GraphQL query string
- ``operationName`` - contains the name of the operation that must be
  executed. It is required if the GraphQL query contains several named
  operations, otherwise it is optional.
- ``variables`` - contains a JSON object where keys and values
  correspond to the variable names and values. It is required if the
  GraphQL query has variables, otherwise it is optional.

GET request
-----------

The HTTP GET request passes the fields as query parameters: ``query``,
``operationName``, and ``variables``.


POST request
------------

The POST request should use ``application/json`` content type and
submit the following JSON-encoded form with the necessary fields:

.. code-block::

    {
      "query": "...",
      "operationName": "...",
      "variables": { "varName": "varValue", ... }
    }


Response
--------

The response format is the same for both methods. The body of the
response is JSON of the following form:

.. code-block::

    {
      "data": { ... },
      "errors": [
        { "message": "Error message"}, ...
      ]
    }

Note that the ``errors`` field will only be present if some errors
actually occurred.
