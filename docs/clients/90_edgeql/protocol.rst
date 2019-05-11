.. _ref_edgeqlql_protocol:


Protocol
========

EdgeDB supports GET and POST methods for handling EdgeQL over HTTP
protocol. Both GET and POST methods use the following fields:

- ``query`` - contains the EdgeQL query string
- ``variables`` - contains a JSON object where keys and values
  correspond to the variable names and values. It is required if the
  EdgeQL query has variables, otherwise it is optional.

The protocol supports HTTP Keep-Alive.

GET request
-----------

The HTTP GET request passes the fields as query parameters: ``query``
string and JSON-encoded ``variables`` mapping.


POST request
------------

The POST request should use ``application/json`` content type and
submit the following JSON-encoded form with the necessary fields:

.. code-block::

    {
      "query": "...",
      "variables": { "varName": "varValue", ... }
    }


Response
--------

The response format is the same for both methods. The body of the
response is JSON of the following form:

.. code-block::

    {
      "data": [ ... ],
      "error": {
        "message": "Error message",
        "type": "ErrorType",
        "code": 123456
      }
    }

The ``data`` response field will contain the response set serialized
as a JSON array.

Note that the ``error`` field will only be present if an error
actually occurred. The ``error`` will further contain the ``message``
field with the error message string, the ``type`` field with the name
of the type of error and the ``code`` field with an integer
:ref:`error code <ref_protocol_error_codes>`.
