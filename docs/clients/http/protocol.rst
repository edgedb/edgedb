.. _ref_edgeqlql_protocol:


Protocol
========

EdgeDB supports GET and POST methods for handling EdgeQL over HTTP
protocol. Both GET and POST methods use the following fields:

- ``query`` - contains the EdgeQL query string
- ``variables``- contains a JSON object where the keys are the parameter names
  from the query and the values are the arguments to be used in this execution
  of the query.
- ``globals``- contains a JSON object where the keys are the fully qualified
  global names and the values are the desired values for those globals.

The protocol supports HTTP Keep-Alive.

GET request
-----------

The HTTP GET request passes the fields as query parameters: ``query``
string and JSON-encoded ``variables`` mapping.


POST request
------------

The POST request should use ``application/json`` content type and
submit the following JSON-encoded form with the necessary fields::

    {
      "query": "...",
      "variables": { "varName": "varValue", ... },
      "globals": {"default::global_name": "value"}
    }


Response
--------

The response format is the same for both methods. The body of the
response is JSON of the following form::

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

.. note::

    Caution is advised when reading ``decimal`` or ``bigint`` values
    using HTTP protocol because the results are provides in JSON
    format. The JSON specification does not have a limit on
    significant digits, so a ``decimal`` or a ``bigint`` number can be
    losslessly represented in JSON. However, JSON decoders in many
    languages will read all such numbers as some kind of of 32- or
    64-bit number type, which may result in errors or precision loss.
    If such loss is unacceptable, then consider casting the value into
    ``str`` and decoding it on the client side into a more appropriate
    type.
