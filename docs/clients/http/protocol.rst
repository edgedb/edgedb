.. _ref_edgeqlql_protocol:


Protocol
========

EdgeDB supports ``GET`` and ``POST`` request methods for handling EdgeQL
over an HTTP protocol. Both of these methods use the following fields:

- ``query`` - contains a string value for an EdgeQL query.
- ``variables`` - contains a JSON object where keys and values
  correspond to the variable names and values. It is required if the
  EdgeQL query has variables, otherwise it is optional.

An HTTP protocol may also support the HTTP ``Keep-Alive`` header as well.


GET request
-----------

An HTTP ``GET`` request pass fields such as query-string parameters (``query``)
and JSON-encoded mapping. (``variables``)


POST request
------------

An HTTP ``POST`` request are recommended to be uses with ``application/json``
for the ``Content-Type`` HTTP header when submitting the following JSON-encoded
form with these necessary fields::

    {
      "query": "...",
      "variables": { "varName": "varValue", ... }
    }


Response
--------

The format of a request's response is the same for both methods. The
``Content-Type`` of a response is returned in ``application/json`` with the following
form::

    {
      "data": [ ... ],
      "error": {
        "message": "Error message",
        "type": "ErrorType",
        "code": 123456
      }
    }

The ``data`` field in a response will be returned as an ``application/json`` content
type of a JSON array.

Note that ``error`` may only be present if an error has actually occurred. The ``error``
field will contain ``message`` and ``type`` fields, with the message; and type
respectively, along with a ``code`` field with an integer representing an
:ref:`error code <ref_protocol_error_codes>`.

.. note::

    When reading values from either ``decimal`` or ``bigint`` with our HTTP protocol,
    keep in mind that all results are provided as the ``application/json`` content type.
    As such, JSON does not constrain nor' restrict the limit of significant digits,
    allowing potential error and/or precision loss to occur. Some JSON decoders in
    languages may interpret these values from 32-to-64 bit representation, resulting in
    less probability in loss of accuracy. If this happens to be undesirable for your needs,
    please consider casting these values into a ``str`` type and decoding it on your
    client-side into an appropriate type suited for your needs.


