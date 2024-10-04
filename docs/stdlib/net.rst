.. _ref_std_net:

===
Net
===

The ``net`` module contains types and functions for network-related operations in EdgeDB.

.. list-table::
  :class: funcoptable

  * - :eql:type:`net::RequestState`
    - An enum representing the state of a network request.
  * - :eql:type:`net::RequestFailureKind`
    - An enum representing the kind of failure that occurred for a network request.
  * - :eql:type:`net::http::Method`
    - An enum representing HTTP methods.
  * - :eql:type:`net::http::Response`
    - A type representing an HTTP response.
  * - :eql:type:`net::http::ScheduledRequest`
    - A type representing a scheduled HTTP request.

----------

.. eql:type:: net::RequestState

  An enumeration of possible states for a network request.

  Possible values are:

  * ``Pending``
  * ``InProgress``
  * ``Completed``
  * ``Failed``

----------

.. eql:type:: net::RequestFailureKind

  An enumeration of possible failure kinds for a network request.

  Possible values are:

  * ``NetworkError``
  * ``Timeout``

----------

HTTP Submodule
==============

The ``net::http`` submodule provides types and functions for making HTTP requests.

.. eql:type:: net::http::Method

  An enumeration of HTTP methods.

  Possible values are:

  * ``GET``
  * ``POST``
  * ``PUT``
  * ``DELETE``
  * ``HEAD``
  * ``OPTIONS``
  * ``PATCH``

----------

.. eql:type:: net::http::Response

  A type representing an HTTP response.

  :eql:synopsis:`created_at -> datetime`
    The timestamp when the response was created.

  :eql:synopsis:`status -> int16`
    The HTTP status code of the response.

  :eql:synopsis:`headers -> array<tuple<name: str, value: str>>`
    The headers of the response.

  :eql:synopsis:`body -> bytes`
    The body of the response.

----------

.. eql:type:: net::http::ScheduledRequest

  A type representing a scheduled HTTP request.

  :eql:synopsis:`state -> net::RequestState`
    The current state of the request.

  :eql:synopsis:`created_at -> datetime`
    The timestamp when the request was created.

  :eql:synopsis:`failure -> tuple<kind: net::RequestFailureKind, message: str>`
    Information about the failure, if the request failed.

  :eql:synopsis:`url -> str`
    The URL of the request.

  :eql:synopsis:`method -> net::http::Method`
    The HTTP method of the request.

  :eql:synopsis:`headers -> array<tuple<name: str, value: str>>`
    The headers of the request.

  :eql:synopsis:`body -> bytes`
    The body of the request.

  :eql:synopsis:`response -> net::http::Response`
    The response to the request, if completed.

----------

.. eql:function:: net::http::schedule_request( \
                    url: str, \
                    body: optional bytes = {}, \
                    method: optional net::http::Method = net::http::Method.GET, \
                    headers: optional array<tuple<name: str, value: str>> = {} \
                  ) -> net::http::ScheduledRequest

  Schedules an HTTP request.

  :param url:
      The URL to send the request to.
  :paramtype url: str

  :param body:
      The body of the request (optional).
  :paramtype body: bytes

  :param method:
      The HTTP method to use (optional, defaults to GET).
  :paramtype method: net::http::Method

  :param headers:
      The headers to include in the request (optional).
  :paramtype headers: array<tuple<name: str, value: str>>

  :return: A object representing the scheduled request.
  :returntype: net::http::ScheduledRequest

  Example:

  .. code-block:: edgeql

    SELECT net::http::schedule_request(
      'https://example.com',
      method := net::http::Method.POST,
      headers := [('Content-Type', 'application/json')],
      body := <bytes>$${"key": "value"}$$
    );
