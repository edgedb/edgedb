.. _ref_std_net:

.. versionadded:: 6.0

===
Net
===

The ``net`` module provides an interface for performing network-related operations directly from EdgeDB. It is useful for integrating with external services, fetching data from APIs, or triggering webhooks as part of your database logic.

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

Overview
--------

The primary function for scheduling HTTP requests is :eql:func:`net::http::schedule_request`. This function lets you specify the URL, HTTP method, headers, and body of the request. Once scheduled, you can monitor the request's status and process its response when available.

Example Usage
-------------

Here's a simple example of how to use the ``net::http`` module to make a GET request:

.. code-block:: edgeql

  with request := (
      net::http::schedule_request(
          'https://example.com',
          method := net::http::Method.POST,
          headers := [('Content-Type', 'application/json')],
          body := <bytes>$${"key": "value"}$$
      )
  )
  select request.id;

This ID will be helpful if you need to observe a request's response. You can poll the ``ScheduledRequest`` object in order to get any response data or failure information:

1. **Check the State**: Use the ``state`` field to determine the current status of the request.

2. **Handle Failures**: If the request has failed, inspect the ``failure`` field to understand the kind of failure (e.g., ``NetworkError`` or ``Timeout``) and any associated message.

3. **Process the Response**: If the request is completed successfully, access the ``response.body`` to retrieve the data returned by the request. The body is in ``bytes`` format and may need conversion or parsing.

In the following example, we'll query the ``ScheduledRequest`` object we created above using the ID we selected. Once the request is completed or it has failed, this query will return the response data or the failure information:

.. code-block:: edgeql

  with
      request := <std::net::http::ScheduledRequest><uuid>$request_id,
  select request {
      state,
      failure,
      response: {
          status,
          headers,
          body,
      },
  } filter .state in {net::RequestState.Failed, net::RequestState.Completed};

Reference
---------

.. eql:type:: net::http::Method

  An enumeration of supported HTTP methods.

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
                    method: optional net::http::Method = net::http::Method.`GET`, \
                    headers: optional array<tuple<name: str, value: str>> = {} \
                  ) -> net::http::ScheduledRequest

  Schedules an HTTP request.

  Parameters:

  * ``url``: The URL to send the request to.
  * ``body``: The body of the request (optional).
  * ``method``: The HTTP method to use (optional, defaults to GET).
  * ``headers``: The headers to include in the request (optional).

  Returns ``net::http::ScheduledRequest`` object representing
  the scheduled request.

  Example:

  .. code-block:: edgeql

    SELECT net::http::schedule_request(
      'https://example.com',
      method := net::http::Method.POST,
      headers := [('Content-Type', 'application/json')],
      body := <bytes>$${"key": "value"}$$
    );
