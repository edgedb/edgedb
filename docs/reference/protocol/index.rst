.. _ref_protocol_overview:

===============
Binary protocol
===============

EdgeDB uses a message-based binary protocol for communication between
clients and servers.  The protocol is supported over TCP/IP.


.. toctree::
    :maxdepth: 3
    :hidden:

    messages
    errors
    typedesc
    dataformats


.. _ref_protocol_connecting:

Connecting to EdgeDB
====================

The EdgeDB binary protocol has two modes of operation: sockets and HTTP
tunnelling. When connecting to EdgeDB, the client can specify an accepted
`ALPN Protocol`_ to use. If the client does not specify an ALPN protocol,
HTTP tunnelling is assumed.

Sockets
-------

When using the ``edgedb-binary`` ALPN protocol, the client and server
communicate over a raw TCP/IP socket, following the :ref:`message format
<ref_message_format>` and :ref:`message flow <ref_message_flow>` described
below.

.. _ref_http_tunnelling:

HTTP Tunnelling
---------------

HTTP tunnelling differs in a few ways:

*  Authentication is handled at ``/auth/token``.

.. versionchanged:: _default

    *  Query execution is handled at ``/db/{DATABASE}``.

.. versionchanged:: 5.0

    *  Query execution is handled at ``/branch/{BRANCH}``.

*  Transactions are not supported.

The :ref:`authentication <ref_authentication>` phase is handled by sending
``GET`` requests to ``/auth/token`` with the ``Authorization`` header
containing the authorization payload with the format:

.. code-block::

  Authorization: {AUTH METHOD} data={PAYLOAD}

The client then reads the ``www-authenticate`` response header with the
following format:

.. code-block::

  www-authenticate: {AUTH METHOD} {AUTH PAYLOAD}

The auth payload's format is described by the auth method, usually
``SCRAM-SHA-256``. If the auth method differs from the requested method,
the client should abort the authentication attempt.

.. versionchanged:: _default

    Once the :ref:`authentication <ref_authentication>` phase is complete, the
    final response's body will contain an authorization token used to authenticate
    the HTTP connection. The client then sends any following message to
    ``/db/{DATABASE}`` with the following headers:

.. versionchanged:: 5.0

    Once the :ref:`authentication <ref_authentication>` phase is complete, the
    final response's body will contain an authorization token used to authenticate
    the HTTP connection. The client then sends any following message to
    ``/branch/{BRANCH}`` with the following headers:

* ``X-EdgeDB-User``: The username specified in the
  :ref:`connection parameters <ref_reference_connection>`.

* ``Authorization``: The authorization token received from the
  :ref:`authentication <ref_authentication>` phase, prefixed by ``Bearer``.

* ``Content-Type``: Always ``application/x.edgedb.v_1_0.binary``.

The response should be checked to match the content type, and the body should
be parsed as the :ref:`message format <ref_message_format>` described below;
multiple message can be included in the response body, and should be parsed in
order.

.. _ALPN Protocol:
    https://github.com/edgedb/rfcs/blob/master/text/
    1008-tls-and-alpn.rst#alpn-and-protocol-changes

.. _ref_protocol_conventions:

Conventions and data Types
==========================

The message format descriptions in this section use a C-like struct definitions
to describe their layout.  The structs are *packed*, i.e. there are never
any alignment gaps.

The following data types are used in the descriptions:

.. list-table::
    :class: funcoptable

    * - ``int8``
      - 8-bit integer

    * - ``int16``
      - 16-bit integer, most significant byte first

    * - ``int32``
      - 32-bit integer, most significant byte first

    * - ``int64``
      - 64-bit integer, most significant byte first

    * - ``uint8``
      - 8-bit unsigned integer

    * - ``uint16``
      - 16-bit unsigned integer, most significant byte first

    * - ``uint32``
      - 32-bit unsigned integer, most significant byte first

    * - ``uint64``
      - 64-bit unsigned integer, most significant byte first

    * - ``int8<T>`` or ``uint8<T>``
      - an 8-bit signed or unsigned integer enumeration,
        where *T* denotes the name of the enumeration

    * - ``string``
      - a UTF-8 encoded text string prefixed with its byte length as ``uint32``

    * - ``bytes``
      - a byte string prefixed with its length as ``uint32``

    * - ``KeyValue``
      - .. eql:struct:: edb.protocol.KeyValue

    * - ``Annotation``
      - .. eql:struct:: edb.protocol.Annotation

    * - ``uuid``
      - an array of 16 bytes with no length prefix, equivalent to
        ``byte[16]``


.. _ref_message_format:

Message Format
==============

All messages in the EdgeDB wire protocol have the following format:

.. code-block:: c

    struct {
        uint8    message_type;
        int32    payload_length;
        uint8    payload[payload_length - 4];
    };

The server and the client *MUST* not fragment messages. I.e the complete
message must be sent before starting a new message. It's advised that whole
message should be buffered before initiating a network call (but this
requirement is neither observable nor enforceable at the other side). It's
also common to buffer the whole message on the receiver side before starting
to process it.

Errors
======

At any point the server may send an :ref:`ref_protocol_msg_error` indicating
an error condition.  This is implied in the message flow documentation, and
only successful paths are explicitly documented.  The handling of the
``ErrorResponse`` message depends on the connection phase, as well as the
severity of the error.

If the server is not able to recover from an error, the connection is closed
immediately after an ``ErrorResponse`` message is sent.


Logs
====

Similarly to ``ErrorResponse`` the server may send a
:ref:`ref_protocol_msg_log` message.  The client should handle the
message and continue as before.

.. _ref_message_flow:

Message Flow
============

There are two main phases in the lifetime of an EdgeDB connection: the
connection phase, and the command phase.  The connection phase is responsible
for negotiating the protocol and connection parameters, including
authentication.  The command phase is the regular operation phase where the
server is processing queries sent by the client.


Connection Phase
----------------

To begin a session, a client opens a connection to the server, and sends
the :ref:`ref_protocol_msg_client_handshake`.  The server responds in one
of three ways:

1. One of the authentication messages (see :ref:`below <ref_authentication>`);
2. :ref:`ref_protocol_msg_server_handshake` followed by one of the
   authentication messages;
3. :ref:`ref_protocol_msg_error` which indicates an invalid client handshake
   message.

:ref:`ref_protocol_msg_server_handshake` is only sent if the requested
connection parameters cannot be fully satisfied; the server responds to
offer the protocol parameters it is willing to support. Client may proceed
by noting lower protocol version and/or absent extensions. Client *MUST* close
the connection if protocol version is unsupported. Server *MUST* send subset
of the extensions received in :ref:`ref_protocol_msg_client_handshake` (i.e.
it never adds extra ones).

While it's not required by the protocol specification itself, EdgeDB server
currently requires setting the following params in
:ref:`ref_protocol_msg_client_handshake`:

.. versionchanged:: _default

    * ``user`` -- username for authentication
    * ``database`` -- database to connect to

.. versionchanged:: 5.0

    * ``user`` -- username for authentication
    * ``branch`` -- branch to connect to


.. _ref_authentication:

Authentication
--------------


The server then initiates the authentication cycle by sending an authentication
request message, to which the client must respond with an appropriate
authentication response message.

The following messages are sent by the server in the authentication cycle:

:ref:`ref_protocol_msg_auth_ok`
    Authentication is successful.

:ref:`ref_protocol_msg_auth_sasl`
    The client must now initiate a SASL negotiation, using one of the
    SASL mechanisms listed in the message.  The client will send an
    :ref:`ref_protocol_msg_auth_sasl_initial_response` with the name of the
    selected mechanism, and the first part of the SASL data stream in
    response to this.  If further messages are needed, the server will
    respond with :ref:`ref_protocol_msg_auth_sasl_continue`.

:ref:`ref_protocol_msg_auth_sasl_continue`
    This message contains challenge data from the previous step of SASL
    negotiation (:ref:`ref_protocol_msg_auth_sasl`, or a previous
    :ref:`ref_protocol_msg_auth_sasl_continue`).  The client must respond
    with an :ref:`ref_protocol_msg_auth_sasl_response` message.

:ref:`ref_protocol_msg_auth_sasl_final`
    SASL authentication has completed with additional mechanism-specific
    data for the client.  The server will next send
    :ref:`ref_protocol_msg_auth_ok` to indicate successful authentication,
    or an :ref:`ref_protocol_msg_error` to indicate failure. This message is
    sent only if the SASL mechanism specifies additional data to be sent
    from server to client at completion.

If the frontend does not support the authentication method requested by the
server, then it should immediately close the connection.

Once the server has confirmed successful authentication with
:ref:`ref_protocol_msg_auth_ok`, it then sends one or more of the following
messages:

:ref:`ref_protocol_msg_server_key_data`
    This message provides per-connection secret-key data that the client
    must save if it wants to be able to issue certain requests later.  The
    client should not respond to this message.

:ref:`ref_protocol_msg_server_parameter_status`
    This message informs the frontend about the setting of certain server
    parameters.  The client can ignore this message, or record the settings
    for its future use.  The client should not respond to this message.

The connection phase ends when the server sends the first
:ref:`ref_protocol_msg_ready_for_command` message, indicating the start of
a command cycle.


Command Phase
-------------

In the command phase, the server expects the client to send one of the
following messages:

:ref:`ref_protocol_msg_parse`
    Instructs the server to parse the provided command or commands for
    execution.  The server responds with a
    :ref:`ref_protocol_msg_command_data_description` containing the
    :ref:`type descriptor <ref_proto_typedesc>` data necessary to perform
    data I/O for this command.

:ref:`ref_protocol_msg_execute`
    Execute the provided command or commands.  This message expects the
    client to declare a correct :ref:`type descriptor <ref_proto_typedesc>`
    identifier for command arguments.  If the declared input type descriptor
    does not match the expected value, a
    :ref:`ref_protocol_msg_command_data_description` message is returned
    followed by a ``ParameterTypeMismatchError`` in an ``ErrorResponse``
    message.

    If the declared output type descriptor does not match, the server
    will send a :ref:`ref_protocol_msg_command_data_description` prior to
    sending any :ref:`ref_protocol_msg_data` messages.

The client could attach state data in both messages. When doing so, the client
must also set a correct :ref:`type descriptor <ref_proto_typedesc>` identifier
for the state data.  If the declared state type descriptor does not match the
expected value, a :ref:`ref_protocol_msg_state_data_description` message is
returned followed by a ``StateMismatchError`` in an ``ErrorResponse`` message.
However, the special type id of zero ``00000000-0000-0000-0000-000000000000``
for empty/default state is always a match.

Each of the messages could contain one or more EdgeQL commands separated
by a semicolon (``;``).  If more than one EdgeQL command is found in a single
message, the server will treat the commands as an EdgeQL script. EdgeQL scripts
are always atomic, they will be executed in an implicit transaction block if no
explicit transaction is currently active. Therefore, EdgeQL scripts have
limitations on the kinds of EdgeQL commands they can contain:

.. versionchanged:: _default

    * Transaction control commands are not allowed, like ``start transaction``,
      ``commit``, ``declare savepoint``, or ``rollback to savepoint``.
    * Non-transactional commands, like ``create database`` or
      ``configure instance`` are not allowed.

.. versionchanged:: 5.0

    * Transaction control commands are not allowed, like ``start transaction``,
      ``commit``, ``declare savepoint``, or ``rollback to savepoint``.
    * Non-transactional commands, like ``create branch`` or
      ``configure instance`` are not allowed.

In the command phase, the server can be in one of the three main states:

* *idle*: server is waiting for a command;
* *busy*: server is executing a command;
* *error*: server encountered an error and is discarding incoming messages.

Whenever a server switches to the *idle* state, it sends a
:ref:`ref_protocol_msg_ready_for_command` message.

Whenever a server encounters an error, it sends an
:ref:`ref_protocol_msg_error` message and switches into
the *error* state.

To switch a server from the *error* state into the *idle* state, a
:ref:`ref_protocol_msg_sync` message must be sent by the client.


.. _ref_protocol_dump_flow:

Dump Flow
---------

Backup flow goes as following:

1. Client sends :ref:`ref_protocol_msg_dump` message
2. Server sends :ref:`ref_protocol_msg_dump_header` message
3. Server sends one or more :ref:`ref_protocol_msg_dump_block` messages
4. Server sends :ref:`ref_protocol_msg_command_complete` message

Usually client should send :ref:`ref_protocol_msg_sync` after ``Dump`` message
to finish implicit transaction.


.. _ref_protocol_restore_flow:

Restore Flow
------------

Restore procedure fills up the :versionreplace:`database;5.0:branch` the
client is connected to with the schema and data from the dump file.

Flow is the following:

1. Client sends :ref:`ref_protocol_msg_restore` message with the dump header
   block
2. Server sends :ref:`ref_protocol_msg_restore_ready` message as a confirmation
   that it has accepted the header, restored schema and ready to receive data
   blocks
3. Clients sends one or more :ref:`ref_protocol_msg_restore_block` messages
4. Client sends :ref:`ref_protocol_msg_restore_eof` message
5. Server sends :ref:`ref_protocol_msg_command_complete` message

Note: :ref:`ref_protocol_msg_error` may be sent from the server at
any time. In case of error, :ref:`ref_protocol_msg_sync` must be sent and all
subsequent messages ignored until :ref:`ref_protocol_msg_ready_for_command` is
received.

Restore protocol doesn't require a :ref:`ref_protocol_msg_sync` message except
for error cases.


Termination
===========

The normal termination procedure is that the client sends a
:ref:`ref_protocol_msg_terminate` message and immediately closes the
connection.  On receipt of this message, the server cleans up the
connection resources and closes the connection.

In some cases the server might disconnect without a client request to do so.
In such cases the server will attempt to send an :ref:`ref_protocol_msg_error`
or a :ref:`ref_protocol_msg_log` message to indicate the reason for the
disconnection.
