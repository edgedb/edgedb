.. _ref_protocol_overview:

========
Overview
========

EdgeDB uses a message-based binary protocol for communication between
clients and servers.  The protocol is supported over TCP/IP and also over
Unix-domain sockets.


Conventions and Data Types
==========================

The message format descriptions in this section use a C-like struct definitions
to describe their layout.  The structs are *packed*, i.e. there are never
any alignment gaps.

The following data types are used in the descriptions:

.. list-table::
    :class: funcoptable

    * - ``int8``
      - 8-bit integer
    * - ``int8<T>``
      - an 8-bit integer enumeration, where *T* denotes the name of
        the enumeration
    * - ``int16``
      - 16-bit integer, most significant byte first
    * - ``int32``
      - 32-bit integer, most significant byte first
    * - ``int64``
      - 64-bit integer, most significant byte first
    * - ``byte``
      - 8-bit unsigned integer
    * - ``byte<T>``
      - 8-bit unsigned integer enumeration, where *T* denotes the name of
        the enumeration
    * - ``string``
      - a UTF-8 encoded text string prefixed with its byte length as ``int32``
    * - ``bytes``
      - a byte string prefixed with its length as ``int32``
    * - ``Headers``
      - a key-value structure with the following layout:

        .. code-block:: c

           struct Headers {
               int16  num_headers;
               Header headers[num_headers];
           };

           struct Header {
               int16  key;
               bytes  value;
           };
    * - ``uuid``
      - a simple array of 16 bytes with no length prefix, equivalent to
        ``byte[16]``



Message Format
==============

All messages in the EdgeDB wire protocol have the following format:

.. code-block:: c

    struct {
        int8  message_type;
        int32 payload_length;
        int8  payload[payload_length - 4];
    };

The server and the client *MUST* not fragment messages. I.e the complete
message must be sent before starting a new message. It's advised that whole
message should be buffered before initiating a network call (but this
requirement is neither observable nor enforceable at the other side). It's
also common to buffer the whole message on receiver before starting to process
it.

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


Flushing
========

The server by default accumulates its output messages in a buffer to minimize
the number of network calls.  Some messages, such as
:ref:`ref_protocol_msg_sync` and :ref:`ref_protocol_msg_ready_for_command`
flush the server buffer automatically.  In other cases, a client can send
a :ref:`ref_protocol_msg_flush` message to ask the server to flush its
send buffer.


Message Flow
============

There are two main phases in the lifetime of an EdgeDB connection: the
connection phase, and the command phase.  The connection phase is responsible
for negotiating the protocol and connection parameters, including
authentication.  The command phase is the regular operation phase where the
server is processing queries sent by the client.  In the command phase
there are two possible command flows: the script flow and the granular flow.


Connection Phase
----------------

To begin a session, a client opens a connection to the server, and sends
the :ref:`ref_protocol_msg_client_handshake`.  Server responds in one of
the three ways:

1. One of the authentication messages (see :ref:`below <ref_authentication>`);
2. :ref:`ref_protocol_msg_server_handshake` followed by one of the
   authentication messages;
3. :ref:`ref_protocol_msg_error` which indicates invalid client handshake
   message.

:ref:`ref_protocol_msg_server_handshake` is only sent if the requested
connection parameters cannot be fully satisfied, the server responds to
offer the protocol parameters it is willing to support. Client may proceed
by noting lower protocol version and/or absent extensions. Client *MUST* close
the connection if protocol version is unsupported. Server *MUST* send subset
of the extensions received in :ref:`ref_protocol_msg_client_handshake` (i.e.
it never adds extra ones).

While it's not required by the protocol specification itself, current EdgeDB
server requires setting the following params in
:ref:`ref_protocol_msg_client_handshake`:

* ``user`` -- username for authentication
* ``database`` -- database to connect to


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
    SASL mechanisms listed in the message.  The client will send a
    :ref:`ref_protocol_msg_auth_sasl_initial_response` with the name of the
    selected mechanism, and the first part of the SASL data stream in
    response to this.  If further messages are needed, the server will
    respond with :ref:`ref_protocol_msg_auth_sasl_continue`.

:ref:`ref_protocol_msg_auth_sasl_continue`
    This message contains challenge data from the previous step of SASL
    negotiation (:ref:`ref_protocol_msg_auth_sasl`, or a previous
    :ref:`ref_protocol_msg_auth_sasl_continue`).  The client must respond
    with a :ref:`ref_protocol_msg_auth_sasl_response` message.

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

In the command phase, the server can be in one of the three main states:

* *idle*: server is waiting for a command;
* *busy*: server is executing a command;
* *error*: server encountered an error and is discarding incoming messages.

Whenever a server switches to the *idle* state, it sends a
:ref:`ref_protocol_msg_ready_for_command` message.

Whenever a server encounters an error, it sends a :ref:`ref_protocol_msg_error`
message.  If an error occurred during a *granular command flow*, the server
switches into the *error* state, otherwise it switches into *idle* directly.

To switch a server from the *error* state into the *idle* state, a
:ref:`ref_protocol_msg_sync` message must be sent by the client.


Script Flow
-----------

In a script command flow the client follows the server's
:ref:`ref_protocol_msg_ready_for_command` message with a
:ref:`ref_protocol_msg_execute_script` message.  The message includes one
or more EdgeQL commands as a text string.  The server then sends
a :ref:`ref_protocol_msg_command_complete` message if the command (or commands)
completed successfully, or :ref:`ref_protocol_msg_error` in case of an error.
The ``CommandComplete`` corresponds to the *last* command in the script.

.. note::

    The script flow is not designed to return any data beyond
    that included in the ``CommandComplete`` message.

.. note::

    EdgeQL scripts are executed in an implicit transaction block, *except*
    when a script contains a single command that cannot be executed inside
    a transaction.


Granular Flow
-------------

The *granular flow* is designed to execute EdgeQL commands one-by-one
with a series of messages.  This flow should be used whenever data
needs to be returned from a command, or arguments passed to a command.

In this mode the server expects the client to send one of the following
messages:

:ref:`ref_protocol_msg_prepare`
    Instructs the server to process and prepare the provided command for
    execution.  The server responds with a
    :ref:`ref_protocol_msg_prepare_complete` message containing the
    unique identifier of the statement
    :ref:`type descriptor <ref_proto_typedesc>`.  The client may then
    send a :ref:`ref_protocol_msg_describe_statement` if it requires the
    type descriptor data.

:ref:`ref_protocol_msg_describe_statement`
    Asks the server to return the type descriptor data for a prepared
    statement.  This message is only valid following the receipt of
    :ref:`ref_protocol_msg_prepare_complete`.  The server responds with
    a :ref:`ref_protocol_msg_command_data_description` message.

:ref:`ref_protocol_msg_execute`
    Execute a previously prepared command.  The server responds with
    zero or more :ref:`ref_protocol_msg_data` messages, followed by
    a :ref:`ref_protocol_msg_command_complete`.

:ref:`ref_protocol_msg_optimistic_execute`
    Execute the provided command text directly, assuming prior knowledge
    of the :ref:`type descriptor <ref_proto_typedesc>` data.  This allows
    the client to perform the prepare/execute operation in a single step.
    The server responds with zero or more :ref:`ref_protocol_msg_data`
    messages, followed by a :ref:`ref_protocol_msg_command_complete`.


Implicit Transactions
---------------------

All EdgeDB commands (with the exception of a few DDL commands) execute in
a transaction block.  An *explicit* transaction block is started by a
:eql:stmt:`START TRANSACTION` command.  If not within an explicit transaction,
an *implicit* transaction block is started when the first message is received
by the server.  If a ``START TRANSACTION`` command is executed in an implicit
transaction block, that block becomes explicit.  An implicit transaction block
ends if:

* a :eql:stmt:`COMMIT` command is executed,
* a :eql:stmt:`ROLLBACK` command is executed,
* a :ref:`ref_protocol_msg_sync` message is received.


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
