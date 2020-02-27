========
Messages
========


.. list-table::
    :class: funcoptable

    * - **Server Messages**
      -

    * - :ref:`ref_protocol_msg_auth_ok`
      - Authentication is successful.

    * - :ref:`ref_protocol_msg_auth_sasl`
      - SASL authentication is required.

    * - :ref:`ref_protocol_msg_auth_sasl_continue`
      - SASL authentication challenge.

    * - :ref:`ref_protocol_msg_auth_sasl_final`
      - SASL authentication final message.

    * - :ref:`ref_protocol_msg_command_complete`
      - Successful completion of a command.

    * - :ref:`ref_protocol_msg_command_data_description`
      - Description of command data input and output.

    * - :ref:`ref_protocol_msg_data`
      - Command result data element.

    * - :ref:`ref_protocol_msg_error`
      - Server error.

    * - :ref:`ref_protocol_msg_log`
      - Server log message.

    * - :ref:`ref_protocol_msg_server_parameter_status`
      - Server parameter value.

    * - :ref:`ref_protocol_msg_prepare_complete`
      - Statement preparation complete.

    * - :ref:`ref_protocol_msg_ready_for_command`
      - Server is ready for a command.

    * - :ref:`ref_protocol_msg_server_handshake`
      - Initial server connection handshake.

    * - :ref:`ref_protocol_msg_server_key_data`
      - Opaque token identifying the server connection.

    * - **Client Messages**
      -

    * - :ref:`ref_protocol_msg_auth_sasl_initial_response`
      - SASL authentication initial response.

    * - :ref:`ref_protocol_msg_auth_sasl_response`
      - SASL authentication response.

    * - :ref:`ref_protocol_msg_client_handshake`
      - Initial client connection handshake.

    * - :ref:`ref_protocol_msg_describe_statement`
      - Describe a previously prepared statement.

    * - :ref:`ref_protocol_msg_execute`
      - Execute a prepared statement.

    * - :ref:`ref_protocol_msg_execute_script`
      - Execute an EdgeQL script.

    * - :ref:`ref_protocol_msg_flush`
      - Force the server to flush its output buffers.

    * - :ref:`ref_protocol_msg_prepare`
      - Prepare an EdgeQL statement.

    * - :ref:`ref_protocol_msg_optimistic_execute`
      - Optimistically prepare and execute a query.

    * - :ref:`ref_protocol_msg_sync`
      - Provide an explicit synchronization point.

    * - :ref:`ref_protocol_msg_terminate`
      - Terminate the connection.


.. _ref_protocol_msg_error:

ErrorResponse
=============

Sent by: server.

Format:

.. code-block:: c

    struct ErrorResponse {
        // Message type ('E')
        int8                mtype = 0x45;

        // Length of message contents in bytes,
        // including self.
        int32               message_length;

        // Error severity.
        int8<ErrorSeverity> severity;

        // Error code.
        int32               code;

        // Error message
        string              message;

        // Other error attributes.
        Headers             attributes;
    };

    enum ErrorSeverity {
        ERROR = 120,
        FATAL = 200,
        PANIC = 255
    };

See the :ref:`list of error codes <ref_protocol_error_codes>` for all possible
error codes.


.. _ref_protocol_msg_log:

LogMessage
==========

Sent by: server.

Format:

.. code-block:: c

    struct LogMessage {
        // Message type ('L')
        int8                  mtype = 0x4c;

        // Length of message contents in bytes,
        // including self.
        int32                 message_length;

        // Message severity.
        int8<MessageSeverity> severity;

        // Message code.
        int32                 code;

        // Message text.
        string                text;

        // Other error attributes.
        Headers               attributes;
    };

    enum MessageSeverity {
        DEBUG = 20,
        INFO = 40,
        NOTICE = 60,
        WARNING = 80
    };

See the :ref:`list of error codes <ref_protocol_error_codes>` for all possible
log message codes.


.. _ref_protocol_msg_ready_for_command:

ReadyForCommand
===============

Sent by: server.

Format:

.. code-block:: c

    struct ReadyForCommand {
        // Message type ('Z')
        int8                   mtype = 0x5a;

        // Length of message contents in bytes,
        // including self.
        int32                  message_length;

        // A set of message headers.
        Headers                headers;

        // Transaction state
        int8<TransactionState> transaction_state;
    };

    enum TransactionState {
        // Not in a transaction block.
        NOT_IN_TRANSACTION = 0x49,

        // In a transaction block.
        IN_TRANSACTION = 0x54,

        // In a failed transaction block
        // (commands will be rejected until the block is ended).
        IN_FAILED_TRANSACTION = 0x45
    };


.. _ref_protocol_msg_command_complete:

CommandComplete
===============

Sent by: server.

Format:

.. code-block:: c

    struct CommandComplete {
        // Message type ('C')
        int8    mtype = 0x43;

        // Length of message contents in bytes,
        // including self.
        int32   message_length;

        // A set of message headers.
        Headers     headers;

        // Command status.
        bytes   status_data;
    };


.. _ref_protocol_msg_execute_script:

ExecuteScript
=============

Sent by: client.

Format:

.. code-block:: c

    struct ExecuteScript {
        // Message type ('Q')
        int8    mtype = 0x51;

        // Length of message contents in bytes,
        // including self.
        int32   message_length;

        // A set of message headers.
        Headers headers;

        // Script text.
        string  script_text;
    };


.. _ref_protocol_msg_prepare:

Prepare
=======

Sent by: client.

Format:

.. code-block:: c

    struct Prepare {
        // Message type ('P')
        int8              mtype = 0x50;

        // Length of message contents in bytes,
        // including self.
        int32             message_length;

        // A set of message headers.
        Headers           headers;

        // Data I/O format.
        int8<IOFormat>    io_format;

        // Expected result cardinality
        int8<Cardinality> expected_cardinality;

        // Prepared statement name.
        // Currently must be empty.
        bytes             statement_name;

        // Command text.
        string            command_text;
    };

    enum IOFormat {
        // Default format that should be used in most cases
        BINARY = 0x62,

        // Returns a single row and single field that contains
        // a resultset as a single JSON array
        JSON = 0x6a,

        // Returns a single JSON string per top-level set element.
        // Preferred over JSON format because might be used for
        // larger responses
        JSON_ELEMENTS = 0x4a,
    };

    enum Cardinality {

        // Zero cardinality is used in statements which don't return
        // any result, such as CREATE DATABASE
        ZERO = 0x6e,

        ONE = 0x6f,
        MANY = 0x6d
    };


.. _ref_protocol_msg_describe_statement:

DescribeStatement
=================

Sent by: client.

Format:

.. code-block:: c

    struct DescribeStatement {
        // Message type ('D')
        int8                 mtype = 0x44;

        // Length of message contents in bytes,
        // including self.
        int32                message_length;

        // A set of message headers.
        Headers              headers;

        // Aspect to describe.
        int8<DescribeAspect> aspect;

        // The name of the statement.
        bytes                statement_name;
    };

    enum DescribeAspect {
        DATA_DESCRIPTION = 0x54
    };



.. _ref_protocol_msg_command_data_description:

CommandDataDescription
======================

Sent by: server.

Format:

.. code-block:: c

    struct CommandDataDescription {
        // Message type ('T')
        int8              mtype = 0x54;

        // Length of message contents in bytes,
        // including self.
        int32             message_length;

        // A set of message headers.
        Headers           headers;

        // Actual result cardinality
        int8<Cardinality> result_cardinality;

        // Argument data descriptor ID.
        uuid              input_typedesc_id;

        // Argument data descriptor.
        bytes             input_typedesc;

        // Output data descriptor ID.
        uuid              output_typedesc_id;

        // Output data descriptor.
        bytes             output_typedesc;
    };

    enum Cardinality {

        // A cardinality used in statements which don't return
        // any result, such as CREATE DATABASE
        NO_RESULT = 0x6e,

        ONE = 0x6f,
        MANY = 0x6d
    };


The format of the *input_typedesc* and *output_typedesc* fields is described
in the :ref:`ref_proto_typedesc` section.


.. _ref_protocol_msg_sync:

Sync
====

Sent by: client.

Format:

.. code-block:: c

    struct Sync {
        // Message type ('S')
        int8          mtype = 0x53;

        // Length of message contents in bytes,
        // including self.
        int32         message_length;
    };


.. _ref_protocol_msg_flush:

Flush
=====

Sent by: client.

Format:

.. code-block:: c

    struct Flush {
        // Message type ('H')
        int8          mtype = 0x48;

        // Length of message contents in bytes,
        // including self.
        int32         message_length;
    };


.. _ref_protocol_msg_execute:

Execute
=======

Sent by: client.

Format:

.. code-block:: c

    struct Execute {
        // Message type ('E')
        int8            mtype = 0x45;

        // Length of message contents in bytes,
        // including self.
        int32           message_length;

        // A set of message headers.
        Headers         headers;

        // Prepared statement name.
        bytes           statement_name;

        // Encoded argument data.
        bytes           arguments;
    };


.. _ref_protocol_msg_optimistic_execute:

Optimistic Execute
==================

Sent by: client.

Format:

.. code-block:: c

    struct OptimisticExecute {
        // Message type ('O')
        int8                mtype = 0x4f;

        // Length of message contents in bytes,
        // including self.
        int32               message_length;

        // A set of message headers.
        Headers             headers;

        // Data I/O format.
        int8<IOFormat>      io_format;

        // Expected result cardinality
        byte<Cardinality>   expected_cardinality;

        // Command text.
        string              command_text;

        // Argument data descriptor ID.
        uuid                input_typedesc_id;

        // Output data descriptor ID.
        uuid                output_typedesc_id;

        // Encoded argument data.
        bytes               arguments;
    };

The data in *arguments* must be encoded as a
:ref:`tuple value <ref_protocol_fmt_tuple>` described by
a type descriptor identified by *input_typedesc_id*.


.. _ref_protocol_msg_data:

Data
====

Sent by: server.

Format:

.. code-block:: c

    struct Data {
        // Message type ('D')
        int8            mtype = 0x44;

        // Length of message contents in bytes,
        // including self.
        int32           message_length;

        // Number of data blocks that follow,
        // currently always 1.
        int16           num_data;

        // Encoded output data.
        bytes           data[num_data];
    };

The type of *data* is determined by the query output type descriptor.  Wire
formats for the standard scalar types and collections are documented in
:ref:`ref_proto_dataformats`.


.. _ref_protocol_msg_server_key_data:

ServerKeyData
=============

Sent by: server.

Format:

.. code-block:: c

    struct ServerKeyData {
        // Message type ('K')
        int8            mtype = 0x4b;

        // Length of message contents in bytes,
        // including self.
        int32           message_length;

        // Key data.
        byte            data[32];
    };


.. _ref_protocol_msg_server_parameter_status:

ParameterStatus
===============

Sent by: server.

Format:

.. code-block:: c

    struct ParameterStatus {
        // Message type ('S')
        int8            mtype = 0x53;

        // Length of message contents in bytes,
        // including self.
        int32           message_length;

        // Parameter name.
        bytes           name;

        // Parameter value.
        bytes           value;
    };


.. _ref_protocol_msg_prepare_complete:

PrepareComplete
===============

Sent by: server.

Format:

.. code-block:: c

    struct PrepareComplete {
        // Message type ('1')
        int8                mtype = 0x31;

        // Length of message contents in bytes,
        // including self.
        int32               message_length;

        // A set of message headers.
        Headers             headers;

        // Result cardinality
        int8<Cardinality>   cardinality;

        // Argument data descriptor ID.
        uuid                input_typedesc_id;

        // Output data descriptor ID.
        uuid                output_typedesc_id;
    };

    enum Cardinality {

        // Zero cardinality is used in statements which don't return
        // any result, such as CREATE DATABASE
        ZERO = 0x6e,

        ONE = 0x6f,
        MANY = 0x6d
    };


.. _ref_protocol_msg_client_handshake:

ClientHandshake
===============

Sent by: client.

Format:

.. code-block:: c

    struct ClientHandshake {
        // Message type ('V')
        int8        mtype = 0x56;

        // Length of message contents in bytes,
        // including self.
        int32       message_length;

        // Requested protocol major version.
        int16       major_ver;

        // Requested protocol minor version.
        int16       minor_ver;

        // Number of connection parameters.
        int16       num_params;

        // Connection parameters.
        Param       params[num_params];

        // Number of protocol extensions.
        int16       num_exts;

        // Requested protocol extensions.
        ProtocolExt exts[num_exts];
    };

    struct Param {
        string parameter_name;
        string parameter_value;
    };

    struct ProtocolExt {
        // Extension name.
        string  extname;
        // Extension headers.
        Headers extheaders;
    };

The ``ClientHandshake`` message is the first message sent by the client.
upon connecting to the server.  It is the first phase of protocol negotiation,
where the client sends the requested protocol version and extensions.
Currently, the only defined ``major_ver`` is ``1``, and ``minor_ver`` is ``0``.
No protocol extensions are currently defined.  The server always responds
with the :ref:`ref_protocol_msg_server_handshake`.


.. _ref_protocol_msg_server_handshake:

ServerHandshake
===============

Sent by: server.

Format:

.. code-block:: c

    struct ServerHandshakeMessage {
        // Message type ('v')
        int8        mtype = 0x76;

        // Length of message contents in bytes,
        // including self.
        int32       message_length;

        // maximum supported or client-requested
        // protocol major version, whichever is greater.
        int16       major_ver;

        // maximum supported or client-requests
        // protocol minor version, whichever
        // is greater.
        int16       minor_ver;

        // number of supported protocol extensions
        int16       num_exts;

        // supported protocol extensions
        ProtocolExt exts[num_exts];
    };

    struct ProtocolExt {
        // extension name
        string  extname;

        // extension headers
        Headers extheaders;
    };

The ``ServerHandshake`` message is a direct response to the
:ref:`ref_protocol_msg_client_handshake` message and is sent by the server.
in the case where the server does not support the protocol version or
protocol extensions requested by the client.  It contains the maximum
protocol version supported by the server, considering the version requested
by the client.  It also contains the intersection of the client-requested and
server-supported protocol extensions.  Any requested extensions not listed
in the ``Server Handshake`` message are considered unsupported.


.. _ref_protocol_msg_auth_ok:

AuthenticationOK
================

Sent by: server.

Format:

.. code-block:: c

    struct AuthenticationOK {
        // Message type ('R')
        int8      mtype = 0x52;

        // Length of message contents in bytes,
        // including self.
        int32     message_length = 0x8;

        // Specifies that this message contains
        // a successful authentication indicator.
        int32     auth_status = 0x0;
    };

The ``AuthenticationOK`` message is sent by the server once it considers
the authentication to be successful.


.. _ref_protocol_msg_auth_sasl:

AuthenticationSASL
==================

Sent by: server.

Format:

.. code-block:: c

    struct AuthenticationRequiredSASLMessage {
        // Message type ('R')
        int8      mtype = 0x52;

        // Length of message contents in bytes,
        // including self.
        int32     message_length;

        // Specifies that this message contains
        // a SASL authentication request.
        int32     auth_status = 0x0A;

        // The number of supported SASL authentication
        // methods.
        int32     method_count;

        // A list of supported SASL authentication
        // methods.
        string    methods[method_count];
    };

The ``AuthenticationSASL`` message is sent by the server if
it determines that a SASL-based authentication method is required in
order to connect using the connection parameters specified in the
:ref:`ref_protocol_msg_client_handshake`.  The message contains a list
of *authentication methods* supported by the server in the order preferred
by the server.

.. note::
    At the moment, the only SASL authentication method supported
    by EdgeDB is ``SCRAM-SHA-256``
    (`RFC 7677 <https://tools.ietf.org/html/rfc7677>`_).

The client must select an appropriate authentication method from the list
returned by the server and send a
:ref:`ref_protocol_msg_auth_sasl_initial_response`.
One or more server-challenge and client-response message follow.  Each
server-challenge is sent in an :ref:`ref_protocol_msg_auth_sasl_continue`,
followed by a response from client in a
:ref:`ref_protocol_msg_auth_sasl_response` message.  The particulars of the
messages are mechanism specific.  Finally, when the authentication
exchange is completed successfully, the server sends an
:ref:`ref_protocol_msg_auth_sasl_final`, followed immediately
by an :ref:`ref_protocol_msg_auth_ok`.


.. _ref_protocol_msg_auth_sasl_continue:

AuthenticationSASLContinue
==========================

Sent by: server.

Format:

.. code-block:: c

    struct AuthenticationSASLContinue {
        // Message type ('R')
        int8      mtype = 0x52;

        // Length of message contents in bytes,
        // including self.
        int32     message_length;

        // Specifies that this message contains
        // a SASL challenge.
        int32     auth_status = 0x0B;

        // Mechanism-specific SASL data.
        bytes     sasl_data;
    };


.. _ref_protocol_msg_auth_sasl_final:

AuthenticationSASLFinal
=======================

Sent by: server.

Format:

.. code-block:: c

    struct AuthenticationSASLFinal {
        // Message type ('R')
        int8      mtype = 0x52;

        // Length of message contents in bytes,
        // including self.
        int32     message_length;

        // Specifies that SASL authentication
        // has completed.
        int32     auth_status = 0x0C;

        // Mechanism-specific SASL data.
        bytes     sasl_data;
    };


.. _ref_protocol_msg_auth_sasl_initial_response:

AuthenticationSASLInitialResponse
=================================

Sent by: client.

Format:

.. code-block:: c

    struct AuthenticationSASLInitialResponse {
        // Message type ('p')
        int8      mtype = 0x70;

        // Length of message contents in bytes,
        // including self.
        int32     message_length;

        // Name of the SASL authentication mechanism
        // that the client selected.
        string    method;

        // Mechanism-specific "Initial Response" data.
        bytes     sasl_data;
    };


.. _ref_protocol_msg_auth_sasl_response:

AuthenticationSASLResponse
==========================

Sent by: client.

Format:

.. code-block:: c

    struct AuthenticationSASLResponse {
        // Message type ('r')
        int8      mtype = 0x72;

        // Length of message contents in bytes,
        // including self.
        int32     message_length;

        // Mechanism-specific response data.
        bytes     sasl_data;
    };


.. _ref_protocol_msg_terminate:

Terminate
=========

Sent by: client.

Format:

.. code-block:: c

    struct Terminate {
        // Message type ('X')
        int8      mtype = 0x58;

        // Length of message contents in bytes,
        // including self.
        int32     message_length;
    };
