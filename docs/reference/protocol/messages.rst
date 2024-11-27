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

    * - :ref:`ref_protocol_msg_state_data_description`
      - Description of state data.

    * - :ref:`ref_protocol_msg_data`
      - Command result data element.

    * - :ref:`ref_protocol_msg_dump_header`
      - Initial message of the database backup protocol

    * - :ref:`ref_protocol_msg_dump_block`
      - Single chunk of database backup data

    * - :ref:`ref_protocol_msg_error`
      - Server error.

    * - :ref:`ref_protocol_msg_log`
      - Server log message.

    * - :ref:`ref_protocol_msg_server_parameter_status`
      - Server parameter value.

    * - :ref:`ref_protocol_msg_ready_for_command`
      - Server is ready for a command.

    * - :ref:`ref_protocol_msg_restore_ready`
      - Successful response to the :ref:`ref_protocol_msg_restore` message

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

    * - :ref:`ref_protocol_msg_dump`
      - Initiate database backup

    * - :ref:`ref_protocol_msg_parse`
      - Parse EdgeQL command(s).

    * - :ref:`ref_protocol_msg_execute`
      - Parse and/or execute a query.

    * - :ref:`ref_protocol_msg_restore`
      - Initiate database restore

    * - :ref:`ref_protocol_msg_restore_block`
      - Next block of database dump

    * - :ref:`ref_protocol_msg_restore_eof`
      - End of database dump

    * - :ref:`ref_protocol_msg_sync`
      - Provide an explicit synchronization point.

    * - :ref:`ref_protocol_msg_terminate`
      - Terminate the connection.


.. _ref_protocol_msg_error:

ErrorResponse
=============

Sent by: server.

Format:

.. eql:struct:: edb.protocol.ErrorResponse

.. eql:struct:: edb.protocol.ErrorSeverity


See the :ref:`list of error codes <ref_protocol_error_codes>` for all possible
error codes.

Known attributes:

* 0x0001 ``HINT``: ``str`` -- error hint.
* 0x0002 ``DETAILS``: ``str`` -- error details.
* 0x0101 ``SERVER_TRACEBACK``: ``str`` -- error traceback from server
  (is only sent in dev mode).
* 0xFFF1 ``POSITION_START`` -- byte offset of the start of the error span.
* 0xFFF2 ``POSITION_END`` -- byte offset of the end of the error span.
* 0xFFF3 ``LINE_START`` -- one-based line number of the start of the
  error span.
* 0xFFF4 ``COLUMN_START`` -- one-based column number of the start of the
  error span.
* 0xFFF5 ``UTF16_COLUMN_START`` -- zero-based column number in UTF-16
  encoding of the start of the error span.
* 0xFFF6 ``LINE_END`` -- one-based line number of the start of the
  error span.
* 0xFFF7 ``COLUMN_END`` -- one-based column number of the start of the
  error span.
* 0xFFF8 ``UTF16_COLUMN_END`` -- zero-based column number in UTF-16
  encoding of the end of the error span.
* 0xFFF9 ``CHARACTER_START`` -- zero-based offset of the error span in
  terms of Unicode code points.
* 0xFFFA ``CHARACTER_END`` -- zero-based offset of the end of the error
  span.

Notes:

1. Error span is the range of characters (or equivalent bytes) of the
   original query that compiler points to as the source of the error.
2. ``COLUMN_*`` is defined in terms of width of characters defined by
   Unicode Standard Annex #11, in other words, the column number in the
   text if rendered with monospace font, e.g. in a terminal.
3. ``UTF16_COLUMN_*`` is defined as number of UTF-16 code units (i.e. two
   byte-pairs) that precede target character on the same line.
4. ``*_END`` points to a next character after the last character of the
   error span.


.. _ref_protocol_msg_log:

LogMessage
==========

Sent by: server.

Format:

.. eql:struct:: edb.protocol.LogMessage

.. eql:struct:: edb.protocol.MessageSeverity

See the :ref:`list of error codes <ref_protocol_error_codes>` for all possible
log message codes.


.. _ref_protocol_msg_ready_for_command:

ReadyForCommand
===============

Sent by: server.

Format:

.. eql:struct:: edb.protocol.ReadyForCommand

.. eql:struct:: edb.protocol.TransactionState

.. eql:struct:: edb.protocol.Annotation

.. _ref_protocol_msg_restore_ready:

RestoreReady
============

Sent by: server.

Initial :ref:`ref_protocol_msg_restore` message accepted, ready to receive
data. See :ref:`ref_protocol_restore_flow`.

Format:

.. eql:struct:: edb.protocol.RestoreReady

.. eql:struct:: edb.protocol.Annotation

.. _ref_protocol_msg_command_complete:

CommandComplete
===============

Sent by: server.

Format:

.. eql:struct:: edb.protocol.CommandComplete

.. eql:struct:: edb.protocol.Annotation


.. _ref_protocol_msg_dump:

Dump
====

Sent by: client.

Initiates a database backup. See :ref:`ref_protocol_dump_flow`.

Format:

.. eql:struct:: edb.protocol.Dump

.. eql:struct:: edb.protocol.Annotation

.. eql:struct:: edb.protocol.DumpFlag

Use:

* ``DUMP_SECRETS`` to include secrets in the backup. By default, secrets are
  not included.


.. _ref_protocol_msg_command_data_description:

CommandDataDescription
======================

Sent by: server.

Format:

.. eql:struct:: edb.protocol.CommandDataDescription

.. eql:struct:: edb.protocol.enums.Cardinality

.. eql:struct:: edb.protocol.Annotation


The format of the *input_typedesc* and *output_typedesc* fields is described
in the :ref:`ref_proto_typedesc` section.


.. _ref_protocol_msg_state_data_description:

StateDataDescription
====================

Sent by: server.

Format:

.. eql:struct:: edb.protocol.StateDataDescription


The format of the *typedesc* fields is described in the
:ref:`ref_proto_typedesc` section.


.. _ref_protocol_msg_sync:

Sync
====

Sent by: client.

Format:

.. eql:struct:: edb.protocol.Sync


.. _ref_protocol_msg_restore:

Restore
=======

Sent by: client.

Initiate restore to the current :versionreplace:`database;5.0:branch`.
See :ref:`ref_protocol_restore_flow`.

Format:

.. eql:struct:: edb.protocol.Restore

.. _ref_protocol_msg_restore_block:

RestoreBlock
============

Sent by: client.

Send dump file data block.
See :ref:`ref_protocol_restore_flow`.

Format:

.. eql:struct:: edb.protocol.RestoreBlock


.. _ref_protocol_msg_restore_eof:

RestoreEof
==========

Sent by: client.

Notify server that dump is fully uploaded.
See :ref:`ref_protocol_restore_flow`.

Format:

.. eql:struct:: edb.protocol.RestoreEof


.. _ref_protocol_msg_execute:

Execute
=======

Sent by: client.

Format:

.. eql:struct:: edb.protocol.Execute

.. eql:struct:: edb.protocol.OutputFormat

.. eql:struct:: edb.protocol.Annotation

Use:

* ``BINARY`` to return data encoded in binary.

* ``JSON`` to return data as single row and single field that contains
  the resultset as a single JSON array".

* ``JSON_ELEMENTS`` to return a single JSON string per top-level set element.
  This can be used to iterate over a large result set efficiently.

* ``NONE`` to prevent the server from returning data, even if the EdgeQL
  command does.

The data in *arguments* must be encoded as a
:ref:`tuple value <ref_protocol_fmt_tuple>` described by
a type descriptor identified by *input_typedesc_id*.


.. eql:struct:: edb.protocol.enums.Cardinality


.. _ref_protocol_msg_parse:

Parse
=====

Sent by: client.

.. eql:struct:: edb.protocol.Parse

.. eql:struct:: edb.protocol.Capability

.. eql:struct:: edb.protocol.Annotation

See RFC1004_ for more information on capability flags.

.. eql:struct:: edb.protocol.CompilationFlag

Use:

* ``0x0000_0000_0000_0001`` (``INJECT_OUTPUT_TYPE_IDS``) -- if set, all
  returned objects have a ``__tid__`` property set to their type ID
  (equivalent to having an implicit ``__tid__ := .__type__.id`` computed
  property.)

* ``0x0000_0000_0000_0002`` (``INJECT_OUTPUT_TYPE_NAMES``) -- if set all
  returned objects have a ``__tname__`` property set to their type name
  (equivalent to having an implicit ``__tname__ := .__type__.name`` computed
  property.)  Note that specifying this flag might slow down queries.

* ``0x0000_0000_0000_0004`` (``INJECT_OUTPUT_OBJECT_IDS``) -- if set all
  returned objects have an ``id`` property set to their identifier, even if
  not specified explicitly in the output shape.

.. eql:struct:: edb.protocol.OutputFormat

Use:

* ``BINARY`` to return data encoded in binary.

* ``JSON`` to return data as single row and single field that contains
  the resultset as a single JSON array".

* ``JSON_ELEMENTS`` to return a single JSON string per top-level set element.
  This can be used to iterate over a large result set efficiently.

* ``NONE`` to prevent the server from returning data, even if the EdgeQL
  statement does.

.. eql:struct:: edb.protocol.enums.Cardinality


.. _ref_protocol_msg_data:

Data
====

Sent by: server.

Format:

.. eql:struct:: edb.protocol.Data

.. eql:struct:: edb.protocol.DataElement

The exact encoding of ``DataElement.data`` is defined by the query output
:ref:`type descriptor <ref_proto_typedesc>`.

Wire formats for the standard scalar types and collections are documented in
:ref:`ref_proto_dataformats`.


.. _ref_protocol_msg_dump_header:

Dump Header
===========

Sent by: server.

Initial message of database backup protocol.
See :ref:`ref_protocol_dump_flow`.

Format:

.. eql:struct:: edb.protocol.DumpHeader

.. eql:struct:: edb.protocol.DumpTypeInfo

.. eql:struct:: edb.protocol.DumpObjectDesc

Known attributes:

* 101 ``BLOCK_TYPE`` -- block type, always "I"
* 102 ``SERVER_TIME`` -- server time when dump is started as a floating point
  unix timestamp stringified
* 103 ``SERVER_VERSION`` -- full version of server as string
* 105 ``SERVER_CATALOG_VERSION`` -- the catalog version of the server, as
  a 64-bit integer. The catalog version is an identifier that is incremented
  whenever a change is made to the database layout or standard library.


.. _ref_protocol_msg_dump_block:

Dump Block
==========

Sent by: server.

The actual protocol data in the backup protocol.
See :ref:`ref_protocol_dump_flow`.

Format:

.. eql:struct:: edb.protocol.DumpBlock


Known attributes:

* 101 ``BLOCK_TYPE`` -- block type, always "D"
* 110 ``BLOCK_ID`` -- block identifier (16 bytes of UUID)
* 111 ``BLOCK_NUM`` -- integer block index stringified
* 112 ``BLOCK_DATA`` -- the actual block data


.. _ref_protocol_msg_server_key_data:

ServerKeyData
=============

Sent by: server.

Format:

.. eql:struct:: edb.protocol.ServerKeyData


.. _ref_protocol_msg_server_parameter_status:

ParameterStatus
===============

Sent by: server.

Format:

.. eql:struct:: edb.protocol.ParameterStatus

Known statuses:

* ``suggested_pool_concurrency`` -- suggested default size for clients
  connection pools. Serialized as UTF-8 encoded string.

* ``system_config`` -- a set of instance-level configuration settings
  exposed to clients on connection. Serialized as:

  .. eql:struct:: edb.protocol.ParameterStatus_SystemConfig

  Where ``DataElement`` is defined in the same way as for the
  :ref:`Data <ref_protocol_msg_data>` message:

  .. eql:struct:: edb.protocol.DataElement


.. _ref_protocol_msg_client_handshake:

ClientHandshake
===============

Sent by: client.

Format:

.. eql:struct:: edb.protocol.ClientHandshake

.. eql:struct:: edb.protocol.ConnectionParam

.. eql:struct:: edb.protocol.ProtocolExtension

The ``ClientHandshake`` message is the first message sent by the client
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

.. eql:struct:: edb.protocol.ServerHandshake

.. eql:struct:: edb.protocol.ProtocolExtension


The ``ServerHandshake`` message is a direct response to the
:ref:`ref_protocol_msg_client_handshake` message and is sent by the server
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

.. eql:struct:: edb.protocol.AuthenticationOK

The ``AuthenticationOK`` message is sent by the server once it considers
the authentication to be successful.


.. _ref_protocol_msg_auth_sasl:

AuthenticationSASL
==================

Sent by: server.

Format:

.. eql:struct:: edb.protocol.AuthenticationRequiredSASLMessage

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
returned by the server and send an
:ref:`ref_protocol_msg_auth_sasl_initial_response`.
One or more server-challenge and client-response message follow.  Each
server-challenge is sent in an :ref:`ref_protocol_msg_auth_sasl_continue`,
followed by a response from the client in an
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

.. eql:struct:: edb.protocol.AuthenticationSASLContinue

.. _ref_protocol_msg_auth_sasl_final:

AuthenticationSASLFinal
=======================

Sent by: server.

Format:

.. eql:struct:: edb.protocol.AuthenticationSASLFinal

.. _ref_protocol_msg_auth_sasl_initial_response:

AuthenticationSASLInitialResponse
=================================

Sent by: client.

Format:

.. eql:struct:: edb.protocol.AuthenticationSASLInitialResponse

.. _ref_protocol_msg_auth_sasl_response:

AuthenticationSASLResponse
==========================

Sent by: client.

Format:

.. eql:struct:: edb.protocol.AuthenticationSASLResponse


.. _ref_protocol_msg_terminate:

Terminate
=========

Sent by: client.

Format:

.. eql:struct:: edb.protocol.Terminate

.. _RFC1004:
    https://github.com/edgedb/rfcs/blob/master/text/1004-transactions-api.rst
