use db_proto::{message_group, protocol};

message_group!(
    /// The `Backend` message group contains messages sent from the backend to the frontend.
    Backend: Message = [
        AuthenticationOk,
        AuthenticationKerberosV5,
        AuthenticationCleartextPassword,
        AuthenticationMD5Password,
        AuthenticationGSS,
        AuthenticationGSSContinue,
        AuthenticationSSPI,
        AuthenticationSASL,
        AuthenticationSASLContinue,
        AuthenticationSASLFinal,
        BackendKeyData,
        BindComplete,
        CloseComplete,
        CommandComplete,
        CopyData,
        CopyDone,
        CopyInResponse,
        CopyOutResponse,
        CopyBothResponse,
        DataRow,
        EmptyQueryResponse,
        ErrorResponse,
        FunctionCallResponse,
        NegotiateProtocolVersion,
        NoData,
        NoticeResponse,
        NotificationResponse,
        ParameterDescription,
        ParameterStatus,
        ParseComplete,
        PortalSuspended,
        ReadyForQuery,
        RowDescription
    ]
);

message_group!(
    /// The `Frontend` message group contains messages sent from the frontend to the backend.
    Frontend: Message = [
        Bind,
        Close,
        CopyData,
        CopyDone,
        CopyFail,
        Describe,
        Execute,
        Flush,
        FunctionCall,
        GSSResponse,
        Parse,
        PasswordMessage,
        Query,
        SASLInitialResponse,
        SASLResponse,
        Sync,
        Terminate
    ]
);

message_group!(
    /// The `Initial` message group contains messages that are sent before the
    /// normal message flow.
    Initial: InitialMessage = [
        CancelRequest,
        GSSENCRequest,
        SSLRequest,
        StartupMessage
    ]
);

protocol!(

/// A generic base for all Postgres mtype/mlen-style messages.
struct Message {
    /// Identifies the message.
    mtype: u8,
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Message contents.
    data: Rest,
}

/// A generic base for all initial Postgres messages.
struct InitialMessage {
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// The identifier for this initial message.
    protocol_version: i32,
    /// Message contents.
    data: Rest
}

/// The `AuthenticationMessage` struct is a base for all Postgres authentication messages.
struct AuthenticationMessage: Message {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Specifies that the authentication was successful.
    status: i32,
}

/// The `AuthenticationOk` struct represents a message indicating successful authentication.
struct AuthenticationOk: Message {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len = 8,
    /// Specifies that the authentication was successful.
    status: i32 = 0,
}

/// The `AuthenticationKerberosV5` struct represents a message indicating that Kerberos V5 authentication is required.
struct AuthenticationKerberosV5: Message {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len = 8,
    /// Specifies that Kerberos V5 authentication is required.
    status: i32 = 2,
}

/// The `AuthenticationCleartextPassword` struct represents a message indicating that a cleartext password is required for authentication.
struct AuthenticationCleartextPassword: Message {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len = 8,
    /// Specifies that a clear-text password is required.
    status: i32 = 3,
}

/// The `AuthenticationMD5Password` struct represents a message indicating that an MD5-encrypted password is required for authentication.
struct AuthenticationMD5Password: Message {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len = 12,
    /// Specifies that an MD5-encrypted password is required.
    status: i32 = 5,
    /// The salt to use when encrypting the password.
    salt: [u8; 4],
}

/// The `AuthenticationSCMCredential` struct represents a message indicating that an SCM credential is required for authentication.
struct AuthenticationSCMCredential: Message {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len = 6,
    /// Any data byte, which is ignored.
    byte: u8 = 0,
}

/// The `AuthenticationGSS` struct represents a message indicating that GSSAPI authentication is required.
struct AuthenticationGSS: Message {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len = 8,
    /// Specifies that GSSAPI authentication is required.
    status: i32 = 7,
}

/// The `AuthenticationGSSContinue` struct represents a message indicating the continuation of GSSAPI authentication.
struct AuthenticationGSSContinue: Message {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Specifies that this message contains GSSAPI or SSPI data.
    status: i32 = 8,
    /// GSSAPI or SSPI authentication data.
    data: Rest,
}

/// The `AuthenticationSSPI` struct represents a message indicating that SSPI authentication is required.
struct AuthenticationSSPI: Message {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len = 8,
    /// Specifies that SSPI authentication is required.
    status: i32 = 9,
}

/// The `AuthenticationSASL` struct represents a message indicating that SASL authentication is required.
struct AuthenticationSASL: Message {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Specifies that SASL authentication is required.
    status: i32 = 10,
    /// List of SASL authentication mechanisms, terminated by a zero byte.
    mechanisms: ZTArray<ZTString>,
}

/// The `AuthenticationSASLContinue` struct represents a message containing a SASL challenge during the authentication process.
struct AuthenticationSASLContinue: Message {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Specifies that this message contains a SASL challenge.
    status: i32 = 11,
    /// SASL data, specific to the SASL mechanism being used.
    data: Rest,
}

/// The `AuthenticationSASLFinal` struct represents a message indicating the completion of SASL authentication.
struct AuthenticationSASLFinal: Message {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Specifies that SASL authentication has completed.
    status: i32 = 12,
    /// SASL outcome "additional data", specific to the SASL mechanism being used.
    data: Rest,
}

/// The `BackendKeyData` struct represents a message containing the process ID and secret key for this backend.
struct BackendKeyData: Message {
    /// Identifies the message as cancellation key data.
    mtype: u8 = 'K',
    /// Length of message contents in bytes, including self.
    mlen: len = 12,
    /// The process ID of this backend.
    pid: i32,
    /// The secret key of this backend.
    key: i32,
}

/// The `Bind` struct represents a message to bind a named portal to a prepared statement.
struct Bind: Message {
    /// Identifies the message as a Bind command.
    mtype: u8 = 'B',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// The name of the destination portal.
    portal: ZTString,
    /// The name of the source prepared statement.
    statement: ZTString,
    /// The parameter format codes.
    format_codes: Array<i16, i16>,
    /// Array of parameter values and their lengths.
    values: Array<i16, Encoded>,
    /// The result-column format codes.
    result_format_codes: Array<i16, i16>,
}

/// The `BindComplete` struct represents a message indicating that a Bind operation was successful.
struct BindComplete: Message {
    /// Identifies the message as a Bind-complete indicator.
    mtype: u8 = '2',
    /// Length of message contents in bytes, including self.
    mlen: len = 4,
}

/// The `CancelRequest` struct represents a message to request the cancellation of a query.
struct CancelRequest: InitialMessage {
    /// Length of message contents in bytes, including self.
    mlen: len = 16,
    /// The cancel request code.
    code: i32 = 80877102,
    /// The process ID of the target backend.
    pid: i32,
    /// The secret key for the target backend.
    key: i32,
}

/// The `Close` struct represents a message to close a prepared statement or portal.
struct Close: Message {
    /// Identifies the message as a Close command.
    mtype: u8 = 'C',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// 'S' to close a prepared statement; 'P' to close a portal.
    ctype: u8,
    /// The name of the prepared statement or portal to close.
    name: ZTString,
}

/// The `CloseComplete` struct represents a message indicating that a Close operation was successful.
struct CloseComplete: Message {
    /// Identifies the message as a Close-complete indicator.
    mtype: u8 = '3',
    /// Length of message contents in bytes, including self.
    mlen: len = 4,
}

/// The `CommandComplete` struct represents a message indicating the successful completion of a command.
struct CommandComplete: Message {
    /// Identifies the message as a command-completed response.
    mtype: u8 = 'C',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// The command tag.
    tag: ZTString,
}

/// The `CopyData` struct represents a message containing data for a copy operation.
struct CopyData: Message {
    /// Identifies the message as COPY data.
    mtype: u8 = 'd',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Data that forms part of a COPY data stream.
    data: Rest,
}

/// The `CopyDone` struct represents a message indicating that a copy operation is complete.
struct CopyDone: Message {
    /// Identifies the message as a COPY-complete indicator.
    mtype: u8 = 'c',
    /// Length of message contents in bytes, including self.
    mlen: len = 4,
}

/// The `CopyFail` struct represents a message indicating that a copy operation has failed.
struct CopyFail: Message {
    /// Identifies the message as a COPY-failure indicator.
    mtype: u8 = 'f',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// An error message to report as the cause of failure.
    error_msg: ZTString,
}

/// The `CopyInResponse` struct represents a message indicating that the server is ready to receive data for a copy-in operation.
struct CopyInResponse: Message {
    /// Identifies the message as a Start Copy In response.
    mtype: u8 = 'G',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// 0 for textual, 1 for binary.
    format: u8,
    /// The format codes for each column.
    format_codes: Array<i16, i16>,
}

/// The `CopyOutResponse` struct represents a message indicating that the server is ready to send data for a copy-out operation.
struct CopyOutResponse: Message {
    /// Identifies the message as a Start Copy Out response.
    mtype: u8 = 'H',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// 0 for textual, 1 for binary.
    format: u8,
    /// The format codes for each column.
    format_codes: Array<i16, i16>,
}

/// The `CopyBothResponse` is used only for Streaming Replication.
struct CopyBothResponse: Message {
    /// Identifies the message as a Start Copy Both response.
    mtype: u8 = 'W',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// 0 for textual, 1 for binary.
    format: u8,
    /// The format codes for each column.
    format_codes: Array<i16, i16>,
}

/// The `DataRow` struct represents a message containing a row of data.
struct DataRow: Message {
    /// Identifies the message as a data row.
    mtype: u8 = 'D',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Array of column values and their lengths.
    values: Array<i16, Encoded>,
}

/// The `Describe` struct represents a message to describe a prepared statement or portal.
struct Describe: Message {
    /// Identifies the message as a Describe command.
    mtype: u8 = 'D',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// 'S' to describe a prepared statement; 'P' to describe a portal.
    dtype: u8,
    /// The name of the prepared statement or portal.
    name: ZTString,
}

/// The `EmptyQueryResponse` struct represents a message indicating that an empty query string was recognized.
struct EmptyQueryResponse: Message {
    /// Identifies the message as a response to an empty query String.
    mtype: u8 = 'I',
    /// Length of message contents in bytes, including self.
    mlen: len = 4,
}

/// The `ErrorResponse` struct represents a message indicating that an error has occurred.
struct ErrorResponse: Message {
    /// Identifies the message as an error.
    mtype: u8 = 'E',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Array of error fields and their values.
    fields: ZTArray<ErrorField>,
}

/// The `ErrorField` struct represents a single error message within an `ErrorResponse`.
struct ErrorField {
    /// A code identifying the field type.
    etype: u8,
    /// The field value.
    value: ZTString,
}

/// The `Execute` struct represents a message to execute a prepared statement or portal.
struct Execute: Message {
    /// Identifies the message as an Execute command.
    mtype: u8 = 'E',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// The name of the portal to execute.
    portal: ZTString,
    /// Maximum number of rows to return.
    max_rows: i32,
}

/// The `Flush` struct represents a message to flush the backend's output buffer.
struct Flush: Message {
    /// Identifies the message as a Flush command.
    mtype: u8 = 'H',
    /// Length of message contents in bytes, including self.
    mlen: len = 4,
}

/// The `FunctionCall` struct represents a message to call a function.
struct FunctionCall: Message {
    /// Identifies the message as a function call.
    mtype: u8 = 'F',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// OID of the function to execute.
    function_id: i32,
    /// The parameter format codes.
    format_codes: Array<i16, i16>,
    /// Array of args and their lengths.
    args: Array<i16, Encoded>,
    /// The format code for the result.
    result_format_code: i16,
}

/// The `FunctionCallResponse` struct represents a message containing the result of a function call.
struct FunctionCallResponse: Message {
    /// Identifies the message as a function-call response.
    mtype: u8 = 'V',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// The function result value.
    result: Encoded,
}

/// The `GSSENCRequest` struct represents a message requesting GSSAPI encryption.
struct GSSENCRequest: InitialMessage {
    /// Length of message contents in bytes, including self.
    mlen: len = 8,
    /// The GSSAPI Encryption request code.
    gssenc_request_code: i32 = 80877104,
}

/// The `GSSResponse` struct represents a message containing a GSSAPI or SSPI response.
struct GSSResponse: Message {
    /// Identifies the message as a GSSAPI or SSPI response.
    mtype: u8 = 'p',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// GSSAPI or SSPI authentication data.
    data: Rest,
}

/// The `NegotiateProtocolVersion` struct represents a message requesting protocol version negotiation.
struct NegotiateProtocolVersion: Message {
    /// Identifies the message as a protocol version negotiation request.
    mtype: u8 = 'v',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Newest minor protocol version supported by the server.
    minor_version: i32,
    /// List of protocol options not recognized.
    options: Array<i32, ZTString>,
}

/// The `NoData` struct represents a message indicating that there is no data to return.
struct NoData: Message {
    /// Identifies the message as a No Data indicator.
    mtype: u8 = 'n',
    /// Length of message contents in bytes, including self.
    mlen: len = 4,
}

/// The `NoticeResponse` struct represents a message containing a notice.
struct NoticeResponse: Message {
    /// Identifies the message as a notice.
    mtype: u8 = 'N',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Array of notice fields and their values.
    fields: ZTArray<NoticeField>,
}

/// The `NoticeField` struct represents a single error message within an `NoticeResponse`.
struct NoticeField: Message {
    /// A code identifying the field type.
    ntype: u8,
    /// The field value.
    value: ZTString,
}

/// The `NotificationResponse` struct represents a message containing a notification from the backend.
struct NotificationResponse: Message {
    /// Identifies the message as a notification.
    mtype: u8 = 'A',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// The process ID of the notifying backend.
    pid: i32,
    /// The name of the notification channel.
    channel: ZTString,
    /// The notification payload.
    payload: ZTString,
}

/// The `ParameterDescription` struct represents a message describing the parameters needed by a prepared statement.
struct ParameterDescription: Message {
    /// Identifies the message as a parameter description.
    mtype: u8 = 't',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// OIDs of the parameter data types.
    param_types: Array<i16, i32>,
}

/// The `ParameterStatus` struct represents a message containing the current status of a parameter.
struct ParameterStatus: Message {
    /// Identifies the message as a runtime parameter status report.
    mtype: u8 = 'S',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// The name of the parameter.
    name: ZTString,
    /// The current value of the parameter.
    value: ZTString,
}

/// The `Parse` struct represents a message to parse a query string.
struct Parse: Message {
    /// Identifies the message as a Parse command.
    mtype: u8 = 'P',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// The name of the destination prepared statement.
    statement: ZTString,
    /// The query string to be parsed.
    query: ZTString,
    /// OIDs of the parameter data types.
    param_types: Array<i16, i32>,
}

/// The `ParseComplete` struct represents a message indicating that a Parse operation was successful.
struct ParseComplete: Message {
    /// Identifies the message as a Parse-complete indicator.
    mtype: u8 = '1',
    /// Length of message contents in bytes, including self.
    mlen: len = 4,
}

/// The `PasswordMessage` struct represents a message containing a password.
struct PasswordMessage: Message {
    /// Identifies the message as a password response.
    mtype: u8 = 'p',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// The password (encrypted or plaintext, depending on context).
    password: ZTString,
}

/// The `PortalSuspended` struct represents a message indicating that a portal has been suspended.
struct PortalSuspended: Message {
    /// Identifies the message as a portal-suspended indicator.
    mtype: u8 = 's',
    /// Length of message contents in bytes, including self.
    mlen: len = 4,
}

/// The `Query` struct represents a message to execute a simple query.
struct Query: Message {
    /// Identifies the message as a simple query command.
    mtype: u8 = 'Q',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// The query String to be executed.
    query: ZTString,
}

/// The `ReadyForQuery` struct represents a message indicating that the backend is ready for a new query.
struct ReadyForQuery: Message {
    /// Identifies the message as a ready-for-query indicator.
    mtype: u8 = 'Z',
    /// Length of message contents in bytes, including self.
    mlen: len = 5,
    /// Current transaction status indicator.
    status: u8,
}

/// The `RowDescription` struct represents a message describing the rows that will be returned by a query.
struct RowDescription: Message {
    /// Identifies the message as a row description.
    mtype: u8 = 'T',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Array of field descriptions.
    fields: Array<i16, RowField>,
}

/// The `RowField` struct represents a row within the `RowDescription` message.
struct RowField {
    /// The field name
    name: ZTString,
    /// The table ID (OID) of the table the column is from, or 0 if not a column reference
    table_oid: i32,
    /// The attribute number of the column, or 0 if not a column reference
    column_attr_number: i16,
    /// The object ID of the field's data type
    data_type_oid: i32,
    /// The data type size (negative if variable size)
    data_type_size: i16,
    /// The type modifier
    type_modifier: i32,
    /// The format code being used for the field (0 for text, 1 for binary)
    format_code: i16,
}

/// The `SASLInitialResponse` struct represents a message containing a SASL initial response.
struct SASLInitialResponse: Message {
    /// Identifies the message as a SASL initial response.
    mtype: u8 = 'p',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Name of the SASL authentication mechanism.
    mechanism: ZTString,
    /// SASL initial response data.
    response: Array<i32, u8>,
}

/// The `SASLResponse` struct represents a message containing a SASL response.
struct SASLResponse: Message {
    /// Identifies the message as a SASL response.
    mtype: u8 = 'p',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// SASL response data.
    response: Rest,
}

/// The `SSLRequest` struct represents a message requesting SSL encryption.
struct SSLRequest: InitialMessage {
    /// Length of message contents in bytes, including self.
    mlen: len = 8,
    /// The SSL request code.
    code: i32 = 80877103,
}

struct SSLResponse {
    /// Specifies if SSL was accepted or rejected.
    code: u8,
}

/// The `StartupMessage` struct represents a message to initiate a connection.
struct StartupMessage: InitialMessage {
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// The protocol version number.
    protocol: i32 = 196608,
    /// List of parameter name-value pairs, terminated by a zero byte.
    params: ZTArray<StartupNameValue>,
}

/// The `StartupMessage` struct represents a name/value pair within the `StartupMessage` message.
struct StartupNameValue {
    /// The parameter name.
    name: ZTString,
    /// The parameter value.
    value: ZTString,
}

/// The `Sync` struct represents a message to synchronize the frontend and backend.
struct Sync: Message {
    /// Identifies the message as a Sync command.
    mtype: u8 = 'S',
    /// Length of message contents in bytes, including self.
    mlen: len = 4,
}

/// The `Terminate` struct represents a message to terminate a connection.
struct Terminate: Message {
    /// Identifies the message as a Terminate command.
    mtype: u8 = 'X',
    /// Length of message contents in bytes, including self.
    mlen: len = 4,
}
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all() {
        let message = meta::Message::default();
        let initial_message = meta::InitialMessage::default();

        for meta in meta::ALL {
            eprintln!("{meta:#?}");
            if **meta != message && **meta != initial_message {
                if meta.field("mtype").is_some() && meta.field("mlen").is_some() {
                    // If a message has mtype and mlen, it should subclass Message
                    assert_eq!(*meta.parent().unwrap(), message);
                } else if meta.field("mlen").is_some() {
                    // If a message has mlen only, it should subclass InitialMessage
                    assert_eq!(*meta.parent().unwrap(), initial_message);
                }
            }
        }
    }
}
