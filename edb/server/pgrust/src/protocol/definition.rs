use super::gen::protocol;

protocol!(
struct Message {
    /// Identifies the message.
    mtype: u8,
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// Message contents.
    data: Rest,
}
struct AuthenticationOk {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 8,
    /// Specifies that the authentication was successful.
    status: i32 = 0,
}

struct AuthenticationKerberosV5 {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 8,
    /// Specifies that Kerberos V5 authentication is required.
    status: i32 = 2,
}

struct AuthenticationCleartextPassword {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 8,
    /// Specifies that a clear-text password is required.
    status: i32 = 3,
}

struct AuthenticationMD5Password {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 12,
    /// Specifies that an MD5-encrypted password is required.
    status: i32 = 5,
    /// The salt to use when encrypting the password.
    salt: [u8; 4],
}

struct AuthenticationGSS {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 8,
    /// Specifies that GSSAPI authentication is required.
    status: i32 = 7,
}

struct AuthenticationGSSContinue {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// Specifies that this message contains GSSAPI or SSPI data.
    status: i32 = 8,
    /// GSSAPI or SSPI authentication data.
    data: Rest,
}

struct AuthenticationSSPI {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 8,
    /// Specifies that SSPI authentication is required.
    status: i32 = 9,
}

struct AuthenticationSASL {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// Specifies that SASL authentication is required.
    status: i32 = 10,
    /// List of SASL authentication mechanisms, terminated by a zero byte.
    mechanisms: ZTArray<ZTString>,
}

struct AuthenticationSASLContinue {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// Specifies that this message contains a SASL challenge.
    status: i32 = 11,
    /// SASL data, specific to the SASL mechanism being used.
    data: Rest,
}

struct AuthenticationSASLFinal {
    /// Identifies the message as an authentication request.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// Specifies that SASL authentication has completed.
    status: i32 = 12,
    /// SASL outcome "additional data", specific to the SASL mechanism being used.
    data: Rest,
}

struct BackendKeyData {
    /// Identifies the message as cancellation key data.
    mtype: u8 = 'K',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 12,
    /// The process ID of this backend.
    pid: i32,
    /// The secret key of this backend.
    key: i32,
}

struct Bind {
    /// Identifies the message as a Bind command.
    mtype: u8 = 'B',
    /// Length of message contents in bytes, including self.
    mlen: i32,
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

struct BindComplete {
    /// Identifies the message as a Bind-complete indicator.
    mtype: u8 = '2',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 4,
}

struct CancelRequest {
    /// Length of message contents in bytes, including self.
    mlen: i32 = 16,
    /// The cancel request code.
    code: i32 = 80877102,
    /// The process ID of the target backend.
    pid: i32,
    /// The secret key for the target backend.
    key: i32,
}

struct Close {
    /// Identifies the message as a Close command.
    mtype: u8 = 'C',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// 'S' to close a prepared statement; 'P' to close a portal.
    ctype: u8,
    /// The name of the prepared statement or portal to close.
    name: ZTString,
}

struct CloseComplete {
    /// Identifies the message as a Close-complete indicator.
    mtype: u8 = '3',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 4,
}

struct CommandComplete {
    /// Identifies the message as a command-completed response.
    mtype: u8 = 'C',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// The command tag.
    tag: ZTString,
}

struct CopyData {
    /// Identifies the message as COPY data.
    mtype: u8 = 'd',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// Data that forms part of a COPY data stream.
    data: Rest,
}

struct CopyDone {
    /// Identifies the message as a COPY-complete indicator.
    mtype: u8 = 'c',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 4,
}

struct CopyFail {
    /// Identifies the message as a COPY-failure indicator.
    mtype: u8 = 'f',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// An error message to report as the cause of failure.
    error_msg: ZTString,
}

struct CopyInResponse {
    /// Identifies the message as a Start Copy In response.
    mtype: u8 = 'G',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// 0 for textual, 1 for binary.
    format: u8,
    /// The format codes for each column.
    format_codes: Array<i16, i16>,
}

struct CopyOutResponse {
    /// Identifies the message as a Start Copy Out response.
    mtype: u8 = 'H',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// 0 for textual, 1 for binary.
    format: u8,
    /// The format codes for each column.
    format_codes: Array<i16, i16>,
}

struct CopyBothResponse {
    /// Identifies the message as a Start Copy Both response.
    mtype: u8 = 'W',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// 0 for textual, 1 for binary.
    format: u8,
    /// The format codes for each column.
    format_codes: Array<i16, i16>,
}

struct DataRow {
    /// Identifies the message as a data row.
    mtype: u8 = 'D',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// Array of column values and their lengths.
    values: Array<i16, Encoded>,
}

struct Describe {
    /// Identifies the message as a Describe command.
    mtype: u8 = 'D',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// 'S' to describe a prepared statement; 'P' to describe a portal.
    dtype: u8,
    /// The name of the prepared statement or portal.
    name: ZTString,
}

struct EmptyQueryResponse {
    /// Identifies the message as a response to an empty query String.
    mtype: u8 = 'I',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 4,
}

struct ErrorResponse {
    /// Identifies the message as an error.
    mtype: u8 = 'E',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// Array of error fields and their values.
    fields: ZTArray<ErrorField>,
}

struct ErrorField {
    /// A code identifying the field type.
    etype: u8,
    /// The field value.
    value: ZTString,
}

struct Execute {
    /// Identifies the message as an Execute command.
    mtype: u8 = 'E',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// The name of the portal to execute.
    portal: ZTString,
    /// Maximum number of rows to return.
    max_rows: i32,
}

struct Flush {
    /// Identifies the message as a Flush command.
    mtype: u8 = 'H',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 4,
}

struct FunctionCall {
    /// Identifies the message as a function call.
    mtype: u8 = 'F',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// OID of the function to execute.
    function_id: i32,
    /// The parameter format codes.
    format_codes: Array<i16, i16>,
    /// Array of args and their lengths.
    args: Array<i16, Encoded>,
    /// The format code for the result.
    result_format_code: i16,
}

struct FunctionCallResponse {
    /// Identifies the message as a function-call response.
    mtype: u8 = 'V',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// The function result value.
    result: Encoded,
}

struct GSSENCRequest {
    /// Identifies the message as a GSSAPI Encryption request.
    mtype: u8 = 'F',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 8,
    /// The GSSAPI Encryption request code.
    gssenc_request_code: i32 = 80877104,
}

struct GSSResponse {
    /// Identifies the message as a GSSAPI or SSPI response.
    mtype: u8 = 'p',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// GSSAPI or SSPI authentication data.
    data: Rest,
}

struct NegotiateProtocolVersion {
    /// Identifies the message as a protocol version negotiation request.
    mtype: u8 = 'v',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// Newest minor protocol version supported by the server.
    minor_version: i32,
    /// List of protocol options not recognized.
    options: Array<i32, ZTString>,
}

struct NoData {
    /// Identifies the message as a No Data indicator.
    mtype: u8 = 'n',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 4,
}

struct NoticeResponse {
    /// Identifies the message as a notice.
    mtype: u8 = 'N',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// Array of notice fields and their values.
    fields: ZTArray<NoticeField>,
}

struct NoticeField {
    /// A code identifying the field type.
    ntype: u8,
    /// The field value.
    value: ZTString,
}

struct NotificationResponse {
    /// Identifies the message as a notification.
    mtype: u8 = 'A',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// The process ID of the notifying backend.
    pid: i32,
    /// The name of the notification channel.
    channel: ZTString,
    /// The notification payload.
    payload: ZTString,
}

struct ParameterDescription {
    /// Identifies the message as a parameter description.
    mtype: u8 = 't',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// OIDs of the parameter data types.
    param_types: Array<i16, i32>,
}

struct ParameterStatus {
    /// Identifies the message as a runtime parameter status report.
    mtype: u8 = 'S',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// The name of the parameter.
    name: ZTString,
    /// The current value of the parameter.
    value: ZTString,
}

struct Parse {
    /// Identifies the message as a Parse command.
    mtype: u8 = 'P',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// The name of the destination prepared statement.
    statement: ZTString,
    /// The query String to be parsed.
    query: ZTString,
    /// OIDs of the parameter data types.
    param_types: Array<i16, i32>,
}

struct ParseComplete {
    /// Identifies the message as a Parse-complete indicator.
    mtype: u8 = '1',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 4,
}

struct PasswordMessage {
    /// Identifies the message as a password response.
    mtype: u8 = 'p',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// The password (encrypted or plaintext, depending on context).
    password: ZTString,
}

struct PortalSuspended {
    /// Identifies the message as a portal-suspended indicator.
    mtype: u8 = 's',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 4,
}

struct Query {
    /// Identifies the message as a simple query command.
    mtype: u8 = 'Q',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// The query String to be executed.
    query: ZTString,
}

struct ReadyForQuery {
    /// Identifies the message as a ready-for-query indicator.
    mtype: u8 = 'Z',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 5,
    /// Current transaction status indicator.
    status: u8,
}

struct RowDescription {
    /// Identifies the message as a row description.
    mtype: u8 = 'T',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// Array of field descriptions.
    fields: Array<i16, RowField>,
}

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

struct SASLInitialResponse {
    /// Identifies the message as a SASL initial response.
    mtype: u8 = 'p',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// Name of the SASL authentication mechanism.
    mechanism: ZTString,
    /// SASL initial response data.
    response: Array<i32, u8>,
}

struct SASLResponse {
    /// Identifies the message as a SASL response.
    mtype: u8 = 'p',
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// SASL response data.
    response: Rest,
}

struct SSLRequest {
    /// Length of message contents in bytes, including self.
    mlen: i32 = 8,
    /// The SSL request code.
    code: i32 = 80877103,
}

struct StartupMessage {
    /// Length of message contents in bytes, including self.
    mlen: i32,
    /// The protocol version number.
    code: i32 = 196608,
    /// List of parameter name-value pairs, terminated by a zero byte.
    params: ZTArray<StartupNameValue>,
}

struct StartupNameValue {
    /// The parameter name. 
    name: ZTString,
    /// The parameter value.
    value: ZTString,
}

struct Sync {
    /// Identifies the message as a Sync command.
    mtype: u8 = 'S',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 4,
}

struct Terminate {
    /// Identifies the message as a Terminate command.
    mtype: u8 = 'X',
    /// Length of message contents in bytes, including self.
    mlen: i32 = 4,
}
);
