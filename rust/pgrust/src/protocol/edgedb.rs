use db_proto::{message_group, protocol};

message_group!(
    EdgeDBBackend: Message = [
        AuthenticationOk,
        AuthenticationRequiredSASLMessage,
        AuthenticationSASLContinue,
        AuthenticationSASLFinal,
        ServerKeyData,
        ParameterStatus,
        ServerHandshake,
        ReadyForCommand,
        RestoreReady,
        CommandComplete,
        CommandDataDescription,
        StateDataDescription,
        Data,
        DumpHeader,
        DumpBlock,
        ErrorResponse,
        LogMessage
    ]
);

message_group!(
    EdgeDBFrontend: Message = [
        ClientHandshake,
        AuthenticationSASLInitialResponse,
        AuthenticationSASLResponse,
        Parse,
        Execute,
        Sync,
        Flush,
        Terminate,
        Dump,
        Restore,
        RestoreBlock,
        RestoreEof
    ]
);

protocol!(

/// A generic base for all EdgeDB mtype/mlen-style messages.
struct Message {
    /// Identifies the message.
    mtype: u8,
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Message contents.
    data: Rest,
}

/// The `ErrorResponse` struct represents an error message sent from the server.
struct ErrorResponse: Message {
    /// Identifies the message as an error response.
    mtype: u8 = 'E',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Message severity.
    severity: u8,
    /// Message code.
    error_code: i32,
    /// Error message.
    message: LString,
    /// Error attributes.
    attributes: Array<i16, KeyValue>,
}

/// The `LogMessage` struct represents a log message sent from the server.
struct LogMessage: Message {
    /// Identifies the message as a log message.
    mtype: u8 = 'L',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Message severity.
    severity: u8,
    /// Message code.
    code: i32,
    /// Message text.
    text: LString,
    /// Message annotations.
    annotations: Array<i16, Annotation>,
}

/// The `ReadyForCommand` struct represents a message indicating the server is ready for a new command.
struct ReadyForCommand: Message {
    /// Identifies the message as ready for command.
    mtype: u8 = 'Z',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Message annotations.
    annotations: Array<i16, Annotation>,
    /// Transaction state.
    transaction_state: u8,
}

/// The `RestoreReady` struct represents a message indicating the server is ready for restore.
struct RestoreReady: Message {
    /// Identifies the message as restore ready.
    mtype: u8 = '+',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Message annotations.
    annotations: Array<i16, Annotation>,
    /// Number of parallel jobs for restore.
    jobs: i16,
}

/// The `CommandComplete` struct represents a message indicating a command has completed.
struct CommandComplete: Message {
    /// Identifies the message as command complete.
    mtype: u8 = 'C',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Message annotations.
    annotations: Array<i16, Annotation>,
    /// A bit mask of allowed capabilities.
    capabilities: u64,
    /// Command status.
    status: LString,
    /// State data descriptor ID.
    state_typedesc_id: Uuid,
    /// Encoded state data.
    state_data: Array<u32, u8>,
}

/// The `CommandDataDescription` struct represents a description of command data.
struct CommandDataDescription: Message {
    /// Identifies the message as command data description.
    mtype: u8 = 'T',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Message annotations.
    annotations: Array<i16, Annotation>,
    /// A bit mask of allowed capabilities.
    capabilities: u64,
    /// Actual result cardinality.
    result_cardinality: u8,
    /// Argument data descriptor ID.
    input_typedesc_id: Uuid,
    /// Argument data descriptor.
    input_typedesc: Array<u32, u8>,
    /// Output data descriptor ID.
    output_typedesc_id: Uuid,
    /// Output data descriptor.
    output_typedesc: Array<u32, u8>,
}

/// The `StateDataDescription` struct represents a description of state data.
struct StateDataDescription: Message {
    /// Identifies the message as state data description.
    mtype: u8 = 's',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Updated state data descriptor ID.
    typedesc_id: Uuid,
    /// State data descriptor.
    typedesc: Array<u32, u8>,
}

/// The `Data` struct represents a data message.
struct Data: Message {
    /// Identifies the message as data.
    mtype: u8 = 'D',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Encoded output data array.
    data: Array<i16, DataElement>,
}

/// The `DumpHeader` struct represents a dump header message.
struct DumpHeader: Message {
    /// Identifies the message as dump header.
    mtype: u8 = '@',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Dump attributes.
    attributes: Array<i16, KeyValue>,
    /// Major version of EdgeDB.
    major_ver: i16,
    /// Minor version of EdgeDB.
    minor_ver: i16,
    /// Schema.
    schema_ddl: LString,
    /// Type identifiers.
    types: Array<i32, DumpTypeInfo>,
    /// Object descriptors.
    descriptors: Array<i32, DumpObjectDesc>,
}

/// The `DumpBlock` struct represents a dump block message.
struct DumpBlock: Message {
    /// Identifies the message as dump block.
    mtype: u8 = '=',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Dump attributes.
    attributes: Array<i16, KeyValue>,
}

/// The `ServerKeyData` struct represents server key data.
struct ServerKeyData: Message {
    /// Identifies the message as server key data.
    mtype: u8 = 'K',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Key data.
    data: [u8; 32],
}

/// The `ParameterStatus` struct represents a parameter status message.
struct ParameterStatus: Message {
    /// Identifies the message as parameter status.
    mtype: u8 = 'S',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Parameter name.
    name: Array<u32, u8>,
    /// Parameter value.
    value: Array<u32, u8>,
}

/// The `ServerHandshake` struct represents a server handshake message.
struct ServerHandshake: Message {
    /// Identifies the message as server handshake.
    mtype: u8 = 'v',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Maximum supported or client-requested protocol major version.
    major_ver: i16,
    /// Maximum supported or client-requested protocol minor version.
    minor_ver: i16,
    /// Supported protocol extensions.
    extensions: Array<i16, ProtocolExtension>,
}

/// The `AuthenticationOk` struct represents a successful authentication message.
struct AuthenticationOk: Message {
    /// Identifies the message as authentication OK.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Specifies that this message contains a successful authentication indicator.
    auth_status: i32 = 0x0,
}

/// The `AuthenticationRequiredSASLMessage` struct represents a SASL authentication request.
struct AuthenticationRequiredSASLMessage: Message {
    /// Identifies the message as authentication required SASL.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Specifies that this message contains a SASL authentication request.
    auth_status: i32 = 0x0A,
    /// A list of supported SASL authentication methods.
    methods: Array<i32, LString>,
}

/// The `AuthenticationSASLContinue` struct represents a SASL challenge.
struct AuthenticationSASLContinue: Message {
    /// Identifies the message as authentication SASL continue.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Specifies that this message contains a SASL challenge.
    auth_status: i32 = 0x0B,
    /// Mechanism-specific SASL data.
    sasl_data: Array<u32, u8>,
}

/// The `AuthenticationSASLFinal` struct represents the completion of SASL authentication.
struct AuthenticationSASLFinal: Message {
    /// Identifies the message as authentication SASL final.
    mtype: u8 = 'R',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Specifies that SASL authentication has completed.
    auth_status: i32 = 0x0C,
    /// SASL data.
    sasl_data: Array<u32, u8>,
}

/// The `Dump` struct represents a dump message from the client.
struct Dump: Message {
    /// Identifies the message as dump.
    mtype: u8 = '>',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Message annotations.
    annotations: Array<i16, Annotation>,
}

/// The `Sync` struct represents a synchronization message from the client.
struct Sync: Message {
    /// Identifies the message as sync.
    mtype: u8 = 'S',
    /// Length of message contents in bytes, including self.
    mlen: len,
}

/// The `Flush` struct represents a flush message from the client.
struct Flush: Message {
    /// Identifies the message as flush.
    mtype: u8 = 'H',
    /// Length of message contents in bytes, including self.
    mlen: len,
}

/// The `Restore` struct represents a restore message from the client.
struct Restore: Message {
    /// Identifies the message as restore.
    mtype: u8 = '<',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Restore attributes.
    attributes: Array<i16, KeyValue>,
    /// Number of parallel jobs for restore.
    jobs: i16,
    /// Original DumpHeader packet data excluding mtype and message_length.
    header_data: Array<u32, u8>,
}

/// The `RestoreBlock` struct represents a restore block message from the client.
struct RestoreBlock: Message {
    /// Identifies the message as restore block.
    mtype: u8 = '=',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Original DumpBlock packet data excluding mtype and message_length.
    block_data: Array<u32, u8>,
}

/// The `RestoreEof` struct represents the end of restore message from the client.
struct RestoreEof: Message {
    /// Identifies the message as restore EOF.
    mtype: u8 = '.',
    /// Length of message contents in bytes, including self.
    mlen: len,
}

/// The `Parse` struct represents a parse message from the client.
struct Parse: Message {
    /// Identifies the message as parse.
    mtype: u8 = 'P',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Message annotations.
    annotations: Array<i16, Annotation>,
    /// A bit mask of allowed capabilities.
    allowed_capabilities: u64,
    /// A bit mask of query options.
    compilation_flags: u64,
    /// Implicit LIMIT clause on returned sets.
    implicit_limit: u64,
    /// Data output format.
    output_format: u8,
    /// Expected result cardinality.
    expected_cardinality: u8,
    /// Command text.
    command_text: LString,
    /// State data descriptor ID.
    state_typedesc_id: Uuid,
    /// Encoded state data.
    state_data: Array<u32, u8>,
}

/// The `Execute` struct represents an execute message from the client.
struct Execute: Message {
    /// Identifies the message as execute.
    mtype: u8 = 'O',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Message annotations.
    annotations: Array<i16, Annotation>,
    /// A bit mask of allowed capabilities.
    allowed_capabilities: u64,
    /// A bit mask of query options.
    compilation_flags: u64,
    /// Implicit LIMIT clause on returned sets.
    implicit_limit: u64,
    /// Data output format.
    output_format: u8,
    /// Expected result cardinality.
    expected_cardinality: u8,
    /// Command text.
    command_text: LString,
    /// State data descriptor ID.
    state_typedesc_id: Uuid,
    /// Encoded state data.
    state_data: Array<u32, u8>,
    /// Argument data descriptor ID.
    input_typedesc_id: Uuid,
    /// Output data descriptor ID.
    output_typedesc_id: Uuid,
    /// Encoded argument data.
    arguments: Array<u32, u8>,
}

/// The `ClientHandshake` struct represents a client handshake message.
struct ClientHandshake: Message {
    /// Identifies the message as client handshake.
    mtype: u8 = 'V',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Requested protocol major version.
    major_ver: i16,
    /// Requested protocol minor version.
    minor_ver: i16,
    /// Connection parameters.
    params: Array<i16, ConnectionParam>,
    /// Requested protocol extensions.
    extensions: Array<i16, ProtocolExtension>,
}

/// The `Terminate` struct represents a termination message from the client.
struct Terminate: Message {
    /// Identifies the message as terminate.
    mtype: u8 = 'X',
    /// Length of message contents in bytes, including self.
    mlen: len,
}

/// The `AuthenticationSASLInitialResponse` struct represents the initial SASL response from the client.
struct AuthenticationSASLInitialResponse: Message {
    /// Identifies the message as authentication SASL initial response.
    mtype: u8 = 'p',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Name of the SASL authentication mechanism that the client selected.
    method: LString,
    /// Mechanism-specific "Initial Response" data.
    sasl_data: Array<u32, u8>,
}

/// The `AuthenticationSASLResponse` struct represents a SASL response from the client.
struct AuthenticationSASLResponse: Message {
    /// Identifies the message as authentication SASL response.
    mtype: u8 = 'r',
    /// Length of message contents in bytes, including self.
    mlen: len,
    /// Mechanism-specific response data.
    sasl_data: Array<u32, u8>,
}

/// The `KeyValue` struct represents a key-value pair.
struct KeyValue {
    /// Key code (specific to the type of the Message).
    code: i16,
    /// Value data.
    value: Array<u32, u8>,
}

/// The `Annotation` struct represents an annotation.
struct Annotation {
    /// Name of the annotation.
    name: LString,
    /// Value of the annotation (in JSON format).
    value: LString,
}

/// The `DataElement` struct represents a data element.
struct DataElement {
    /// Encoded output data.
    data: Array<i32, u8>,
}

/// The `DumpTypeInfo` struct represents type information in a dump.
struct DumpTypeInfo {
    /// Type name.
    type_name: LString,
    /// Type class.
    type_class: LString,
    /// Type ID.
    type_id: Uuid,
}

/// The `DumpObjectDesc` struct represents an object descriptor in a dump.
struct DumpObjectDesc {
    /// Object ID.
    object_id: Uuid,
    /// Description.
    description: Array<u32, u8>,
    /// Dependencies.
    dependencies: Array<i16, Uuid>,
}

/// The `ProtocolExtension` struct represents a protocol extension.
struct ProtocolExtension {
    /// Extension name.
    name: LString,
    /// A set of extension annotations.
    annotations: Array<i16, Annotation>,
}

/// The `ConnectionParam` struct represents a connection parameter.
struct ConnectionParam {
    /// Parameter name.
    name: LString,
    /// Parameter value.
    value: LString,
}
);
