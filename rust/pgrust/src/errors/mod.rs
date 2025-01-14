use core::str;
use paste::paste;
use std::{collections::HashMap, str::FromStr};

pub mod edgedb;

use crate::protocol::postgres::data::{ErrorResponse, NoticeResponse};

#[macro_export]
macro_rules! pg_error_class {
    ($(
        #[doc=$doc:literal]
        $code:expr => $name:ident
    ),* $(,)?) => {

        paste!(
            /// Postgres error classes. See https://www.postgresql.org/docs/current/errcodes-appendix.html.
            #[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
            enum PgErrorClass {
                $(
                    #[doc=$doc]
                    [<$name:camel>],
                )*
                /// Unknown error class
                Other([u8; 2])
            }
        );

        impl PgErrorClass {
            paste!(
                pub const fn from_code(code: [u8; 2]) -> Self {
                    $(
                        const [<$name:upper>]: [u8; 2] = [stringify!($code).as_bytes()[0], stringify!($code).as_bytes()[1]];
                    )*

                    match code {
                        $(
                            [<$name:upper>] => Self::[<$name:camel>],
                        )*
                        _ => Self::Other(code)
                    }
                }
            );

            pub const fn to_code(self) -> [u8; 2] {
                match self {
                    $(
                        paste!(Self::[<$name:camel>]) => {
                            let s = stringify!($code).as_bytes();
                            [s[0], s[1]]
                        }
                    )*
                    Self::Other(code) => code,
                }
            }

            pub const fn get_class_string(&self) -> &'static str {
                match self {
                    $(
                        paste!(Self::[<$name:camel>]) => stringify!($name),
                    )*
                    Self::Other(..) => "other",
                }
            }
        }
    };
}

macro_rules! pg_error {
    ($(
        $class:ident {
            $(
                $($code_l:literal)? $($code_i:ident)? => $name:ident
            ),* $(,)?
        }
    ),* $(,)?) => {
        $(
            paste!(
                #[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
                pub enum [<PgError $class:camel>] {
                    $(
                        [<$name:camel>],
                    )*
                }

                impl [<PgError $class:camel>] {
                    pub const fn from_code(code: [u8; 3]) -> Option<Self> {
                        $(
                            const [<$name:upper>]: [u8; 3] = [stringify!($($code_i)? $($code_l)?).as_bytes()[0], stringify!($($code_i)? $($code_l)?).as_bytes()[1], stringify!($($code_i)? $($code_l)?).as_bytes()[2]];
                        )*

                        match code {
                            $(
                                [<$name:upper>] => Some(Self::[<$name:camel>]),
                            )*
                            _ => None
                        }
                    }

                    pub const fn to_code(self) -> [u8; 5] {
                        match self {
                            $(
                                Self::[<$name:camel>] => {
                                    let s = stringify!($($code_i)? $($code_l)?).as_bytes();
                                    let c = paste!(PgErrorClass::[<$class:camel>].to_code());
                                    [c[0], c[1], s[0], s[1], s[2]]
                                }
                            )*
                        }
                    }

                    pub const fn get_code_string(&self) -> &'static str {
                        match self {
                            $(
                                Self::[<$name:camel>] => {
                                    const CODE: [u8; 5] = [<PgError $class:camel>]::[<$name:camel>].to_code();
                                    match str::from_utf8(&CODE) {
                                        Ok(s) => s,
                                        _ => panic!()
                                    }
                                }
                            )*
                        }
                    }

                    pub const fn get_error_string(&self) -> &'static str {
                        match self {
                            $(
                                paste!(Self::[<$name:camel>]) => stringify!($name),
                            )*
                        }
                    }
                }
            );
        )*

        paste!(
            /// Postgres error codes. See <https://www.postgresql.org/docs/current/errcodes-appendix.html>.
            #[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
            pub enum PgError {
                $(
                    [<$class:camel>]([<PgError $class:camel>]),
                )*
                Other([u8; 5])
            }

            impl PgError {
                pub const fn from_code(code: [u8; 5]) -> Self {
                    match PgErrorClass::from_code([code[0], code[1]]) {
                        $(
                            PgErrorClass::[<$class:camel>] => {
                                if let Some(code) = [<PgError $class:camel>]::from_code([code[2], code[3], code[4]]) {
                                    Self::[<$class:camel>](code)
                                } else {
                                    Self::Other(code)
                                }
                            }
                        )*,
                        PgErrorClass::Other(..) => Self::Other(code)
                    }
                }

                pub const fn to_code(self) -> [u8; 5] {
                    match self {
                        $(
                            Self::[<$class:camel>](error) => {
                                error.to_code()
                            }
                        )*
                        Self::Other(code) => code,
                    }
                }

                pub const fn get_code_string(&self) -> &str {
                    match self {
                        $(
                            Self::[<$class:camel>](error) => error.get_code_string(),
                        )*
                        Self::Other(code) => match str::from_utf8(code) {
                            Ok(s) => s,
                            _ => ""
                        }
                    }
                }

                pub const fn get_error_string(self) -> &'static str {
                    match self {
                        $(
                            Self::[<$class:camel>](error) => error.get_error_string(),
                        )*
                        Self::Other(..) => "other",
                    }
                }
            }
        );
    };
}

impl std::fmt::Display for PgErrorClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = self.to_code();
        for &byte in &code {
            if byte.is_ascii() {
                write!(f, "{}", byte as char)?;
            } else {
                write!(f, "{{{:02X}}}", byte)?;
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for PgErrorClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let clazz = self.get_class_string();
        write!(f, "{clazz}({self})")?;
        Ok(())
    }
}

impl std::fmt::Display for PgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = self.to_code();
        for &byte in &code {
            if byte.is_ascii() {
                write!(f, "{}", byte as char)?;
            } else {
                write!(f, "{{{:02X}}}", byte)?;
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for PgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let clazz = self.get_error_string();
        write!(f, "{self}: {clazz}")?;
        Ok(())
    }
}

impl From<[u8; 5]> for PgError {
    fn from(code: [u8; 5]) -> Self {
        Self::from_code(code)
    }
}

impl From<PgError> for [u8; 5] {
    fn from(error: PgError) -> Self {
        error.to_code()
    }
}

#[derive(Debug)]
pub struct PgErrorParseError {
    kind: PgErrorParseErrorKind,
}

#[derive(Debug)]
pub enum PgErrorParseErrorKind {
    InvalidLength,
    InvalidCharacter,
}

impl FromStr for PgError {
    type Err = PgErrorParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 5 {
            return Err(PgErrorParseError {
                kind: PgErrorParseErrorKind::InvalidLength,
            });
        }

        let code = s.as_bytes();
        if !code.is_ascii() {
            return Err(PgErrorParseError {
                kind: PgErrorParseErrorKind::InvalidCharacter,
            });
        }

        Ok(PgError::from_code([
            code[0], code[1], code[2], code[3], code[4],
        ]))
    }
}

impl std::fmt::Display for PgErrorParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            PgErrorParseErrorKind::InvalidLength => write!(f, "Invalid PgError code length"),
            PgErrorParseErrorKind::InvalidCharacter => {
                write!(f, "Invalid character in PgError code")
            }
        }
    }
}

impl std::error::Error for PgErrorParseError {}

impl std::error::Error for PgError {}

/// A fully-qualified Postgres wire error message.
#[derive(Debug)]
pub struct PgServerError {
    pub code: PgError,
    pub severity: PgErrorSeverity,
    pub message: String,
    pub extra: HashMap<PgServerErrorField, String>,
}

impl PgServerError {
    pub fn new(
        code: PgError,
        arg: impl AsRef<str>,
        extra: HashMap<PgServerErrorField, String>,
    ) -> Self {
        Self {
            code,
            severity: PgErrorSeverity::Error,
            message: arg.as_ref().to_owned(),
            extra,
        }
    }

    /// Iterate all the fields of this error.
    pub fn fields(&self) -> impl Iterator<Item = (PgServerErrorField, &str)> {
        PgServerErrorFieldIterator::new(self)
    }
}

struct PgServerErrorBasicFieldIterator<'a> {
    error: &'a PgServerError,
    index: usize,
}

impl<'a> PgServerErrorBasicFieldIterator<'a> {
    fn new(error: &'a PgServerError) -> Self {
        Self { error, index: 0 }
    }
}

impl<'a> Iterator for PgServerErrorBasicFieldIterator<'a> {
    type Item = (PgServerErrorField, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        let result = match self.index {
            0 => Some((PgServerErrorField::Code, self.error.code.get_code_string())),
            1 => Some((PgServerErrorField::Message, self.error.message.as_str())),
            2 => Some((
                PgServerErrorField::SeverityNonLocalized,
                self.error.severity.as_ref(),
            )),
            _ => None,
        };
        self.index += 1;
        result
    }
}

#[allow(clippy::type_complexity)]
struct PgServerErrorFieldIterator<'a> {
    iter: std::iter::Chain<
        PgServerErrorBasicFieldIterator<'a>,
        std::iter::Map<
            std::collections::hash_map::Iter<'a, PgServerErrorField, String>,
            fn((&'a PgServerErrorField, &'a String)) -> (PgServerErrorField, &'a str),
        >,
    >,
}

impl<'a> PgServerErrorFieldIterator<'a> {
    pub fn new(error: &'a PgServerError) -> Self {
        let f: fn((&'a PgServerErrorField, &'a String)) -> (PgServerErrorField, &'a str) =
            |(f, e)| (*f, e.as_str());
        let a = PgServerErrorBasicFieldIterator::new(error);
        let b = error.extra.iter().map(f);
        let iter = Iterator::chain(a, b);
        Self { iter }
    }
}

impl<'a> Iterator for PgServerErrorFieldIterator<'a> {
    type Item = (PgServerErrorField, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl std::fmt::Display for PgServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Server error: {}: {}", self.code, self.message)
    }
}

impl std::error::Error for PgServerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.code)
    }
}

impl From<ErrorResponse<'_>> for PgServerError {
    fn from(error: ErrorResponse) -> Self {
        let mut code = String::new();
        let mut message = String::new();
        let mut extra = HashMap::new();
        let mut severity = PgErrorSeverity::Error;

        for field in error.fields() {
            let value = field.value().to_string_lossy().into_owned();
            match PgServerErrorField::try_from(field.etype()) {
                Ok(PgServerErrorField::Code) => code = value,
                Ok(PgServerErrorField::Message) => message = value,
                Ok(PgServerErrorField::SeverityNonLocalized) => {
                    severity = PgErrorSeverity::from_str(&value).unwrap_or_default()
                }
                Ok(field_type) => {
                    extra.insert(field_type, value);
                }
                Err(_) => {}
            }
        }

        // It's very unlikely the server will give us a non-five-character code
        let code = match PgError::from_str(&code) {
            Ok(code) => code,
            Err(_) => PgError::Other(*b"?????"),
        };

        PgServerError {
            code,
            severity,
            message,
            extra,
        }
    }
}

impl From<NoticeResponse<'_>> for PgServerError {
    fn from(error: NoticeResponse) -> Self {
        let mut code = String::new();
        let mut message = String::new();
        let mut extra = HashMap::new();
        let mut severity = PgErrorSeverity::Error;

        for field in error.fields() {
            let value = field.value().to_string_lossy().into_owned();
            match PgServerErrorField::try_from(field.ntype()) {
                Ok(PgServerErrorField::Code) => code = value,
                Ok(PgServerErrorField::Message) => message = value,
                Ok(PgServerErrorField::SeverityNonLocalized) => {
                    severity = PgErrorSeverity::from_str(&value).unwrap_or_default()
                }
                Ok(field_type) => {
                    extra.insert(field_type, value);
                }
                Err(_) => {}
            }
        }

        // It's very unlikely the server will give us a non-five-character code
        let code = match PgError::from_str(&code) {
            Ok(code) => code,
            Err(_) => PgError::Other(*b"?????"),
        };

        PgServerError {
            code,
            severity,
            message,
            extra,
        }
    }
}

/// Enum representing the field types in ErrorResponse and NoticeResponse messages.
///
/// See <https://www.postgresql.org/docs/current/protocol-error-fields.html>
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::TryFrom)]
#[try_from(repr)]
pub enum PgServerErrorField {
    /// Severity: ERROR, FATAL, PANIC, WARNING, NOTICE, DEBUG, INFO, or LOG
    Severity = b'S',
    /// Severity (non-localized): ERROR, FATAL, PANIC, WARNING, NOTICE, DEBUG, INFO, or LOG
    SeverityNonLocalized = b'V',
    /// SQLSTATE code for the error
    Code = b'C',
    /// Primary human-readable error message
    Message = b'M',
    /// Optional secondary error message with more detail
    Detail = b'D',
    /// Optional suggestion on how to resolve the problem
    Hint = b'H',
    /// Error cursor position as an index into the original query string
    Position = b'P',
    /// Internal position for internally generated commands
    InternalPosition = b'p',
    /// Text of a failed internally-generated command
    InternalQuery = b'q',
    /// Context in which the error occurred (e.g., call stack traceback)
    Where = b'W',
    /// Schema name associated with the error
    SchemaName = b's',
    /// Table name associated with the error
    TableName = b't',
    /// Column name associated with the error
    ColumnName = b'c',
    /// Data type name associated with the error
    DataTypeName = b'd',
    /// Constraint name associated with the error
    ConstraintName = b'n',
    /// Source-code file name where the error was reported
    File = b'F',
    /// Source-code line number where the error was reported
    Line = b'L',
    /// Source-code routine name reporting the error
    Routine = b'R',
}

/// Enum representing the severity levels of PostgreSQL errors and notices.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgErrorSeverity {
    #[default]
    Error,
    Fatal,
    Panic,
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

impl std::fmt::Display for PgErrorSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgErrorSeverity::Error => write!(f, "ERROR"),
            PgErrorSeverity::Fatal => write!(f, "FATAL"),
            PgErrorSeverity::Panic => write!(f, "PANIC"),
            PgErrorSeverity::Warning => write!(f, "WARNING"),
            PgErrorSeverity::Notice => write!(f, "NOTICE"),
            PgErrorSeverity::Debug => write!(f, "DEBUG"),
            PgErrorSeverity::Info => write!(f, "INFO"),
            PgErrorSeverity::Log => write!(f, "LOG"),
        }
    }
}
impl std::str::FromStr for PgErrorSeverity {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ERROR" => Ok(PgErrorSeverity::Error),
            "FATAL" => Ok(PgErrorSeverity::Fatal),
            "PANIC" => Ok(PgErrorSeverity::Panic),
            "WARNING" => Ok(PgErrorSeverity::Warning),
            "NOTICE" => Ok(PgErrorSeverity::Notice),
            "DEBUG" => Ok(PgErrorSeverity::Debug),
            "INFO" => Ok(PgErrorSeverity::Info),
            "LOG" => Ok(PgErrorSeverity::Log),
            _ => Err(()),
        }
    }
}

impl std::ops::Deref for PgErrorSeverity {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            PgErrorSeverity::Error => "ERROR",
            PgErrorSeverity::Fatal => "FATAL",
            PgErrorSeverity::Panic => "PANIC",
            PgErrorSeverity::Warning => "WARNING",
            PgErrorSeverity::Notice => "NOTICE",
            PgErrorSeverity::Debug => "DEBUG",
            PgErrorSeverity::Info => "INFO",
            PgErrorSeverity::Log => "LOG",
        }
    }
}

pg_error_class!(
    /// Successful Completion
    00 => successful_completion,
    /// Warning
    01 => warning,
    /// No Data (this is also a warning class per the SQL standard)
    02 => no_data,
    /// SQL Statement Not Yet Complete
    03 => sql_statement_not_yet_complete,
    /// Connection Exception
    08 => connection_exception,
    /// Triggered Action Exception
    09 => triggered_action_exception,
    /// Feature Not Supported
    0A => feature_not_supported,
    /// Invalid Transaction Initiation
    0B => invalid_transaction_initiation,
    /// Locator Exception
    0F => locator_exception,
    /// Invalid Grantor
    0L => invalid_grantor,
    /// Invalid Role Specification
    0P => invalid_role_specification,
    /// Diagnostics Exception
    0Z => diagnostics_exception,
    /// Case Not Found
    20 => case_not_found,
    /// Cardinality Violation
    21 => cardinality_violation,
    /// Data Exception
    22 => data_exception,
    /// Integrity Constraint Violation
    23 => integrity_constraint_violation,
    /// Invalid Cursor State
    24 => invalid_cursor_state,
    /// Invalid Transaction State
    25 => invalid_transaction_state,
    /// Invalid SQL Statement Name
    26 => invalid_sql_statement_name,
    /// Triggered Data Change Violation
    27 => triggered_data_change_violation,
    /// Invalid Authorization Specification
    28 => invalid_authorization_specification,
    /// Dependent Privilege Descriptors Still Exist
    2B => dependent_privilege_descriptors_still_exist,
    /// Invalid Transaction Termination
    2D => invalid_transaction_termination,
    /// SQL Routine Exception
    2F => sql_routine_exception,
    /// Invalid Cursor Name
    34 => invalid_cursor_name,
    /// External Routine Exception
    38 => external_routine_exception,
    /// External Routine Invocation Exception
    39 => external_routine_invocation_exception,
    /// Savepoint Exception
    3B => savepoint_exception,
    /// Invalid Catalog Name
    3D => invalid_catalog_name,
    /// Invalid Schema Name
    3F => invalid_schema_name,
    /// Transaction Rollback
    40 => transaction_rollback,
    /// Syntax Error or Access Rule Violation
    42 => syntax_error_or_access_rule_violation,
    /// WITH CHECK OPTION Violation
    44 => with_check_option_violation,
    /// Insufficient Resources
    53 => insufficient_resources,
    /// Program Limit Exceeded
    54 => program_limit_exceeded,
    /// Object Not In Prerequisite State
    55 => object_not_in_prerequisite_state,
    /// Operator Intervention
    57 => operator_intervention,
    /// System Error (errors external to PostgreSQL itself)
    58 => system_error,
    /// Configuration File Error
    F0 => config_file_error,
    /// Foreign Data Wrapper Error (SQL/MED)
    HV => fdw_error,
    /// PL/pgSQL Error
    P0 => plpgsql_error,
    /// Internal Error
    XX => internal_error
);

pg_error!(
    successful_completion {
        000 => successful_completion,
    },
    warning {
        000 => warning,
        00C => dynamic_result_sets_returned,
        008 => implicit_zero_bit_padding,
        003 => null_value_eliminated_in_set_function,
        007 => privilege_not_granted,
        006 => privilege_not_revoked,
        004 => string_data_right_truncation,
        P01 => deprecated_feature,
    },
    no_data {
        000 => no_data,
        001 => no_additional_dynamic_result_sets_returned,
    },
    sql_statement_not_yet_complete {
        000 => sql_statement_not_yet_complete,
    },
    connection_exception {
        000 => connection_exception,
        003 => connection_does_not_exist,
        006 => connection_failure,
        001 => sqlclient_unable_to_establish_sqlconnection,
        004 => sqlserver_rejected_establishment_of_sqlconnection,
        007 => transaction_resolution_unknown,
        P01 => protocol_violation,
    },
    triggered_action_exception {
        000 => triggered_action_exception,
    },
    feature_not_supported {
        000 => feature_not_supported,
    },
    invalid_transaction_initiation {
        000 => invalid_transaction_initiation,
    },
    locator_exception {
        000 => locator_exception,
        001 => invalid_locator_specification,
    },
    invalid_grantor {
        000 => invalid_grantor,
        P01 => invalid_grant_operation,
    },
    invalid_role_specification {
        000 => invalid_role_specification,
    },
    diagnostics_exception {
        000 => diagnostics_exception,
        002 => stacked_diagnostics_accessed_without_active_handler,
    },
    case_not_found {
        000 => case_not_found,
    },
    cardinality_violation {
        000 => cardinality_violation,
    },
    data_exception {
        000 => data_exception,
        "02E" => array_subscript_error,
        021 => character_not_in_repertoire,
        008 => datetime_field_overflow,
        012 => division_by_zero,
        005 => error_in_assignment,
        00B => escape_character_conflict,
        022 => indicator_overflow,
        015 => interval_field_overflow,
        "01E" => invalid_argument_for_logarithm,
        014 => invalid_argument_for_ntile_function,
        016 => invalid_argument_for_nth_value_function,
        01F => invalid_argument_for_power_function,
        01G => invalid_argument_for_width_bucket_function,
        018 => invalid_character_value_for_cast,
        007 => invalid_datetime_format,
        019 => invalid_escape_character,
        00D => invalid_escape_octet,
        025 => invalid_escape_sequence,
        P06 => nonstandard_use_of_escape_character,
        010 => invalid_indicator_parameter_value,
        023 => invalid_parameter_value,
        013 => invalid_preceding_or_following_size,
        01B => invalid_regular_expression,
        01W => invalid_row_count_in_limit_clause,
        01X => invalid_row_count_in_result_offset_clause,
        02H => invalid_tablesample_argument,
        02G => invalid_tablesample_repeat,
        009 => invalid_time_zone_displacement_value,
        00C => invalid_use_of_escape_character,
        00G => most_specific_type_mismatch,
        004 => null_value_not_allowed,
        002 => null_value_no_indicator_parameter,
        003 => numeric_value_out_of_range,
        00H => sequence_generator_limit_exceeded,
        026 => string_data_length_mismatch,
        001 => string_data_right_truncation,
        011 => substring_error,
        027 => trim_error,
        024 => unterminated_c_string,
        00F => zero_length_character_string,
        P01 => floating_point_exception,
        P02 => invalid_text_representation,
        P03 => invalid_binary_representation,
        P04 => bad_copy_file_format,
        P05 => untranslatable_character,
        00L => not_an_xml_document,
        00M => invalid_xml_document,
        00N => invalid_xml_content,
        00S => invalid_xml_comment,
        00T => invalid_xml_processing_instruction,
        030 => duplicate_json_object_key_value,
        031 => invalid_argument_for_sql_json_datetime_function,
        032 => invalid_json_text,
        033 => invalid_sql_json_subscript,
        034 => more_than_one_sql_json_item,
        035 => no_sql_json_item,
        036 => non_numeric_sql_json_item,
        037 => non_unique_keys_in_a_json_object,
        038 => singleton_sql_json_item_required,
        039 => sql_json_array_not_found,
        03A => sql_json_member_not_found,
        03B => sql_json_number_not_found,
        03C => sql_json_object_not_found,
        03D => too_many_json_array_elements,
        "03E" => too_many_json_object_members,
        03F => sql_json_scalar_required,
        03G => sql_json_item_cannot_be_cast_to_target_type,
    },
    integrity_constraint_violation {
        000 => integrity_constraint_violation,
        001 => restrict_violation,
        502 => not_null_violation,
        503 => foreign_key_violation,
        505 => unique_violation,
        514 => check_violation,
        P01 => exclusion_violation,
    },
    invalid_cursor_state {
        000 => invalid_cursor_state,
    },
    invalid_transaction_state {
        000 => invalid_transaction_state,
        001 => active_sql_transaction,
        002 => branch_transaction_already_active,
        008 => held_cursor_requires_same_isolation_level,
        003 => inappropriate_access_mode_for_branch_transaction,
        004 => inappropriate_isolation_level_for_branch_transaction,
        005 => no_active_sql_transaction_for_branch_transaction,
        006 => read_only_sql_transaction,
        007 => schema_and_data_statement_mixing_not_supported,
        P01 => no_active_sql_transaction,
        P02 => in_failed_sql_transaction,
        P03 => idle_in_transaction_session_timeout,
        P04 => transaction_timeout,
    },
    invalid_sql_statement_name {
        000 => invalid_sql_statement_name,
    },
    triggered_data_change_violation {
        000 => triggered_data_change_violation,
    },
    invalid_authorization_specification {
        000 => invalid_authorization_specification,
        P01 => invalid_password,
    },
    dependent_privilege_descriptors_still_exist {
        000 => dependent_privilege_descriptors_still_exist,
        P01 => dependent_objects_still_exist,
    },
    invalid_transaction_termination {
        000 => invalid_transaction_termination,
    },
    sql_routine_exception {
        000 => sql_routine_exception,
        005 => function_executed_no_return_statement,
        002 => modifying_sql_data_not_permitted,
        003 => prohibited_sql_statement_attempted,
        004 => reading_sql_data_not_permitted,
    },
    invalid_cursor_name {
        000 => invalid_cursor_name,
    },
    external_routine_exception {
        000 => external_routine_exception,
        001 => containing_sql_not_permitted,
        002 => modifying_sql_data_not_permitted,
        003 => prohibited_sql_statement_attempted,
        004 => reading_sql_data_not_permitted,
    },
    external_routine_invocation_exception {
        000 => external_routine_invocation_exception,
        001 => invalid_sqlstate_returned,
        004 => null_value_not_allowed,
        P01 => trigger_protocol_violated,
        P02 => srf_protocol_violated,
        P03 => event_trigger_protocol_violated,
    },
    savepoint_exception {
        000 => savepoint_exception,
        001 => invalid_savepoint_specification,
    },
    invalid_catalog_name {
        000 => invalid_catalog_name,
    },
    invalid_schema_name {
        000 => invalid_schema_name,
    },
    transaction_rollback {
        000 => transaction_rollback,
        002 => transaction_integrity_constraint_violation,
        001 => serialization_failure,
        003 => statement_completion_unknown,
        P01 => deadlock_detected,
    },
    syntax_error_or_access_rule_violation {
        000 => syntax_error_or_access_rule_violation,
        601 => syntax_error,
        501 => insufficient_privilege,
        846 => cannot_coerce,
        803 => grouping_error,
        P20 => windowing_error,
        P19 => invalid_recursion,
        830 => invalid_foreign_key,
        602 => invalid_name,
        622 => name_too_long,
        939 => reserved_name,
        804 => datatype_mismatch,
        P18 => indeterminate_datatype,
        P21 => collation_mismatch,
        P22 => indeterminate_collation,
        809 => wrong_object_type,
        8C9 => generated_always,
        703 => undefined_column,
        883 => undefined_function,
        P01 => undefined_table,
        P02 => undefined_parameter,
        704 => undefined_object,
        701 => duplicate_column,
        P03 => duplicate_cursor,
        P04 => duplicate_database,
        723 => duplicate_function,
        P05 => duplicate_prepared_statement,
        P06 => duplicate_schema,
        P07 => duplicate_table,
        712 => duplicate_alias,
        710 => duplicate_object,
        702 => ambiguous_column,
        725 => ambiguous_function,
        P08 => ambiguous_parameter,
        P09 => ambiguous_alias,
        P10 => invalid_column_reference,
        611 => invalid_column_definition,
        P11 => invalid_cursor_definition,
        P12 => invalid_database_definition,
        P13 => invalid_function_definition,
        P14 => invalid_prepared_statement_definition,
        P15 => invalid_schema_definition,
        P16 => invalid_table_definition,
        P17 => invalid_object_definition,
    },
    with_check_option_violation {
        000 => with_check_option_violation,
    },
    insufficient_resources {
        000 => insufficient_resources,
        100 => disk_full,
        200 => out_of_memory,
        300 => too_many_connections,
        400 => configuration_limit_exceeded,
    },
    program_limit_exceeded {
        000 => program_limit_exceeded,
        001 => statement_too_complex,
        011 => too_many_columns,
        023 => too_many_arguments,
    },
    object_not_in_prerequisite_state {
        000 => object_not_in_prerequisite_state,
        006 => object_in_use,
        P02 => cant_change_runtime_param,
        P03 => lock_not_available,
        P04 => unsafe_new_enum_value_usage,
    },
    operator_intervention {
        000 => operator_intervention,
        014 => query_canceled,
        P01 => admin_shutdown,
        P02 => crash_shutdown,
        P03 => cannot_connect_now,
        P04 => database_dropped,
        P05 => idle_session_timeout,
    },
    system_error {
        000 => system_error,
        030 => io_error,
        P01 => undefined_file,
        P02 => duplicate_file,
    },
    config_file_error {
        000 => config_file_error,
        001 => lock_file_exists,
    },
    fdw_error {
        000 => fdw_error,
        005 => fdw_column_name_not_found,
        002 => fdw_dynamic_parameter_value_needed,
        010 => fdw_function_sequence_error,
        021 => fdw_inconsistent_descriptor_information,
        024 => fdw_invalid_attribute_value,
        007 => fdw_invalid_column_name,
        008 => fdw_invalid_column_number,
        004 => fdw_invalid_data_type,
        006 => fdw_invalid_data_type_descriptors,
        091 => fdw_invalid_descriptor_field_identifier,
        00B => fdw_invalid_handle,
        00C => fdw_invalid_option_index,
        00D => fdw_invalid_option_name,
        090 => fdw_invalid_string_length_or_buffer_length,
        00A => fdw_invalid_string_format,
        009 => fdw_invalid_use_of_null_pointer,
        014 => fdw_too_many_handles,
        001 => fdw_out_of_memory,
        00P => fdw_no_schemas,
        00J => fdw_option_name_not_found,
        00K => fdw_reply_handle,
        00Q => fdw_schema_not_found,
        00R => fdw_table_not_found,
        00L => fdw_unable_to_create_execution,
        00M => fdw_unable_to_create_reply,
        00N => fdw_unable_to_establish_connection,
    },
    plpgsql_error {
        000 => plpgsql_error,
        001 => raise_exception,
        002 => no_data_found,
        003 => too_many_rows,
        004 => assert_failure,
    },
    internal_error {
        000 => internal_error,
        001 => data_corrupted,
        002 => index_corrupted,
    }
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_codes() {
        assert_eq!(PgError::from_code(*b"badco"), PgError::Other(*b"badco"));
        assert_eq!(
            PgError::from_code(*b"00000"),
            PgError::SuccessfulCompletion(PgErrorSuccessfulCompletion::SuccessfulCompletion)
        );
        assert_eq!(
            PgError::from_code(*b"22P04"),
            PgError::DataException(PgErrorDataException::BadCopyFileFormat)
        );
        assert_eq!(
            PgError::from_code(*b"2F003"),
            PgError::SqlRoutineException(
                PgErrorSqlRoutineException::ProhibitedSqlStatementAttempted
            )
        );
        assert_eq!(
            PgError::from_code(*b"XX002"),
            PgError::InternalError(PgErrorInternalError::IndexCorrupted)
        );
        assert_eq!(PgError::from_code(*b"XXXXX"), PgError::Other(*b"XXXXX"));

        assert_eq!(format!("{}", PgError::from_code(*b"badco")), "badco");
        assert_eq!(format!("{}", PgError::from_code(*b"00000")), "00000");
        assert_eq!(format!("{}", PgError::from_code(*b"22P04")), "22P04");
        assert_eq!(format!("{}", PgError::from_code(*b"2F003")), "2F003");
        assert_eq!(format!("{}", PgError::from_code(*b"XX002")), "XX002");
        assert_eq!(format!("{}", PgError::from_code(*b"XXXXX")), "XXXXX");

        assert_eq!(
            format!("{:?}", PgError::from_code(*b"badco")),
            "badco: other"
        );
        assert_eq!(
            format!("{:?}", PgError::from_code(*b"00000")),
            "00000: successful_completion"
        );
        assert_eq!(
            format!("{:?}", PgError::from_code(*b"22P04")),
            "22P04: bad_copy_file_format"
        );
        assert_eq!(
            format!("{:?}", PgError::from_code(*b"2F003")),
            "2F003: prohibited_sql_statement_attempted"
        );
        assert_eq!(
            format!("{:?}", PgError::from_code(*b"XX002")),
            "XX002: index_corrupted"
        );
        assert_eq!(
            format!("{:?}", PgError::from_code(*b"XXXXX")),
            "XXXXX: other"
        );
    }

    #[test]
    fn test_pg_server_error() {
        let error = PgServerError::new(
            PgError::from_code("28000".as_bytes().try_into().unwrap()),
            "message!",
            Default::default(),
        );
        let map: HashMap<_, _> = error.fields().collect();
        assert_eq!(
            map,
            HashMap::from([
                (PgServerErrorField::SeverityNonLocalized, "ERROR"),
                (PgServerErrorField::Message, "message!"),
                (PgServerErrorField::Code, "28000"),
            ])
        );
    }
}
