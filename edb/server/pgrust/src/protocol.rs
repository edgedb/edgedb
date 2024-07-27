use std::marker::PhantomData;

macro_rules! protocol {
    ($(
        struct $name:ident {
            $(
                #[ $doc:meta ] $field:ident : $type:ty $( = $value:literal)?
            ),*
            $(,)?
        }
    )*) => {
        mod struct_defs {
            $(
                protocol!{__one_struct__ struct $name {
                    $(
                        #[$doc] $field : $type $( = $value)?
                    ),*
                }}
            )*
        }

        $(
            #[allow(unused_imports)]
            pub use struct_defs::$name::$name;
        )*
    };

    (__one_struct__
        struct $name:ident {
            $(
                #[$doc:meta] $field:ident : $type:ty $( = $value:literal)?
            ),*
        }
    ) => {
        #[allow(non_snake_case)]
        pub mod $name {
            #[allow(unused)]
            use $crate::protocol::*;

            const FIELD_COUNT: usize = [$(stringify!($field)),*].len();

            #[allow(unused)]
            #[allow(non_camel_case_types)]
            #[derive(Eq, PartialEq)]
            #[repr(u8)]
            enum Fields {
                $(
                    $field,
                )*
            }

            #[allow(unused)]
            pub struct $name<'a> {
                buf: &'a [u8],
                fields: [usize; FIELD_COUNT + 1]
            }

            impl PartialEq for $name<'_> {
                fn eq(&self, other: &Self) -> bool {
                    self.buf.eq(other.buf)
                }
            }

            impl std::fmt::Debug for $name<'_> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    let mut s = f.debug_struct(stringify!($name));
                    s.field("buf", &self.buf);
                    s.finish()
                }
            }

            impl <'a> FieldAccess<$name<'a>> {
                pub const fn size_of_field_at( buf: &[u8]) -> usize {
                    let mut offset = 0;
                    $(
                        offset += FieldAccess::<$type>::size_of_field_at(buf.split_at(offset).1);
                    )*
                    offset
                }
                pub const fn extract(mut buf: &'a [u8]) -> $name<'a> {
                    $name::new(buf)
                }
            }

            impl <'a> FieldAccess<Array<'a, i16, $name<'a>>> {
                pub const fn size_of_field_at(mut buf: &[u8]) -> usize {
                    let mut size = std::mem::size_of::<i16>();
                    let mut len = FieldAccess::<i16>::extract(buf);
                    buf = buf.split_at(2).1;
                    loop {
                        if len == 0 {
                            break;
                        }
                        len -= 1;
                        let elem_size = FieldAccess::<$name>::size_of_field_at(buf);
                        buf = buf.split_at(elem_size).1;
                        size += elem_size;
                    }
                    size
                }
                pub const fn extract(mut buf: &'a [u8]) -> Array<'a, i16, $name<'a>> {
                    let len = FieldAccess::<i16>::extract(buf);
                    Array::new(buf.split_at(2).1, len as u32)
                }
            }

            impl <'a> FieldAccess<ZTArray<'a, $name<'a>>> {
                pub const fn size_of_field_at(mut buf: &[u8]) -> usize {
                    let mut size = 1;
                    loop {
                        if buf[0] == 0 {
                            return size;
                        }
                        let elem_size = FieldAccess::<$name>::size_of_field_at(buf);
                        buf = buf.split_at(elem_size).1;
                        size += elem_size;
                    }
                }
                pub const fn extract(mut buf: &'a [u8]) -> ZTArray<$name<'a>> {
                    ZTArray::new(buf)
                }
            }

            field_access!{'a $name<'a>}

            impl <'a> $name<'a> {
                pub const fn new(buf: &'a [u8]) -> Self{
                    let mut fields = [0; FIELD_COUNT + 1];
                    let mut offset = 0;
                    let mut index = 0;
                    $(
                        fields[index] = offset;
                        offset += FieldAccess::<$type>::size_of_field_at(buf.split_at(offset).1);
                        index += 1;
                    )*
                    fields[index] = offset;
                    
                    Self {
                        buf,
                        fields,
                    }
                }

                fn field_offset(buf: &[u8], field: Fields) -> usize {
                    let mut offset = 0;
                    $(
                        if field == Fields::$field {
                            return offset;
                        }
                        offset += FieldAccess::<$type>::size_of_field_at(buf.split_at(offset).1);
                    )*
                    unreachable!("{offset}")
                }

                $(
                    #[allow(unused)]
                    pub const fn $field(&self) -> $type {
                        let offset1 = self.fields[Fields::$field as usize];
                        let offset2 = self.fields[Fields::$field as usize + 1];
                        let (_, buf) = self.buf.split_at(offset1);
                        let (buf, _) = buf.split_at(offset2 - offset1);
                        FieldAccess::<$type>::extract(buf)
                    }
                )*
            }
        }
    };
}

/// Delegates to a concrete `FieldAccess` but as a non-const trait.
trait FieldAccessNonConst<'a, T: 'a> {
    fn size_of_field_at(buf: &[u8]) -> usize;
    fn extract(buf: &'a [u8]) -> T;
}

pub struct FieldAccess<T> {
    _phantom_data: PhantomData<T>,
}

macro_rules! field_access {
    ($lt:lifetime $ty:ty) => {
        impl <$lt> FieldAccessNonConst<$lt, $ty> for $ty {
            fn size_of_field_at(buf: &[u8]) -> usize {
                FieldAccess::<$ty>::size_of_field_at(buf)
            }
            fn extract(buf: &$lt [u8]) -> $ty {
                FieldAccess::<$ty>::extract(buf)
            }
        }
    };
}

macro_rules! basic_types {
    ($($ty:ty)*) => {
        $(
        field_access!{'a $ty}

        impl FieldAccess<$ty> {
            pub const fn size_of_field_at(_buf: &[u8]) -> usize {
                std::mem::size_of::<$ty>()
            }
            pub const fn extract(buf: &[u8]) -> $ty {
                if let Some(bytes) = buf.first_chunk() {
                    <$ty>::from_ne_bytes(*bytes)
                } else {
                    panic!()
                }
            }
        }

        impl <const S: usize> FieldAccess<[$ty; S]> {
            pub const fn size_of_field_at(_buf: &[u8]) -> usize {
                std::mem::size_of::<$ty>() * S
            }
            pub const fn extract(mut buf: &[u8]) -> [$ty; S] {
                let mut out: [$ty; S] = [0; S];
                let mut i = 0;
                loop {
                    if i == S {
                        break;
                    }
                    (out[i], buf) = if let Some((bytes, rest)) = buf.split_first_chunk() {
                        (<$ty>::from_ne_bytes(*bytes), rest)
                    } else {
                        panic!()
                    };
                    i += 1;
                }
                out
            }
        }

        impl <'a> FieldAccess<Array<'a, $ty, u8>> {
            pub const fn size_of_field_at(buf: &[u8]) -> usize {
                (buf[0] + 1) as _
            }
            pub const fn extract(mut buf: &[u8]) -> Array<$ty, u8> {
                unimplemented!()
            }
        }

        impl <'a> FieldAccess<Array<'a, $ty, i16>> {
            pub const fn size_of_field_at(buf: &[u8]) -> usize {
                let Some(len) = buf.split_first_chunk(std::mem::size_of::<i16>()) {
                    i16::from_ne_bytes(len) * std::mem::size_of::<i16>() + std::mem::size_of::<i16>()
                } else {
                    panic!()
                }
            }
            pub const fn extract(mut buf: &[u8]) -> Array<$ty, i16> {
                Array::new(buf, len)
            }
        }

        impl <'a> FieldAccess<Array<'a, $ty, i32>> {
            pub const fn size_of_field_at(_buf: &[u8]) -> usize {
                unimplemented!()
            }
            pub const fn extract(mut buf: &[u8]) -> Array<$ty, i32> {
                unimplemented!()
            }
        }

        impl <'a> FieldAccess<Array<'a, $ty, Encoded>> {
            pub const fn size_of_field_at(_buf: &[u8]) -> usize {
                unimplemented!()
            }
            pub const fn extract(mut buf: &[u8]) -> Array<$ty, Encoded> {
                unimplemented!()
            }
        }

        impl <'a> FieldAccess<Array<'a, $ty, ZTString<'a>>> {
            pub const fn size_of_field_at(buf: &[u8]) -> usize {
                unimplemented!()
            }
            pub const fn extract(mut buf: &'a [u8]) -> Array<$ty, ZTString> {
                unimplemented!()
            }
        }

        )*
    };
}

basic_types!(u8 i16 i32);

impl <'a> FieldAccess<Rest<'a>> {
    pub const fn size_of_field_at(buf: &[u8]) -> usize {
        buf.len()
    }
    pub const fn extract(buf: &[u8]) -> Rest {
        Rest { buf }
    }
}

impl <'a> FieldAccess<ZTString<'a>> {
    pub const fn size_of_field_at(buf: &[u8]) -> usize {
        let mut i = 0;
        loop {
            if buf[i] == 0 {
                return i + 1;
            }
            i += 1;
        }
    }
    pub const fn extract(buf: &[u8]) -> ZTString {
        let buf = buf.split_at(buf.len() - 1).0;
        ZTString { buf }
    }
}

field_access!{'a ZTString<'a>}
field_access!{'a Encoded}

impl FieldAccess<Encoded> {
    pub const fn size_of_field_at(_buf: &[u8]) -> usize {
        unimplemented!()
    }
    pub const fn extract(buf: &[u8]) -> Encoded {
        unimplemented!()
    }
}

impl <'a> FieldAccess<ZTArray<'a, ZTString<'a>>> {
    pub const fn size_of_field_at(buf: &[u8]) -> usize {
        unimplemented!()
    }
    pub const fn extract(buf: &'a [u8]) -> ZTArray<ZTString<'a>> {
        ZTArray::new(buf)
    }
}

pub struct ZTArray<'a, T: FieldAccessNonConst<'a, T> + 'a> {
    _phantom: PhantomData<T>,
    buf: &'a [u8]
}

impl <'a, T: FieldAccessNonConst<'a, T> + 'a> ZTArray<'a, T> {
    pub const fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            _phantom: PhantomData
        }
    }
}

pub struct ZTArrayIter<'a, T: FieldAccessNonConst<'a, T> + 'a> {
    _phantom: PhantomData<T>,
    buf: &'a [u8]
}

impl <'a, T: FieldAccessNonConst<'a, T> + 'a> IntoIterator for ZTArray<'a, T> {
    type Item = T;
    type IntoIter = ZTArrayIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        ZTArrayIter {
            _phantom: PhantomData,
            buf: self.buf
        }
    }
}

impl <'a, T: FieldAccessNonConst<'a, T> + 'a> IntoIterator for &ZTArray<'a, T> {
    type Item = T;
    type IntoIter = ZTArrayIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        ZTArrayIter {
            _phantom: PhantomData,
            buf: self.buf
        }
    }
}

impl <'a, T: FieldAccessNonConst<'a, T> + 'a> Iterator for ZTArrayIter<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.buf[0] == 0 {
            return None;
        }
        let (value, buf) = self.buf.split_at(T::size_of_field_at(&self.buf));
        self.buf = buf;
        Some(T::extract(value))
    }
}


pub struct Array<'a, L: 'static, T: FieldAccessNonConst<'a, T> + 'a> {
    _phantom: PhantomData<(L, T)>,
    buf: &'a [u8],
    len: u32
}

impl <'a, L, T: FieldAccessNonConst<'a, T> + 'a> Array<'a, L, T> {
    pub const fn new(buf: &'a [u8], len: u32) -> Self {
        Self {
            buf,
            _phantom: PhantomData,
            len
        }
    }

    pub const fn len(&self) -> usize {
        self.len as usize
    }
}

pub struct ArrayIter<'a, T: FieldAccessNonConst<'a, T> + 'a> {
    _phantom: PhantomData<T>,
    buf: &'a [u8],
    len: u32
}

impl <'a, L, T: FieldAccessNonConst<'a, T> + 'a> IntoIterator for Array<'a, L, T> {
    type Item = T;
    type IntoIter = ArrayIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        ArrayIter {
            _phantom: PhantomData,
            buf: self.buf,
            len: self.len
        }
    }
}

impl <'a, L, T: FieldAccessNonConst<'a, T> + 'a> IntoIterator for &Array<'a, L, T> {
    type Item = T;
    type IntoIter = ArrayIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        ArrayIter {
            _phantom: PhantomData,
            buf: self.buf,
            len: self.len
        }
    }
}

impl <'a, T: FieldAccessNonConst<'a, T> + 'a> Iterator for ArrayIter<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        let len = T::size_of_field_at(&self.buf);
        let (value, buf) = self.buf.split_at(len);
        self.buf = buf;
        Some(T::extract(value))
    }
}

#[allow(unused)]
pub struct Rest<'a> {
    buf: &'a [u8]
}

impl <'a> Rest<'a> {
    pub fn len(&self) -> usize {
        self.buf.len()
    }
}

#[allow(unused)]
pub struct ZTString<'a> {
    buf: &'a [u8]
}

impl std::fmt::Debug for ZTString<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        String::from_utf8_lossy(self.buf).fmt(f)
    }
}

impl <'a> ZTString<'a> {
    pub fn to_owned(&self) -> String {
        String::from_utf8(self.buf.to_owned()).unwrap()
    }
}

impl PartialEq for ZTString<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.buf == other.buf
    }
}
impl Eq for ZTString<'_> {}

impl PartialEq<str> for ZTString<'_> {
    fn eq(&self, other: &str) -> bool {
        self.buf == other.as_bytes()
    }
}

impl PartialEq<&str> for ZTString<'_> {
    fn eq(&self, other: &&str) -> bool {
        self.buf == other.as_bytes()
    }
}


#[allow(unused)]
pub struct Encoded {

}

// Some fields are at a known, fixed position. Other fields require us to decode previous fields.

protocol!{
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sasl_response() {
        let buf = [b'p', 5, 0, 0, 0, 2];
        let message = SASLResponse::new(&buf);
        assert_eq!(message.mlen(), 5);
        assert_eq!(message.response().len(), 1);
    }

    #[test]
    fn test_startup_message() {
        let buf = [
            5, 0, 0, 0, 
            0, 0x30, 0, 0, 
            b'a', 0, b'b', 0,
            b'c', 0, b'd', 0, 0];
        let message = StartupMessage::new(&buf);
        let arr = message.params();
        let mut vals = vec![];
        for entry in arr {
            vals.push(entry.name().to_owned());
            vals.push(entry.value().to_owned());
        }
        assert_eq!(vals, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn test_row_description() {
        let buf = [
            b'T',
            0, 0, 0, 0,
            2, 0, // # of fields
            b'f', b'1', 0,
            0, 0, 0, 0,
            0, 0,
            0, 0, 0, 0,
            0, 0,
            0, 0, 0, 0,
            0, 0,
            b'f', b'2', 0,
            0, 0, 0, 0,
            0, 0,
            0, 0, 0, 0,
            0, 0,
            0, 0, 0, 0,
            0, 0,
        ];
        let message = RowDescription::new(&buf);
        assert_eq!(message.fields().len(), 2);
        let mut iter = message.fields().into_iter();
        let f1 = iter.next().unwrap();
        assert_eq!(f1.name(), "f1");
        let f2 = iter.next().unwrap();
        assert_eq!(f2.name(), "f2");
        assert_eq!(None, iter.next());
    }
}
