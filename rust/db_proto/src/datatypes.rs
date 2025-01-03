use std::{marker::PhantomData, str::Utf8Error};

pub use uuid::Uuid;

use crate::{
    declare_field_access, declare_field_access_fixed_size, writer::BufWriter, Enliven, FieldAccess,
    FieldAccessArray, Meta, ParseError,
};

pub mod meta {
    pub use super::BasicMeta as Basic;
    pub use super::EncodedMeta as Encoded;
    pub use super::LStringMeta as LString;
    pub use super::LengthMeta as Length;
    pub use super::RestMeta as Rest;
    pub use super::UuidMeta as Uuid;
    pub use super::ZTStringMeta as ZTString;
}

/// Represents the remainder of data in a message.
#[derive(Debug, PartialEq, Eq)]
pub struct Rest<'a> {
    buf: &'a [u8],
}

declare_field_access! {
    Meta = RestMeta,
    Inflated = Rest<'a>,
    Measure = &'a [u8],
    Builder = &'a [u8],

    pub const fn meta() -> &'static dyn Meta {
        &RestMeta {}
    }

    pub const fn size_of_field_at(buf: &[u8]) -> Result<usize, ParseError> {
        Ok(buf.len())
    }

    pub const fn extract(buf: &[u8]) -> Result<Rest<'_>, ParseError> {
        Ok(Rest { buf })
    }

    pub const fn measure(buf: &[u8]) -> usize {
        buf.len()
    }

    pub fn copy_to_buf(buf: &mut BufWriter, value: &[u8]) {
        buf.write(value)
    }

    pub const fn constant(_constant: usize) -> Rest<'static> {
        panic!("Constants unsupported for this data type")
    }
}

pub struct RestMeta {}
impl Meta for RestMeta {
    fn name(&self) -> &'static str {
        "Rest"
    }
}

impl<'a> Rest<'a> {}

impl<'a> AsRef<[u8]> for Rest<'a> {
    fn as_ref(&self) -> &[u8] {
        self.buf
    }
}

impl<'a> std::ops::Deref for Rest<'a> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.buf
    }
}

impl PartialEq<[u8]> for Rest<'_> {
    fn eq(&self, other: &[u8]) -> bool {
        self.buf == other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for Rest<'_> {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.buf == *other
    }
}

impl PartialEq<&[u8]> for Rest<'_> {
    fn eq(&self, other: &&[u8]) -> bool {
        self.buf == *other
    }
}

/// A zero-terminated string.
#[allow(unused)]
pub struct ZTString<'a> {
    buf: &'a [u8],
}

declare_field_access!(
    Meta = ZTStringMeta,
    Inflated = ZTString<'a>,
    Measure = &'a str,
    Builder = &'a str,

    pub const fn meta() -> &'static dyn Meta {
        &ZTStringMeta {}
    }

    pub const fn size_of_field_at(buf: &[u8]) -> Result<usize, ParseError> {
        let mut i = 0;
        loop {
            if i >= buf.len() {
                return Err(ParseError::TooShort);
            }
            if buf[i] == 0 {
                return Ok(i + 1);
            }
            i += 1;
        }
    }

    pub const fn extract(buf: &[u8]) -> Result<ZTString<'_>, ParseError> {
        let buf = buf.split_at(buf.len() - 1).0;
        Ok(ZTString { buf })
    }

    pub const fn measure(buf: &str) -> usize {
        buf.len() + 1
    }

    pub fn copy_to_buf(buf: &mut BufWriter, value: &str) {
        buf.write(value.as_bytes());
        buf.write_u8(0);
    }

    pub const fn constant(_constant: usize) -> ZTString<'static> {
        panic!("Constants unsupported for this data type")
    }
);

pub struct ZTStringMeta {}
impl Meta for ZTStringMeta {
    fn name(&self) -> &'static str {
        "ZTString"
    }
}

impl std::fmt::Debug for ZTString<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        String::from_utf8_lossy(self.buf).fmt(f)
    }
}

impl<'a> ZTString<'a> {
    pub fn to_owned(&self) -> Result<String, std::str::Utf8Error> {
        std::str::from_utf8(self.buf).map(|s| s.to_owned())
    }

    pub fn to_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.buf)
    }

    pub fn to_string_lossy(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(self.buf)
    }

    pub fn to_bytes(&self) -> &[u8] {
        self.buf
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

impl<'a> TryInto<&'a str> for ZTString<'a> {
    type Error = Utf8Error;
    fn try_into(self) -> Result<&'a str, Self::Error> {
        std::str::from_utf8(self.buf)
    }
}

/// A length-prefixed string.
#[allow(unused)]
pub struct LString<'a> {
    buf: &'a [u8],
}

declare_field_access!(
    Meta = LStringMeta,
    Inflated = LString<'a>,
    Measure = &'a str,
    Builder = &'a str,

    pub const fn meta() -> &'static dyn Meta {
        &LStringMeta {}
    }

    pub const fn size_of_field_at(buf: &[u8]) -> Result<usize, ParseError> {
        if buf.len() < 4 {
            return Err(ParseError::TooShort);
        }
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        Ok(4 + len)
    }

    pub const fn extract(buf: &[u8]) -> Result<LString<'_>, ParseError> {
        if buf.len() < 4 {
            return Err(ParseError::TooShort);
        }
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if buf.len() < 4 + len {
            return Err(ParseError::TooShort);
        }
        Ok(LString {
            buf: buf.split_at(4).1,
        })
    }

    pub const fn measure(buf: &str) -> usize {
        4 + buf.len()
    }

    pub fn copy_to_buf(buf: &mut BufWriter, value: &str) {
        let len = value.len() as u32;
        buf.write(&len.to_be_bytes());
        buf.write(value.as_bytes());
    }

    pub const fn constant(_constant: usize) -> LString<'static> {
        panic!("Constants unsupported for this data type")
    }
);

pub struct LStringMeta {}
impl Meta for LStringMeta {
    fn name(&self) -> &'static str {
        "LString"
    }
}

impl std::fmt::Debug for LString<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        String::from_utf8_lossy(self.buf).fmt(f)
    }
}

impl<'a> LString<'a> {
    pub fn to_owned(&self) -> Result<String, std::str::Utf8Error> {
        std::str::from_utf8(self.buf).map(|s| s.to_owned())
    }

    pub fn to_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.buf)
    }

    pub fn to_string_lossy(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(self.buf)
    }

    pub fn to_bytes(&self) -> &[u8] {
        self.buf
    }
}

impl PartialEq for LString<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.buf == other.buf
    }
}
impl Eq for LString<'_> {}

impl PartialEq<str> for LString<'_> {
    fn eq(&self, other: &str) -> bool {
        self.buf == other.as_bytes()
    }
}

impl PartialEq<&str> for LString<'_> {
    fn eq(&self, other: &&str) -> bool {
        self.buf == other.as_bytes()
    }
}

impl<'a> TryInto<&'a str> for LString<'a> {
    type Error = Utf8Error;
    fn try_into(self) -> Result<&'a str, Self::Error> {
        std::str::from_utf8(self.buf)
    }
}

declare_field_access_fixed_size! {
    Meta = UuidMeta,
    Inflated = Uuid,
    Measure = Uuid,
    Builder = Uuid,
    Size = 16,
    Zero = Uuid::nil(),

    pub const fn meta() -> &'static dyn Meta {
        &UuidMeta {}
    }

    pub const fn extract(buf: &[u8; 16]) -> Result<Uuid, ParseError> {
        Ok(Uuid::from_u128(<u128>::from_be_bytes(*buf)))
    }

    pub fn copy_to_buf(buf: &mut BufWriter, value: &Uuid) {
        buf.write(value.as_bytes().as_slice())
    }

    pub const fn constant(_constant: usize) -> Uuid {
        panic!("Constants unsupported for this data type")
    }
}

pub struct UuidMeta {}
impl Meta for UuidMeta {
    fn name(&self) -> &'static str {
        "Uuid"
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
/// An encoded row value.
pub enum Encoded<'a> {
    #[default]
    Null,
    Value(&'a [u8]),
}

impl<'a> Encoded<'a> {
    pub fn to_string_lossy(&self) -> std::borrow::Cow<'_, str> {
        match self {
            Encoded::Null => "".into(),
            Encoded::Value(value) => String::from_utf8_lossy(value),
        }
    }
}

impl<'a> AsRef<Encoded<'a>> for Encoded<'a> {
    fn as_ref(&self) -> &Encoded<'a> {
        self
    }
}

declare_field_access! {
    Meta = EncodedMeta,
    Inflated = Encoded<'a>,
    Measure = Encoded<'a>,
    Builder = Encoded<'a>,

    pub const fn meta() -> &'static dyn Meta {
        &EncodedMeta {}
    }

    pub const fn size_of_field_at(buf: &[u8]) -> Result<usize, ParseError> {
        const N: usize = std::mem::size_of::<i32>();
        if let Some(len) = buf.first_chunk::<N>() {
            let len = i32::from_be_bytes(*len);
            if len == -1 {
                Ok(N)
            } else if len < 0 {
                Err(ParseError::InvalidData)
            } else if buf.len() < len as usize + N {
                Err(ParseError::TooShort)
            } else {
                Ok(len as usize + N)
            }
        } else {
            Err(ParseError::TooShort)
        }
    }

    pub const fn extract(buf: &[u8]) -> Result<Encoded<'_>, ParseError> {
        const N: usize = std::mem::size_of::<i32>();
        if let Some((len, array)) = buf.split_first_chunk::<N>() {
            let len = i32::from_be_bytes(*len);
            if len == -1 && array.is_empty() {
                Ok(Encoded::Null)
            } else if len < 0 {
                Err(ParseError::InvalidData)
            } else if array.len() < len as _ {
                Err(ParseError::TooShort)
            } else {
                Ok(Encoded::Value(array))
            }
        } else {
            Err(ParseError::TooShort)
        }
    }

    pub const fn measure(value: &Encoded) -> usize {
        match value {
            Encoded::Null => std::mem::size_of::<i32>(),
            Encoded::Value(value) => value.len() + std::mem::size_of::<i32>(),
        }
    }

    pub fn copy_to_buf(buf: &mut BufWriter, value: &Encoded) {
        match value {
            Encoded::Null => buf.write(&[0xff, 0xff, 0xff, 0xff]),
            Encoded::Value(value) => {
                let len: i32 = value.len() as _;
                buf.write(&len.to_be_bytes());
                buf.write(value);
            }
        }
    }

    pub const fn constant(_constant: usize) -> Encoded<'static> {
        panic!("Constants unsupported for this data type")
    }
}

pub struct EncodedMeta {}
impl Meta for EncodedMeta {
    fn name(&self) -> &'static str {
        "Encoded"
    }
}

impl<'a> Encoded<'a> {}

impl PartialEq<str> for Encoded<'_> {
    fn eq(&self, other: &str) -> bool {
        self == &Encoded::Value(other.as_bytes())
    }
}

impl PartialEq<&str> for Encoded<'_> {
    fn eq(&self, other: &&str) -> bool {
        self == &Encoded::Value(other.as_bytes())
    }
}

impl PartialEq<[u8]> for Encoded<'_> {
    fn eq(&self, other: &[u8]) -> bool {
        self == &Encoded::Value(other)
    }
}

impl PartialEq<&[u8]> for Encoded<'_> {
    fn eq(&self, other: &&[u8]) -> bool {
        self == &Encoded::Value(other)
    }
}

pub struct Length(pub i32);

declare_field_access_fixed_size! {
    Meta = LengthMeta,
    Inflated = usize,
    Measure = i32,
    Builder = i32,
    Size = 4,
    Zero = 0,

    pub const fn meta() -> &'static dyn Meta {
        &LengthMeta {}
    }

    pub const fn extract(buf: &[u8; 4]) -> Result<usize, ParseError> {
        let n = i32::from_be_bytes(*buf);
        if n >= 0 {
            Ok(n as _)
        } else {
            Err(ParseError::InvalidData)
        }
    }

    pub fn copy_to_buf(buf: &mut BufWriter, value: &i32) {
        FieldAccess::<i32>::copy_to_buf(buf, value)
    }

    pub const fn constant(value: usize) -> usize {
        value
    }
}

impl FieldAccess<LengthMeta> {
    pub fn copy_to_buf_rewind(buf: &mut BufWriter, rewind: usize, value: usize) {
        buf.write_rewind(rewind, &(value as i32).to_be_bytes());
    }
}

// We alias usize here. Note that if this causes trouble in the future we can
// probably work around this by adding a new "const value" function to
// FieldAccess. For now it works!
pub struct LengthMeta {}

impl Meta for LengthMeta {
    fn name(&self) -> &'static str {
        "len"
    }
}

pub struct BasicMeta<T> {
    _phantom: PhantomData<T>,
}

impl<T> Meta for BasicMeta<T> {
    fn name(&self) -> &'static str {
        std::any::type_name::<T>()
    }
}

macro_rules! basic_types {
    ($($ty:ty),*) => {
        $(
        declare_field_access_fixed_size! {
            Meta = $ty,
            Inflated = $ty,
            Measure = $ty,
            Builder = $ty,
            Size = std::mem::size_of::<$ty>(),
            Zero = 0,

            pub const fn meta() -> &'static dyn Meta {
                &BasicMeta::<$ty> { _phantom: PhantomData }
            }

            pub const fn extract(buf: &[u8; std::mem::size_of::<$ty>()]) -> Result<$ty, ParseError> {
                Ok(<$ty>::from_be_bytes(*buf))
            }

            pub fn copy_to_buf(buf: &mut BufWriter, value: &$ty) {
                buf.write(&<$ty>::to_be_bytes(*value));
            }

            pub const fn constant(value: usize) -> $ty {
                value as _
            }
        }
        )*
    };
}

basic_types!(i8, u8, i16, u16, i32, u32, i64, u64, i128, u128);
