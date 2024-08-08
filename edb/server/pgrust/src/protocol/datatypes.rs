use super::{
    arrays::{array_access, Array, ArrayMeta},
    field_access,
    writer::BufWriter,
    Enliven, FieldAccess,
};

pub mod meta {
    pub use super::EncodedMeta as Encoded;
    pub use super::LengthMeta as Length;
    pub use super::RestMeta as Rest;
    pub use super::ZTStringMeta as ZTString;
}

/// Represents the remainder of data in a message.
#[derive(Debug, PartialEq, Eq)]
pub struct Rest<'a> {
    buf: &'a [u8],
}

field_access!(RestMeta);

pub struct RestMeta {}
impl<'a> Enliven<'a> for RestMeta {
    type WithLifetime = Rest<'a>;
    type ForMeasure = &'a [u8];
    type ForBuilder = &'a [u8];
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

impl FieldAccess<RestMeta> {
    #[inline(always)]
    pub const fn size_of_field_at(buf: &[u8]) -> usize {
        buf.len()
    }
    #[inline(always)]
    pub const fn extract(buf: &[u8]) -> Rest<'_> {
        Rest { buf }
    }
    #[inline(always)]
    pub const fn measure(buf: &[u8]) -> usize {
        buf.len()
    }
    #[inline(always)]
    pub fn copy_to_buf(buf: &mut BufWriter, value: &[u8]) {
        buf.write(value)
    }
}

/// A zero-terminated string.
#[allow(unused)]
pub struct ZTString<'a> {
    buf: &'a [u8],
}

field_access!(ZTStringMeta);
array_access!(ZTStringMeta);

pub struct ZTStringMeta {}
impl<'a> Enliven<'a> for ZTStringMeta {
    type WithLifetime = ZTString<'a>;
    type ForMeasure = &'a str;
    type ForBuilder = &'a str;
}

impl std::fmt::Debug for ZTString<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        String::from_utf8_lossy(self.buf).fmt(f)
    }
}

impl<'a> ZTString<'a> {
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

impl FieldAccess<ZTStringMeta> {
    #[inline(always)]
    pub const fn size_of_field_at(buf: &[u8]) -> usize {
        let mut i = 0;
        loop {
            if buf[i] == 0 {
                return i + 1;
            }
            i += 1;
        }
    }
    #[inline(always)]
    pub const fn extract(buf: &[u8]) -> ZTString<'_> {
        let buf = buf.split_at(buf.len() - 1).0;
        ZTString { buf }
    }
    #[inline(always)]
    pub const fn measure(buf: &str) -> usize {
        buf.len() + 1
    }
    #[inline(always)]
    pub fn copy_to_buf(buf: &mut BufWriter, value: &str) {
        buf.write(value.as_bytes());
        buf.write_u8(0);
    }
}

/// An encoded row value.
pub struct Encoded<'a> {
    buf: Option<&'a [u8]>,
}

field_access!(EncodedMeta);
array_access!(EncodedMeta);

pub struct EncodedMeta {}
impl<'a> Enliven<'a> for EncodedMeta {
    type WithLifetime = Encoded<'a>;
    type ForMeasure = &'a [u8];
    type ForBuilder = &'a [u8];
}

impl<'a> Encoded<'a> {
    pub const fn new(buf: Option<&'a [u8]>) -> Self {
        Self { buf }
    }
}

impl FieldAccess<EncodedMeta> {
    #[inline(always)]
    pub const fn size_of_field_at(buf: &[u8]) -> usize {
        const N: usize = std::mem::size_of::<i32>();
        if let Some(len) = buf.first_chunk::<N>() {
            let mut len = i32::from_be_bytes(*len);
            if len == -1 {
                len = 0;
            }
            len as usize * N + N
        } else {
            panic!()
        }
    }
    #[inline(always)]
    pub const fn extract(buf: &[u8]) -> Encoded<'_> {
        const N: usize = std::mem::size_of::<i32>();
        if let Some((len, array)) = buf.split_first_chunk::<N>() {
            let len = i32::from_be_bytes(*len);
            if len == -1 {
                Encoded::new(None)
            } else {
                Encoded::new(Some(array))
            }
        } else {
            panic!()
        }
    }
    #[inline(always)]
    pub const fn measure(_: &[u8]) -> usize {
        unimplemented!()
    }
    #[inline(always)]
    pub fn copy_to_buf(_: &mut BufWriter, _: &[u8]) -> Result<usize, usize> {
        unimplemented!()
    }
}

// We alias usize here. Note that if this causes trouble in the future we can
// probably work around this by adding a new "const value" function to
// FieldAccess. For now it works!
pub struct LengthMeta(#[allow(unused)] i32);
impl<'a> Enliven<'a> for LengthMeta {
    type WithLifetime = usize;
    type ForMeasure = usize;
    type ForBuilder = usize;
}

impl FieldAccess<LengthMeta> {
    #[inline(always)]
    pub const fn constant(value: usize) -> LengthMeta {
        LengthMeta(value as i32)
    }
    #[inline(always)]
    pub const fn size_of_field_at(buf: &[u8]) -> usize {
        FieldAccess::<i32>::size_of_field_at(buf)
    }
    #[inline(always)]
    pub const fn extract(buf: &[u8]) -> usize {
        FieldAccess::<i32>::extract(buf) as _
    }
    #[inline(always)]
    pub const fn measure(value: usize) -> usize {
        FieldAccess::<i32>::measure(value as i32)
    }
    #[inline(always)]
    pub fn copy_to_buf(buf: &mut BufWriter, value: usize) {
        FieldAccess::<i32>::copy_to_buf(buf, value as i32)
    }
    #[inline(always)]
    pub fn copy_to_buf_rewind(buf: &mut BufWriter, rewind: usize, value: usize) {
        FieldAccess::<i32>::copy_to_buf_rewind(buf, rewind, value as i32)
    }
}

macro_rules! basic_types {
    ($($ty:ty)*) => {
        $(
        field_access!{$ty}

        impl <'a> Enliven<'a> for $ty {
            type WithLifetime = $ty;
            type ForMeasure = $ty;
            type ForBuilder = $ty;
        }

        impl <'a, const S: usize> Enliven<'a> for [$ty; S] {
            type WithLifetime = [$ty; S];
            type ForMeasure = [$ty; S];
            type ForBuilder = [$ty; S];
        }

        #[allow(unused)]
        impl FieldAccess<$ty> {
            #[inline(always)]
            pub const fn constant(value: usize) -> $ty {
                value as _
            }
            #[inline(always)]
            pub const fn size_of_field_at(_: &[u8]) -> usize {
                std::mem::size_of::<$ty>()
            }
            #[inline(always)]
            pub const fn extract(buf: &[u8]) -> $ty {
                if let Some(bytes) = buf.first_chunk() {
                    <$ty>::from_be_bytes(*bytes)
                } else {
                    panic!()
                }
            }
            #[inline(always)]
            pub const fn measure(_: $ty) -> usize {
                std::mem::size_of::<$ty>()
            }
            #[inline(always)]
            pub fn copy_to_buf(buf: &mut BufWriter, value: $ty) {
                buf.write(&<$ty>::to_be_bytes(value));
            }
            #[inline(always)]
            pub fn copy_to_buf_rewind(buf: &mut BufWriter, rewind: usize, value: $ty) {
                buf.write_rewind(rewind, &<$ty>::to_be_bytes(value));
            }
        }

        #[allow(unused)]
        impl <const S: usize> FieldAccess<[$ty; S]> {
            #[inline(always)]
            pub const fn size_of_field_at(_buf: &[u8]) -> usize {
                std::mem::size_of::<$ty>() * S
            }
            #[inline(always)]
            pub const fn extract(mut buf: &[u8]) -> [$ty; S] {
                let mut out: [$ty; S] = [0; S];
                let mut i = 0;
                loop {
                    if i == S {
                        break;
                    }
                    (out[i], buf) = if let Some((bytes, rest)) = buf.split_first_chunk() {
                        (<$ty>::from_be_bytes(*bytes), rest)
                    } else {
                        panic!()
                    };
                    i += 1;
                }
                out
            }
            #[inline(always)]
            pub const fn measure(_: [$ty; S]) -> usize {
                std::mem::size_of::<$ty>() * S
            }
            #[inline(always)]
            pub fn copy_to_buf(mut buf: &mut BufWriter, value: [$ty; S]) {
                if !buf.test(std::mem::size_of::<$ty>() * S) {
                    return;
                }
                for n in value {
                    buf.write(&<$ty>::to_be_bytes(n));
                }
            }
        }

        basic_types!(: array<$ty> u8 i16 i32);
        )*
    };

    (: array<$ty:ty> $($len:ty)*) => {
        $(
            #[allow(unused)]
            impl FieldAccess<ArrayMeta<$len, $ty>> {
                #[inline(always)]
                pub const fn size_of_field_at(buf: &[u8]) -> usize {
                    const N: usize = std::mem::size_of::<$ty>();
                    const L: usize = std::mem::size_of::<$len>();
                    if let Some(len) = buf.first_chunk::<L>() {
                        (<$len>::from_be_bytes(*len) as usize * N + L)
                    } else {
                        panic!()
                    }
                }
                #[inline(always)]
                pub const fn extract(mut buf: &[u8]) -> Array<$len, $ty> {
                    const N: usize = std::mem::size_of::<$ty>();
                    const L: usize = std::mem::size_of::<$len>();
                    if let Some((len, array)) = buf.split_first_chunk::<L>() {
                        Array::new(array, <$len>::from_be_bytes(*len) as u32)
                    } else {
                        panic!()
                    }
                }
                #[inline(always)]
                pub const fn measure(buffer: &[$ty]) -> usize {
                    buffer.len() * std::mem::size_of::<$ty>() + std::mem::size_of::<$len>()
                }
                #[inline(always)]
                pub fn copy_to_buf(mut buf: &mut BufWriter, value: &[$ty]) {
                    let size: usize = std::mem::size_of::<$ty>() * value.len() + std::mem::size_of::<$len>();
                    if !buf.test(size) {
                        return;
                    }
                    buf.write(&<$len>::to_be_bytes(value.len() as _));
                    for n in value {
                        buf.write(&<$ty>::to_be_bytes(*n));
                    }
                }
            }
        )*
    }
}
basic_types!(u8 i16 i32);
