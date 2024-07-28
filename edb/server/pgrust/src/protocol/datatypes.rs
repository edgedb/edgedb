use super::{arrays::{array_access, ArrayMeta, Array}, field_access, FieldAccess, Enliven};

pub mod meta {
    pub use super::RestMeta as Rest;
    pub use super::ZTStringMeta as ZTString;
    pub use super::EncodedMeta as Encoded;
}

/// Represents the remainder of data in a message.
#[allow(unused)]
pub struct Rest<'a> {
    buf: &'a [u8]
}

field_access!(RestMeta);

pub struct RestMeta {}
impl <'a> Enliven<'a> for RestMeta { type WithLifetime = Rest<'a>; }

impl <'a> Rest<'a> {
    pub fn len(&self) -> usize {
        self.buf.len()
    }
}

impl FieldAccess<RestMeta> {
    #[inline(always)]
    pub const fn size_of_field_at(buf: &[u8]) -> usize {
        buf.len()
    }
    #[inline(always)]
    pub const fn extract<'a>(buf: &'a [u8]) -> Rest<'a> {
        Rest { buf }
    }
}


/// A zero-terminated string.
#[allow(unused)]
pub struct ZTString<'a> {
    buf: &'a [u8]
}

field_access!(ZTStringMeta);
array_access!(ZTStringMeta);

pub struct ZTStringMeta {}
impl <'a> Enliven<'a> for ZTStringMeta { type WithLifetime = ZTString<'a>; }


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
    pub const fn extract<'a>(buf: &'a [u8]) -> ZTString<'a> {
        let buf = buf.split_at(buf.len() - 1).0;
        ZTString { buf }
    }
}


/// An encoded row value.
pub struct Encoded<'a> {
    buf: Option<&'a[u8]>
}

field_access!(EncodedMeta);
array_access!(EncodedMeta);

pub struct EncodedMeta {}
impl <'a> Enliven<'a> for EncodedMeta { type WithLifetime = Encoded<'a>; }

impl <'a> Encoded<'a> {
    pub const fn new(buf: Option<&'a[u8]>) -> Self {
        Self {
            buf
        }
    }
}

impl FieldAccess<EncodedMeta> {
    #[inline(always)]
    pub const fn size_of_field_at(buf: &[u8]) -> usize {
        const N: usize = std::mem::size_of::<i32>();
        if let Some(len) = buf.first_chunk::<N>() {
            let mut len = i32::from_ne_bytes(*len);
            if len == -1 {
                len = 0;
            }
            len as usize * N + N
        } else {
            panic!()
        }
    }
    #[inline(always)]
    pub const fn extract<'a>(buf: &'a [u8]) -> Encoded<'a> {
        const N: usize = std::mem::size_of::<i32>();
        if let Some((len, array)) = buf.split_first_chunk::<N>() {
            let len = i32::from_ne_bytes(*len);
            if len == -1 {
                Encoded::new(None)
            } else {
                Encoded::new(Some(array))
            }
        } else {
            panic!()
        }
    }
}


macro_rules! basic_types {
    ($($ty:ty)*) => {
        $(
        field_access!{$ty}

        impl <'a> Enliven<'a> for $ty {
            type WithLifetime = $ty;
        }

        impl <'a, const S: usize> Enliven<'a> for [$ty; S] {
            type WithLifetime = [$ty; S];
        }

        #[allow(unused)]
        impl FieldAccess<$ty> {
            #[inline(always)]
            pub const fn size_of_field_at(_: &[u8]) -> usize {
                std::mem::size_of::<$ty>()
            }
            #[inline(always)]
            pub const fn extract(buf: &[u8]) -> $ty {
                if let Some(bytes) = buf.first_chunk() {
                    <$ty>::from_ne_bytes(*bytes)
                } else {
                    panic!()
                }
            }
            // #[inline(always)]
            // pub const fn measure(_: $ty) -> usize {
            //     std::mem::size_of::<$ty>()
            // }
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
                        (<$ty>::from_ne_bytes(*bytes), rest)
                    } else {
                        panic!()
                    };
                    i += 1;
                }
                out
            }
            // #[inline(always)]
            // pub const fn measure(_: [$ty; S]) -> usize {
            //     std::mem::size_of::<$ty>() * S
            // }
        }

        #[allow(unused)]
        impl <'a> FieldAccess<ArrayMeta<$ty, u8>> {
            #[inline(always)]
            pub const fn size_of_field_at(buf: &[u8]) -> usize {
                (buf[0] + 1) as _
            }
            #[inline(always)]
            pub const fn extract(mut buf: &'a [u8]) -> Array<'a, $ty, u8> {
                Array::new(buf.split_at(1).1, (buf.len() - 1) as _)
            }
            // #[inline(always)]
            // pub const fn measure(buffer: &[u8]) -> usize {
            //     buffer.len() + std::mem::size_of::<$ty>()
            // }
        }

        #[allow(unused)]
        impl <'a> FieldAccess<ArrayMeta<$ty, i16>> {
            #[inline(always)]
            pub const fn size_of_field_at(buf: &[u8]) -> usize {
                const N: usize = std::mem::size_of::<i16>();
                if let Some(len) = buf.first_chunk::<N>() {
                    (i16::from_ne_bytes(*len) as usize * N + N)
                } else {
                    panic!()
                }
            }
            #[inline(always)]
            pub const fn extract(mut buf: &[u8]) -> Array<$ty, i16> {
                const N: usize = std::mem::size_of::<i16>();
                if let Some((len, array)) = buf.split_first_chunk::<N>() {
                    Array::new(array, i16::from_ne_bytes(*len) as u32)
                } else {
                    panic!()
                }
            }
            // #[inline(always)]
            // pub const fn measure(buffer: &[i16]) -> usize {
            //     buffer.len() * std::mem::size_of::<i16>() + std::mem::size_of::<$ty>()
            // }
        }

        #[allow(unused)]
        impl FieldAccess<ArrayMeta<$ty, i32>> {
            #[inline(always)]
            pub const fn size_of_field_at(buf: &[u8]) -> usize {
                const N: usize = std::mem::size_of::<i32>();
                if let Some(len) = buf.first_chunk::<N>() {
                    (i32::from_ne_bytes(*len) as usize * N + N)
                } else {
                    panic!()
                }
            }
            #[inline(always)]
            pub const fn extract(mut buf: &[u8]) -> Array<$ty, i32> {
                const N: usize = std::mem::size_of::<i32>();
                if let Some((len, array)) = buf.split_first_chunk::<N>() {
                    Array::new(array, i32::from_ne_bytes(*len) as u32)
                } else {
                    panic!()
                }
            }
            // #[inline(always)]
            // pub const fn measure(buffer: &[i32]) -> usize {
            //     buffer.len() * std::mem::size_of::<i32>() + std::mem::size_of::<$ty>()
            // }
        }

        )*
    };
}
basic_types!(u8 i16 i32);
