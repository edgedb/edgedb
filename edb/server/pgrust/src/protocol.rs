#![allow(private_bounds)]
use std::marker::PhantomData;

trait TupleNest {
    type Nested;
}

impl TupleNest for () {
    type Nested = ();
}

trait TupleUnnest {
    type Unnested;
}

macro_rules! tuple_nest {
    () => {};
    ($first:ident $(,$tail:ident)*) => {
        tuple_nest!($($tail),*);

        impl <$first,$($tail),*> TupleNest for ($first,$($tail),*) {
            type Nested = ($first, <($($tail,)*) as TupleNest>::Nested);
        }
    };
}

tuple_nest!(A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z);

macro_rules! protocol {
    ($(
        struct $name:ident {
            $(
                #[ $doc:meta ] $field:ident : $type:tt $(< $($gen:tt),* >)? $( = $value:literal)?
            ),*
            $(,)?
        }
    )+) => {
        // The first phase of the macro adds lifetimes to the structs and then
        // calls __one_struct__ for each individual struct.
        #[allow(unused_parens)]
        mod struct_defs {
            $(
                protocol!{__one_struct__ 
                    struct $name <'a> {
                        $(
                            // Note that we pass type in parens to keep it as a
                            // token tree.
                            #[$doc] $field : ($type $(<$($gen),*>)?) $( = $value)?
                        ),*
                    }
                }
            )*
        }

        $(
            #[allow(unused_imports)]
            pub use struct_defs::$name::$name;
        )*
    };

    (__one_struct__
        struct $name:ident <$lt:lifetime> {
            $(
                // Type is parenthesized here but (T) is equivalent to T. This
                // allows us to keep it as a token tree and perform matches on
                // it.
                #[$doc:meta] $field:ident : $type:tt $( = $value:literal)?
            ),*
        }
    ) => {
        #[allow(non_snake_case)]
        pub mod $name {
            #[allow(unused)]
            use $crate::protocol::*;

            const FIELD_COUNT: usize = [$(stringify!($field)),*].len();

            // type FieldTypes<$lt> = ($(
            //     protocol!(__lifetime__ $lt $type)
            // ),*);

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
            pub struct $name<$lt> {
                buf: &$lt [u8],
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

            impl FieldAccess<$name<'_>> {
                #[inline]
                pub const fn size_of_field_at(buf: &[u8]) -> usize {
                    let mut offset = 0;
                    $(
                        offset += FieldAccess::<$type>::size_of_field_at(buf.split_at(offset).1);
                    )*
                    offset
                }
                #[inline(always)]
                pub const fn extract(buf: &[u8]) -> $name {
                    $name::new(buf)
                }
            }

            field_access!{'a $name<'a>}
            array_access!{'a $name<'a>}

            #[allow(unused)]
            impl <$lt> $name<$lt> {
                #[inline]
                pub const fn new(buf: &$lt [u8]) -> Self{
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

                // protocol!(__measure__ { $( $field : $type ; )* });

                $(
                    #[allow(unused)]
                    #[inline]
                    pub const fn $field<'s>(&'s self) -> protocol!(__lifetime__ $lt $type) where $lt: 's {
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
    (__measure__ { $( $field:ident : $type:ty ; )* }) => {
        // #[inline]
        // pub const fn measure<'s>(
        //     $( 
        //          $field : $type
        //     ),*
        // ) -> usize {
        //     let mut size = 0;
        //     $( size +=  FieldAccess::<$type>::measure($field); )
        //     size
        // }
    };
    (__lifetime__ $lt:lifetime ([$ty:ident; $count:literal])) => ([$ty; $count]);
    (__lifetime__ $lt:lifetime (u8)) => (u8);
    (__lifetime__ $lt:lifetime (i16)) => (i16);
    (__lifetime__ $lt:lifetime (i32)) => (i32);
    (__lifetime__ $lt:lifetime (ZTArray<$b:ty>)) => (ZTArray<$lt, protocol!(__lifetime__ $lt ($b))>);
    (__lifetime__ $lt:lifetime (Array<$a:ty, $b:ty>)) => (Array<$lt, $a, protocol!(__lifetime__ $lt ($b))>);
    (__lifetime__ $lt:lifetime ($ty:tt)) => ($ty);
    (__measure_param__ (ZTString)) => (&str);
    (__measure_param__ (Rest)) => (&[u8]);
    (__measure_param__ (Encoded)) => (&[u8]);
    (__measure_param__ (ZTArray<$b:tt>)) => (&[ protocol!(__measure_param__ ($b)) ]);
    (__measure_param__ (Array<$a:tt, $b:tt>)) => (&[ protocol!(__measure_param__ ($b)) ]);
    (__measure_param__ ($ty:ty)) => ($ty);
    // (__measure_param_any__ $field:ident (ZTString)) => (#[cfg()]);
    // (__measure_param_any__ $field:ident (Rest)) => (#[cfg()]);
    // (__measure_param_any__ $field:ident (Encoded)) => ();
    // (__measure_param_any__ $field:ident (ZTArray<$b:tt>)) => ();
    // (__measure_param_any__ $field:ident (Array<$a:tt, $b:tt>)) => ();
    (__measure_param_any__) => ("");
}


/// Delegates to a concrete `FieldAccess` but as a non-const trait.
trait FieldAccessNonConst<'a, T: 'a> {
    fn size_of_field_at(buf: &[u8]) -> usize;
    fn extract(buf: &'a [u8]) -> T;
}

/// This struct is specialized for each type we want to extract data from. We
/// have to do it this way to work around Rust's lack of specialization.
struct FieldAccess<T> {
    _phantom_data: PhantomData<T>,
}

macro_rules! field_access {
    ($lt:lifetime $ty:ty) => {
        impl <$lt> FieldAccessNonConst<$lt, $ty> for $ty {
            #[inline(always)]
            fn size_of_field_at(buf: &[u8]) -> usize {
                FieldAccess::<$ty>::size_of_field_at(buf)
            }
            #[inline(always)]
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
        impl <'a> FieldAccess<Array<'a, $ty, u8>> {
            #[inline(always)]
            pub const fn size_of_field_at(buf: &[u8]) -> usize {
                (buf[0] + 1) as _
            }
            #[inline(always)]
            pub const fn extract(mut buf: &[u8]) -> Array<$ty, u8> {
                Array::new(buf.split_at(1).1, (buf.len() - 1) as _)
            }
            // #[inline(always)]
            // pub const fn measure(buffer: &[u8]) -> usize {
            //     buffer.len() + std::mem::size_of::<$ty>()
            // }
        }

        #[allow(unused)]
        impl <'a> FieldAccess<Array<'a, $ty, i16>> {
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
        impl <'a> FieldAccess<Array<'a, $ty, i32>> {
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

impl <'a> FieldAccess<Rest<'a>> {
    #[inline(always)]
    pub const fn size_of_field_at(buf: &[u8]) -> usize {
        buf.len()
    }
    #[inline(always)]
    pub const fn extract(buf: &[u8]) -> Rest {
        Rest { buf }
    }
    // #[inline(always)]
    // pub const fn measure(data: &[u8]) -> usize {
    //     data.len()
    // }
}

impl <'a> FieldAccess<ZTString<'a>> {
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
    pub const fn extract(buf: &[u8]) -> ZTString {
        let buf = buf.split_at(buf.len() - 1).0;
        ZTString { buf }
    }
    // #[inline(always)]
    // pub const fn measure(data: &str) -> usize {
    //     data.len() + 1
    // }
}

impl <'a> FieldAccess<Encoded<'a>> {
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
    pub const fn extract(buf: &[u8]) -> Encoded {
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
    // #[inline(always)]
    // pub const fn measure(data: &[u8]) -> usize {
    //     data.len() + 4
    // }
}

field_access!{'a Rest<'a>}
field_access!{'a ZTString<'a>}
field_access!{'a Encoded<'a>}

macro_rules! array_access {
    ($lt:lifetime $ty:ty) => {
        array_access!($lt $ty | u8 i16 i32);
    };
    ($lt:lifetime $ty:ty | $($len:ty)*) => {
        $(
        #[allow(unused)]
        impl <$lt> FieldAccess<Array<$lt, $len, $ty>> {
            #[inline]
            pub const fn size_of_field_at(mut buf: &[u8]) -> usize {
                let mut size = std::mem::size_of::<$len>();
                let mut len = FieldAccess::<$len>::extract(buf);
                buf = buf.split_at(size).1;
                loop {
                    if len == 0 {
                        break;
                    }
                    len -= 1;
                    let elem_size = FieldAccess::<$ty>::size_of_field_at(buf);
                    buf = buf.split_at(elem_size).1;
                    size += elem_size;
                }
                size
            }
            #[inline(always)]
            pub const fn extract(buf: &$lt [u8]) -> Array<$lt, $len, $ty> {
                let len = FieldAccess::<$len>::extract(buf);
                Array::new(buf.split_at(std::mem::size_of::<$len>()).1, len as u32)
            }
        }
        )*

        #[allow(unused)]
        impl <$lt> FieldAccess<ZTArray<$lt, $ty>> {
            #[inline]
            pub const fn size_of_field_at(mut buf: &[u8]) -> usize {
                let mut size = 1;
                loop {
                    if buf[0] == 0 {
                        return size;
                    }
                    let elem_size = FieldAccess::<$ty>::size_of_field_at(buf);
                    buf = buf.split_at(elem_size).1;
                    size += elem_size;
                }
            }
            #[inline(always)]
            pub const fn extract(mut buf: &$lt [u8]) -> ZTArray<$lt, $ty> {
                ZTArray::new(buf)
            }
            // #[inline]
            // pub const fn measure(data: &[$ty]) -> usize {
            //     let mut size = 0;
            //     let mut index = 0;
            //     loop {
            //         unimplemented!();
            //         // size += FieldAccess::<$ty>::measure(data[index]);
            //         index += 1;
            //     }
            //     size
            // }
        }
    };
}

array_access!{'a ZTString<'a>}
array_access!{'a Encoded<'a>}

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


pub struct Encoded<'a> {
    buf: Option<&'a[u8]>
}

impl <'a> Encoded<'a> {
    pub const fn new(buf: Option<&'a[u8]>) -> Self {
        Self {
            buf
        }
    }
}

// Some fields are at a known, fixed position. Other fields require us to decode previous fields.

protocol!{
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
