use crate::{BufWriter, Enliven, Meta, ParseError};

/// As Rust does not currently support const in traits, we use this struct to
/// provide the const methods. It requires more awkward code, so we make use of
/// macros to generate the code.
///
/// Note that another consequence is that we have to declare this struct twice:
/// once for this crate, and again when someone tries to instantiate a protocol.
/// The reason for this is that we cannot add additional `impl`s for this `FieldAccess`
/// outside of this crate. Instead, we use a macro to "copy" the existing `impl`s from
/// this crate to the newtype.
pub struct FieldAccess<T: Enliven> {
    _phantom_data: std::marker::PhantomData<T>,
}

/// Delegates to a concrete [`FieldAccess`] but as a non-const trait. This is
/// used for performing extraction in iterators.
pub trait FieldAccessArray: Enliven {
    const META: &'static dyn Meta;
    fn size_of_field_at(buf: &[u8]) -> Result<usize, ParseError>;
    fn extract(buf: &[u8]) -> Result<<Self as Enliven>::WithLifetime<'_>, ParseError>;
    fn copy_to_buf(buf: &mut BufWriter, value: &Self::ForBuilder<'_>);
}

/// A trait for types which are fixed-size, used to provide a `get` implementation
/// in arrays and iterators.
pub trait FixedSize: Enliven {
    const SIZE: usize;
    /// Extract this type from the given buffer, assuming that enough bytes are available.
    fn extract_infallible(buf: &[u8]) -> <Self as Enliven>::WithLifetime<'_>;
}

/// Declares a field access for a given type which is variably-sized.
#[macro_export]
#[doc(hidden)]
macro_rules! declare_field_access {
    (
        Meta = $meta:ty,
        Inflated = $inflated:ty,
        Measure = $measured:ty,
        Builder = $builder:ty,

        pub const fn meta() -> &'static dyn Meta
            $meta_body:block

        pub const fn size_of_field_at($size_of_arg0:ident : &[u8]) -> Result<usize, ParseError>
            $size_of:block

        pub const fn extract($extract_arg0:ident : &[u8]) -> Result<$extract_ret:ty, ParseError>
           $extract:block

        pub const fn measure($measure_arg0:ident : &$measure_param:ty) -> usize
            $measure:block

        pub fn copy_to_buf($copy_arg0:ident : &mut BufWriter, $copy_arg1:ident : &$value_param:ty)
            $copy:block

        pub const fn constant($constant_arg0:ident : usize) -> $constant_ret:ty
            $constant:block
    ) => {
        impl Enliven for $meta {
            type WithLifetime<'a> = $inflated;
            type ForMeasure<'a> = $measured;
            type ForBuilder<'a> = $builder;
        }

        impl FieldAccess<$meta> {
            #[inline(always)]
            pub const fn meta() -> &'static dyn Meta {
                $meta_body
            }
            #[inline(always)]
            pub const fn size_of_field_at($size_of_arg0: &[u8]) -> Result<usize, ParseError> {
                $size_of
            }
            #[inline(always)]
            pub const fn extract($extract_arg0: &[u8]) -> Result<$extract_ret, ParseError> {
                $extract
            }
            #[inline(always)]
            pub const fn measure($measure_arg0: &$measure_param) -> usize {
                $measure
            }
            #[inline(always)]
            pub fn copy_to_buf($copy_arg0: &mut BufWriter, $copy_arg1: &$value_param) {
                $copy
            }
            #[inline(always)]
            pub const fn constant($constant_arg0: usize) -> $constant_ret {
                $constant
            }
        }

        $crate::field_access!($crate::FieldAccess, $meta);
        $crate::array_access!(variable, $crate::FieldAccess, $meta);
    };
}

/// Declares a field access for a given type which is fixed-size. Fixed-size
/// fields have simpler extraction logic, and support mapping to Rust arrays.
#[macro_export]
#[doc(hidden)]
macro_rules! declare_field_access_fixed_size {
    (
        Meta = $meta:ty,
        Inflated = $inflated:ty,
        Measure = $measured:ty,
        Builder = $builder:ty,
        Size = $size:expr,
        Zero = $zero:expr,

        pub const fn meta() -> &'static dyn Meta
            $meta_body:block

        pub const fn extract($extract_arg0:ident : &$extract_type:ty) -> Result<$extract_ret:ty, ParseError>
           $extract:block

        pub fn copy_to_buf($copy_arg0:ident : &mut BufWriter, $copy_arg1:ident : &$value_param:ty)
            $copy:block

        pub const fn constant($constant_arg0:ident : usize) -> $constant_ret:ty
            $constant:block
    ) => {
        impl Enliven for $meta {
            type WithLifetime<'a> = $inflated;
            type ForMeasure<'a> = $measured;
            type ForBuilder<'a> = $builder;
        }

        impl FieldAccess<$meta> {
            #[inline(always)]
            pub const fn meta() -> &'static dyn Meta {
                $meta_body
            }
            #[inline(always)]
            pub const fn size_of_field_at(buf: &[u8]) -> Result<usize, ParseError> {
                if let Ok(_) = Self::extract(buf) {
                    Ok($size)
                } else {
                    Err(ParseError::TooShort)
                }
            }
            #[inline(always)]
            pub const fn extract($extract_arg0: &[u8]) -> Result<$extract_ret, ParseError> {
                if let Some(chunk) = $extract_arg0.first_chunk() {
                    FieldAccess::<$meta>::extract_exact(chunk)
                } else {
                    Err(ParseError::TooShort)
                }
            }
            #[inline(always)]
            pub const fn extract_exact(
                $extract_arg0: &[u8; $size],
            ) -> Result<$extract_ret, ParseError> {
                $extract
            }
            #[inline(always)]
            pub const fn measure(_: &$measured) -> usize {
                $size
            }
            #[inline(always)]
            pub fn copy_to_buf($copy_arg0: &mut BufWriter, $copy_arg1: &$value_param) {
                $copy
            }
            #[inline(always)]
            pub const fn constant($constant_arg0: usize) -> $constant_ret {
                $constant
            }
        }

        impl $crate::FixedSize for $meta {
            const SIZE: usize = std::mem::size_of::<$inflated>();
            #[inline(always)]
            fn extract_infallible(buf: &[u8]) -> $inflated {
                FieldAccess::<$meta>::extract(buf).unwrap()
            }
        }

        impl<const S: usize> Enliven for $crate::meta::FixedArray<S, $meta> {
            type WithLifetime<'a> = [$inflated; S];
            type ForMeasure<'a> = [$measured; S];
            type ForBuilder<'a> = [$builder; S];
        }

        #[allow(unused)]
        impl<const S: usize> FieldAccess<$crate::meta::FixedArray<S, $meta>> {
            #[inline(always)]
            pub const fn meta() -> &'static dyn Meta {
                &$crate::meta::FixedArray::<S, $meta> {
                    _phantom: PhantomData,
                }
            }
            #[inline(always)]
            pub const fn size_of_field_at(buf: &[u8]) -> Result<usize, $crate::ParseError> {
                let size = $size * S;
                if size > buf.len() {
                    Err($crate::ParseError::TooShort)
                } else {
                    Ok(size)
                }
            }
            #[inline(always)]
            pub const fn measure(_: &[$measured; S]) -> usize {
                ($size * (S))
            }
            #[inline(always)]
            pub const fn extract(mut buf: &[u8]) -> Result<[$inflated; S], $crate::ParseError> {
                let mut out: [$inflated; S] = [const { $zero }; S];
                let mut i = 0;
                loop {
                    if i == S {
                        break;
                    }
                    (out[i], buf) = if let Some((bytes, rest)) = buf.split_first_chunk() {
                        match FieldAccess::<$meta>::extract_exact(bytes) {
                            Ok(value) => (value, rest),
                            Err(e) => return Err(e),
                        }
                    } else {
                        return Err($crate::ParseError::TooShort);
                    };
                    i += 1;
                }
                Ok(out)
            }
            #[inline(always)]
            pub fn copy_to_buf(mut buf: &mut BufWriter, value: &[$builder; S]) {
                if !buf.test(std::mem::size_of::<$builder>() * S) {
                    return;
                }
                for n in value {
                    FieldAccess::<$meta>::copy_to_buf(buf, n);
                }
            }
        }

        impl<const S: usize> FieldAccessArray for $crate::meta::FixedArray<S, $meta> {
            const META: &'static dyn Meta = FieldAccess::<$meta>::meta();
            #[inline(always)]
            fn size_of_field_at(buf: &[u8]) -> Result<usize, ParseError> {
                // TODO: needs to verify the values as well
                FieldAccess::<$meta>::size_of_field_at(buf).map(|size| size * S)
            }
            #[inline(always)]
            fn extract(mut buf: &[u8]) -> Result<[$inflated; S], ParseError> {
                let mut out = [$zero; S];
                for i in 0..S {
                    (out[i], buf) = if let Some((bytes, rest)) = buf.split_first_chunk() {
                        (FieldAccess::<$meta>::extract_exact(bytes)?, rest)
                    } else {
                        return Err(ParseError::TooShort);
                    };
                }
                Ok(out)
            }
            #[inline(always)]
            fn copy_to_buf(buf: &mut BufWriter, value: &[$builder; S]) {
                for n in value {
                    FieldAccess::<$meta>::copy_to_buf(buf, n);
                }
            }
        }

        $crate::field_access!($crate::FieldAccess, $meta);
        $crate::array_access!(fixed, $crate::FieldAccess, $meta);
    };
}

/// Delegate to the concrete [`FieldAccess`] for each type we want to extract.
#[macro_export]
#[doc(hidden)]
macro_rules! field_access {
    ($acc:ident :: FieldAccess, $ty:ty) => {
        impl $crate::FieldAccessArray for $ty {
            const META: &'static dyn $crate::Meta = $acc::FieldAccess::<$ty>::meta();
            #[inline(always)]
            fn size_of_field_at(buf: &[u8]) -> Result<usize, $crate::ParseError> {
                $acc::FieldAccess::<$ty>::size_of_field_at(buf)
            }
            #[inline(always)]
            fn extract(
                buf: &[u8],
            ) -> Result<<Self as $crate::Enliven>::WithLifetime<'_>, $crate::ParseError> {
                $acc::FieldAccess::<$ty>::extract(buf)
            }
            #[inline(always)]
            fn copy_to_buf(
                buf: &mut $crate::BufWriter,
                value: &<$ty as $crate::Enliven>::ForBuilder<'_>,
            ) {
                $acc::FieldAccess::<$ty>::copy_to_buf(buf, value)
            }
        }
    };
}

/// Define array accesses for inflated, strongly-typed arrays of both
/// zero-terminated and length-delimited types.
#[macro_export]
#[doc(hidden)]
macro_rules! array_access {
    (fixed, $acc:ident :: FieldAccess, $ty:ty) => {
        $crate::array_access!(fixed, $acc :: FieldAccess, $ty | u8 i16 u16 i32 u32);
    };
    (variable, $acc:ident :: FieldAccess, $ty:ty) => {
        $crate::array_access!(variable, $acc :: FieldAccess, $ty | u8 i16 u16 i32 u32);
    };
    (fixed, $acc:ident :: FieldAccess, $ty:ty | $($len:ty)*) => {
        $(
        #[allow(unused)]
        impl FieldAccess<$crate::meta::Array<$len, $ty>> {
            pub const fn meta() -> &'static dyn Meta {
                &$crate::meta::Array::<$len, $ty> { _phantom: PhantomData }
            }
            #[inline(always)]
            pub const fn size_of_field_at(buf: &[u8]) -> Result<usize, $crate::ParseError> {
                const N: usize = <$ty as $crate::FixedSize>::SIZE;
                const L: usize = std::mem::size_of::<$len>();
                if let Some(len) = buf.first_chunk::<L>() {
                    let len_value = <$len>::from_be_bytes(*len);
                    #[allow(unused_comparisons)]
                    if len_value < 0 {
                        return Err($crate::ParseError::InvalidData);
                    }
                    let mut byte_len = len_value as usize;
                    byte_len = match byte_len.checked_mul(N) {
                        Some(l) => l,
                        None => return Err($crate::ParseError::TooShort),
                    };
                    byte_len = match byte_len.checked_add(L) {
                        Some(l) => l,
                        None => return Err($crate::ParseError::TooShort),
                    };
                    if buf.len() < byte_len {
                        Err($crate::ParseError::TooShort)
                    } else {
                        Ok(byte_len)
                    }
                } else {
                    Err($crate::ParseError::TooShort)
                }
            }
            #[inline(always)]
            pub const fn extract(mut buf: &[u8]) -> Result<$crate::Array<$len, $ty>, $crate::ParseError> {
                const N: usize = <$ty as $crate::FixedSize>::SIZE;
                const L: usize = std::mem::size_of::<$len>();
                if let Some((len, array)) = buf.split_first_chunk::<L>() {
                    let len_value = <$len>::from_be_bytes(*len);
                    #[allow(unused_comparisons)]
                    if len_value < 0 {
                        return Err($crate::ParseError::InvalidData);
                    }
                    let mut byte_len = len_value as usize;
                    byte_len = match byte_len.checked_mul(N) {
                        Some(l) => l,
                        None => return Err($crate::ParseError::TooShort),
                    };
                    byte_len = match byte_len.checked_add(L) {
                        Some(l) => l,
                        None => return Err($crate::ParseError::TooShort),
                    };
                    if buf.len() < byte_len {
                        Err($crate::ParseError::TooShort)
                    } else {
                        Ok($crate::Array::new(array, len_value as u32))
                    }
                } else {
                    Err($crate::ParseError::TooShort)
                }
            }
            #[inline(always)]
            pub const fn measure<'a>(buffer: &'a[<$ty as $crate::Enliven>::ForMeasure<'a>]) -> usize {
                buffer.len() * std::mem::size_of::<$ty>() + std::mem::size_of::<$len>()
            }
            #[inline(always)]
            pub fn copy_to_buf<'a>(mut buf: &mut BufWriter, value: &'a[<$ty as $crate::Enliven>::ForBuilder<'a>]) {
                let size: usize = std::mem::size_of::<$ty>() * value.len() + std::mem::size_of::<$len>();
                if !buf.test(size) {
                    return;
                }
                buf.write(&<$len>::to_be_bytes(value.len() as _));
                for n in value {
                    $acc::FieldAccess::<$ty>::copy_to_buf(buf, n);
                }
            }
            #[inline(always)]
            pub const fn constant(value: usize) -> $crate::Array<'static, $len, $ty> {
                panic!("Constants unsupported for this data type")
            }
        }
        )*

        #[allow(unused)]
        impl $acc::FieldAccess<$crate::meta::ZTArray<$ty>> {
            pub const fn meta() -> &'static dyn $crate::Meta {
                &$crate::meta::ZTArray::<$ty> { _phantom: std::marker::PhantomData }
            }
            #[inline]
            pub const fn size_of_field_at(mut buf: &[u8]) -> Result<usize, $crate::ParseError> {
                let mut size = 1;
                loop {
                    if buf.is_empty() {
                        return Err($crate::ParseError::TooShort);
                    }
                    if buf[0] == 0 {
                        return Ok(size);
                    }
                    let elem_size = match $acc::FieldAccess::<$ty>::size_of_field_at(buf) {
                        Ok(n) => n,
                        Err(e) => return Err(e),
                    };
                    buf = buf.split_at(elem_size).1;
                    size += elem_size;
                }
            }
            #[inline(always)]
            pub const fn extract(mut buf: &[u8]) -> Result<$crate::ZTArray<$ty>, $crate::ParseError> {
                Ok($crate::ZTArray::new(buf))
            }
            #[inline]
            pub const fn measure<'a>(buffer: &'a[<$ty as $crate::Enliven>::ForMeasure<'a>]) -> usize {
                let mut size = 1;
                let mut index = 0;
                loop {
                    if index + 1 > buffer.len() {
                        break;
                    }
                    let item = &buffer[index];
                    size += $acc::FieldAccess::<$ty>::measure(item);
                    index += 1;
                }
                size
            }
            #[inline(always)]
            pub fn copy_to_buf(buf: &mut $crate::BufWriter, value: &[<$ty as $crate::Enliven>::ForBuilder<'_>]) {
                for elem in value {
                    $acc::FieldAccess::<$ty>::copy_to_buf(buf, elem);
                }
                buf.write_u8(0);
            }
            #[inline(always)]
            pub const fn constant(value: usize) -> $crate::ZTArray<'static, $ty> {
                panic!("Constants unsupported for this data type")
            }
        }
    };
    (variable, $acc:ident :: FieldAccess, $ty:ty | $($len:ty)*) => {
        $(
        #[allow(unused)]
        impl $acc::FieldAccess<$crate::meta::Array<$len, $ty>> {
            pub const fn meta() -> &'static dyn $crate::Meta {
                &$crate::meta::Array::<$len, $ty> { _phantom: std::marker::PhantomData }
            }
            #[inline]
            pub const fn size_of_field_at(mut buf: &[u8]) -> Result<usize, $crate::ParseError> {
                let mut size = std::mem::size_of::<$len>();
                let mut len = match $acc::FieldAccess::<$len>::extract(buf) {
                    Ok(n) => n,
                    Err(e) => return Err(e),
                };
                #[allow(unused_comparisons)]
                if len < 0 {
                    return Err($crate::ParseError::InvalidData);
                }
                buf = buf.split_at(size).1;
                loop {
                    if len <= 0 {
                        break;
                    }
                    len -= 1;
                    let elem_size = match $acc::FieldAccess::<$ty>::size_of_field_at(buf) {
                        Ok(n) => n,
                        Err(e) => return Err(e),
                    };
                    buf = buf.split_at(elem_size).1;
                    size += elem_size;
                }
                Ok(size)
            }
            #[inline(always)]
            pub const fn extract(buf: &[u8]) -> Result<$crate::Array<'_, $len, $ty>, $crate::ParseError> {
                match $acc::FieldAccess::<$len>::extract(buf) {
                    Ok(len) => Ok($crate::Array::new(buf.split_at(std::mem::size_of::<$len>()).1, len as u32)),
                    Err(e) => Err(e)
                }
            }
            #[inline]
            pub const fn measure<'a>(buffer: &'a[<$ty as $crate::Enliven>::ForMeasure<'a>]) -> usize {
                let mut size = std::mem::size_of::<$len>();
                let mut index = 0;
                loop {
                    if index + 1 > buffer.len() {
                        break;
                    }
                    let item = &buffer[index];
                    size += $acc::FieldAccess::<$ty>::measure(item);
                    index += 1;
                }
                size
            }
            #[inline(always)]
            pub fn copy_to_buf<'a>(buf: &mut $crate::BufWriter, value: &'a[<$ty as $crate::Enliven>::ForBuilder<'a>]) {
                buf.write(&<$len>::to_be_bytes(value.len() as _));
                for elem in value {
                    $acc::FieldAccess::<$ty>::copy_to_buf(buf, elem);
                }
            }
            #[inline(always)]
            pub const fn constant(value: usize) -> $crate::Array<'static, $len, $ty> {
                panic!("Constants unsupported for this data type")
            }
        }
        )*

        #[allow(unused)]
        impl $acc::FieldAccess<$crate::meta::ZTArray<$ty>> {
            pub const fn meta() -> &'static dyn $crate::Meta {
                &$crate::meta::ZTArray::<$ty> { _phantom: std::marker::PhantomData }
            }
            #[inline]
            pub const fn size_of_field_at(mut buf: &[u8]) -> Result<usize, $crate::ParseError> {
                let mut size = 1;
                loop {
                    if buf.is_empty() {
                        return Err($crate::ParseError::TooShort);
                    }
                    if buf[0] == 0 {
                        return Ok(size);
                    }
                    let elem_size = match $acc::FieldAccess::<$ty>::size_of_field_at(buf) {
                        Ok(n) => n,
                        Err(e) => return Err(e),
                    };
                    buf = buf.split_at(elem_size).1;
                    size += elem_size;
                }
            }
            #[inline(always)]
            pub const fn extract(mut buf: &[u8]) -> Result<$crate::ZTArray<$ty>, $crate::ParseError> {
                Ok($crate::ZTArray::new(buf))
            }
            #[inline]
            pub const fn measure<'a>(buffer: &'a[<$ty as $crate::Enliven>::ForMeasure<'a>]) -> usize {
                let mut size = 1;
                let mut index = 0;
                loop {
                    if index + 1 > buffer.len() {
                        break;
                    }
                    let item = &buffer[index];
                    size += $acc::FieldAccess::<$ty>::measure(item);
                    index += 1;
                }
                size
            }
            #[inline(always)]
            pub fn copy_to_buf(buf: &mut $crate::BufWriter, value: &[<$ty as $crate::Enliven>::ForBuilder<'_>]) {
                for elem in value {
                    $acc::FieldAccess::<$ty>::copy_to_buf(buf, elem);
                }
                buf.write_u8(0);
            }
            #[inline(always)]
            pub const fn constant(value: usize) -> $crate::ZTArray<'static, $ty> {
                panic!("Constants unsupported for this data type")
            }
        }
    };
}
