use crate::{BufWriter, Enliven, Meta, ParseError};

/// Delegates to a concrete [`FieldAccess`] but as a non-const trait. This is
/// used for performing extraction in iterators.
pub trait FieldAccessArray: Enliven {
    const META: &'static dyn Meta;
    fn size_of_field_at(buf: &[u8]) -> Result<usize, ParseError>;
    fn extract(buf: &[u8]) -> Result<<Self as Enliven>::WithLifetime<'_>, ParseError>;
    fn copy_to_buf(buf: &mut BufWriter, value: &Self::ForBuilder<'_>);
}

/// As Rust does not currently support const in traits, we use this struct to
/// provide the const methods. It requires more awkward code, so we make use of
/// macros to generate the code.
pub struct FieldAccess<T: Enliven> {
    _phantom_data: std::marker::PhantomData<T>,
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
        $crate::array_access!($crate::FieldAccess, $meta);
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
                struct Meta {}
                impl $crate::Meta for Meta {
                    fn name(&self) -> &'static str {
                        // TODO: can we extract this constant?
                        concat!('[', stringify!($ty), "; ", "S")
                    }
                }
                &Meta {}
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

        impl<const S: usize> FieldAccessArray for FixedArrayMeta<S, $meta> {
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
        $crate::array_access!($crate::FieldAccess, $meta);
    };
}
