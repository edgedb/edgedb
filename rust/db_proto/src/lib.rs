mod arrays;
mod buffer;
mod datatypes;
mod field_access;
mod gen;
mod message_group;
mod writer;

#[doc(hidden)]
pub mod test_protocol;

/// Metatypes for the protocol and related arrays/strings.
pub mod meta {
    pub use super::arrays::meta::*;
    pub use super::datatypes::meta::*;
}

#[allow(unused)]
pub use arrays::{Array, ArrayIter, ZTArray, ZTArrayIter};
pub use buffer::StructBuffer;
#[allow(unused)]
pub use datatypes::{Encoded, LString, Length, Rest, Uuid, ZTString};
pub use field_access::{FieldAccess, FieldAccessArray, FixedSize};
pub use writer::BufWriter;

#[doc(inline)]
pub use gen::protocol;
#[doc(inline)]
pub use message_group::{match_message, message_group};

/// Re-export for the `protocol!` macro.
#[doc(hidden)]
pub use paste::paste;

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    #[error("Buffer is too short")]
    TooShort,
    #[error("Invalid data")]
    InvalidData,
}

/// Implemented for all structs.
pub trait StructMeta {
    type Struct<'a>: std::fmt::Debug;
    fn new(buf: &[u8]) -> Result<Self::Struct<'_>, ParseError>;
    fn to_vec(s: &Self::Struct<'_>) -> Vec<u8>;
}

/// Implemented for all generated structs that have a [`meta::Length`] field at a fixed offset.
pub trait StructLength: StructMeta {
    fn length_field_of(of: &Self::Struct<'_>) -> usize;
    fn length_field_offset() -> usize;
    fn length_of_buf(buf: &[u8]) -> Option<usize> {
        if buf.len() < Self::length_field_offset() + std::mem::size_of::<u32>() {
            None
        } else {
            let len = FieldAccess::<datatypes::LengthMeta>::extract(
                &buf[Self::length_field_offset()
                    ..Self::length_field_offset() + std::mem::size_of::<u32>()],
            )
            .ok()?;
            Some(Self::length_field_offset() + len)
        }
    }
}

/// For a given metaclass, returns the inflated type, a measurement type and a
/// builder type.
///
/// Types that don't include a lifetime can use the same type for the meta type
/// and the `WithLifetime` type.
pub trait Enliven {
    type WithLifetime<'a>;
    type ForMeasure<'a>: 'a;
    type ForBuilder<'a>: 'a;
}

#[derive(Debug, Eq, PartialEq)]
pub enum MetaRelation {
    Parent,
    Length,
    Item,
    Field(&'static str),
}

pub trait Meta {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
    fn relations(&self) -> &'static [(MetaRelation, &'static dyn Meta)] {
        &[]
    }
    fn fixed_length(&self) -> Option<usize> {
        None
    }
    fn field(&self, name: &'static str) -> Option<&'static dyn Meta> {
        for (relation, meta) in self.relations() {
            if relation == &MetaRelation::Field(name) {
                return Some(*meta);
            }
        }
        None
    }
    fn parent(&self) -> Option<&'static dyn Meta> {
        for (relation, meta) in self.relations() {
            if relation == &MetaRelation::Parent {
                return Some(*meta);
            }
        }
        None
    }
}

impl<T: Meta> PartialEq<T> for dyn Meta {
    fn eq(&self, other: &T) -> bool {
        other.name() == self.name()
    }
}

impl std::fmt::Debug for dyn Meta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct(self.name());
        if let Some(length) = self.fixed_length() {
            s.field("Length", &length);
        }
        for (relation, meta) in self.relations() {
            if relation == &MetaRelation::Parent {
                s.field(&format!("{relation:?}"), &meta.name());
            } else {
                s.field(&format!("{relation:?}"), meta);
            }
        }
        s.finish()
    }
}

/// Used internally by the `protocol!` macro to copy from `FieldAccess` in this crate to
/// `FieldAccess` in the generated code.
#[macro_export]
#[doc(hidden)]
macro_rules! field_access_copy {
    ($acc1:ident :: FieldAccess, $acc2:ident :: FieldAccess, $($ty:ty),*) => {
        $(
            $crate::field_access_copy!(: $acc1 :: FieldAccess, $acc2 :: FieldAccess,
                $ty,
                $crate::meta::ZTArray<$ty>,
                $crate::meta::Array<u8, $ty>,
                $crate::meta::Array<i16, $ty>,
                $crate::meta::Array<u16, $ty>,
                $crate::meta::Array<i32, $ty>,
                $crate::meta::Array<u32, $ty>
            );
        )*
    };

    (basic $acc1:ident :: FieldAccess, $acc2:ident :: FieldAccess, $($ty:ty),*) => {
        $(

        $crate::field_access_copy!(: $acc1 :: FieldAccess, $acc2 :: FieldAccess,
            $ty,
            $crate::meta::ZTArray<$ty>,
            $crate::meta::Array<u8, $ty>,
            $crate::meta::Array<i16, $ty>,
            $crate::meta::Array<u16, $ty>,
            $crate::meta::Array<i32, $ty>,
            $crate::meta::Array<u32, $ty>
        );

        impl <const S: usize> $acc2 :: FieldAccess<$crate::meta::FixedArray<S, $ty>> {
            #[inline(always)]
            pub const fn meta() -> &'static dyn $crate::Meta {
                $acc1::FieldAccess::<$crate::meta::FixedArray<S, $ty>>::meta()
            }
            #[inline(always)]
            pub const fn size_of_field_at(buf: &[u8]) -> Result<usize, $crate::ParseError> {
                $acc1::FieldAccess::<$crate::meta::FixedArray<S, $ty>>::size_of_field_at(buf)
            }
            #[inline(always)]
            pub const fn extract(buf: &[u8]) -> Result<[<$ty as $crate::Enliven>::WithLifetime<'_>; S], $crate::ParseError> {
                $acc1::FieldAccess::<$crate::meta::FixedArray<S, $ty>>::extract(buf)
            }
            pub const fn constant(_: usize) -> $ty {
                panic!("Constants unsupported for this data type")
            }
            #[inline(always)]
            pub const fn measure(value: &[<$ty as $crate::Enliven>::ForMeasure<'_>; S]) -> usize {
                $acc1::FieldAccess::<$crate::meta::FixedArray<S, $ty>>::measure(value)
            }
        }
        )*
    };
    (: $acc1:ident :: FieldAccess, $acc2:ident :: FieldAccess, $($ty:ty),*) => {
        $(
        impl $acc2 :: FieldAccess<$ty> {
            #[inline(always)]
            pub const fn meta() -> &'static dyn $crate::Meta {
                $acc1::FieldAccess::<$ty>::meta()
            }
            #[inline(always)]
            pub const fn size_of_field_at(buf: &[u8]) -> Result<usize, $crate::ParseError> {
                $acc1::FieldAccess::<$ty>::size_of_field_at(buf)
            }
            #[inline(always)]
            pub const fn extract(buf: &[u8]) -> Result<<$ty as $crate::Enliven>::WithLifetime<'_>, $crate::ParseError> {
                $acc1::FieldAccess::<$ty>::extract(buf)
            }
            pub const fn constant(value: usize) -> <$ty as $crate::Enliven>::WithLifetime<'static> {
                $acc1::FieldAccess::<$ty>::constant(value)
            }
            #[inline(always)]
            pub const fn measure(value: &<$ty as $crate::Enliven>::ForMeasure<'_>) -> usize {
                $acc1::FieldAccess::<$ty>::measure(value)
            }
        }
        )*
    };
}
