mod arrays;
mod buffer;
mod datatypes;
mod gen;
mod message_group;
mod writer;

#[cfg(test)]
mod test_protocol;

/// Metatypes for the protocol and related arrays/strings.
pub mod meta {
    pub use super::arrays::meta::*;
    pub use super::datatypes::meta::*;
}

#[allow(unused)]
pub use arrays::{Array, ArrayIter, ZTArray, ZTArrayIter};
pub use buffer::StructBuffer;
#[allow(unused)]
pub use datatypes::{Encoded, LString, Rest, ZTString};
pub use message_group::{match_message, message_group};
pub use writer::BufWriter;
pub use gen::protocol;

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
pub trait Enliven {
    type WithLifetime<'a>;
    type ForMeasure<'a>: 'a;
    type ForBuilder<'a>: 'a;
}

pub trait FixedSize: Enliven {
    const SIZE: usize;
    /// Extract this type from the given buffer, assuming that enough bytes are available.
    fn extract_infallible(buf: &[u8]) -> <Self as Enliven>::WithLifetime<'_>;
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

/// Delegates to a concrete [`FieldAccess`] but as a non-const trait. This is
/// used for performing extraction in iterators.
pub trait FieldAccessArray: Enliven {
    const META: &'static dyn Meta;
    fn size_of_field_at(buf: &[u8]) -> Result<usize, ParseError>;
    fn extract(buf: &[u8]) -> Result<<Self as Enliven>::WithLifetime<'_>, ParseError>;
    fn copy_to_buf(buf: &mut BufWriter, value: &Self::ForBuilder<'_>);
}

pub struct FieldAccess<T: Enliven> {
    _phantom_data: std::marker::PhantomData<T>,
}

/// Delegate to the concrete [`FieldAccess`] for each type we want to extract.
#[macro_export]
#[doc(hidden)]
macro_rules! field_access {
    ($acc:ident :: FieldAccess, $ty:ty) => {
        impl $crate::FieldAccessArray for $ty {
            const META: &'static dyn $crate::Meta =
                $acc::FieldAccess::<$ty>::meta();
            #[inline(always)]
            fn size_of_field_at(buf: &[u8]) -> Result<usize, $crate::ParseError> {
                $acc::FieldAccess::<$ty>::size_of_field_at(buf)
            }
            #[inline(always)]
            fn extract(
                buf: &[u8],
            ) -> Result<
                <Self as $crate::Enliven>::WithLifetime<'_>,
                $crate::ParseError,
            > {
                $acc::FieldAccess::<$ty>::extract(buf)
            }
            #[inline(always)]
            fn copy_to_buf(buf: &mut $crate::BufWriter, value: &<$ty as $crate::Enliven>::ForBuilder<'_>) {
                $acc::FieldAccess::<$ty>::copy_to_buf(buf, value)
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! field_access_copy {
    ($acc1:ident :: FieldAccess, $acc2:ident :: FieldAccess, $($ty:ty),*) => {
        $(
            $crate::field_access_copy!(: $acc1 :: FieldAccess, $acc2 :: FieldAccess, 
                $ty, 
                $crate::meta::Array<u8, $ty>,
                $crate::meta::Array<i16, $ty>
            );
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
            pub const fn constant(value: usize) -> $ty {
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
