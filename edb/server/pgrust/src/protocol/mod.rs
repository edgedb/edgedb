// TODO

// Maybe they can also have the "measurement" / creation types too
// Finish tuplenest/tupleunest
// Create filtering for nested tuples to remove the fixed size stuff for measurement
// Maybe it can also remove the fixed-value itsems as well for creation


use std::marker::PhantomData;

mod arrays;
mod datatypes;
mod definition;
mod gen;
mod tuples;

/// Metatypes
pub mod meta {
    pub use super::definition::gen::meta::*;
    pub use super::datatypes::meta::{Encoded, Rest, ZTString};
    pub use super::arrays::meta::*;
}

#[allow(unused)]
pub use definition::gen::data::*;
#[allow(unused)]
pub use datatypes::*;
#[allow(unused)]
pub use arrays::*;

pub(crate) trait Enliven<'a> {
    type WithLifetime;
}

/// Delegates to a concrete `FieldAccess` but as a non-const trait.
pub(crate) trait FieldAccessNonConst<'a, T: 'a> {
    fn size_of_field_at(buf: &[u8]) -> usize;
    fn extract(buf: &'a [u8]) -> T;
}

pub trait FieldTypes {
    type FieldTypes;
}

/// This struct is specialized for each type we want to extract data from. We
/// have to do it this way to work around Rust's lack of const specialization.
pub(crate) struct FieldAccess<T: for <'a> Enliven<'a>> {
    _phantom_data: PhantomData<T>,
}

/// Delegate to the concrete `FieldAccess` for each type we want to extract.
macro_rules! field_access {
    ($ty:ty) => {
        impl <'a> $crate::protocol::FieldAccessNonConst<'a, <$ty as Enliven<'a>>::WithLifetime> for <$ty as Enliven<'a>>::WithLifetime {
            #[inline(always)]
            fn size_of_field_at(buf: &[u8]) -> usize {
                $crate::protocol::FieldAccess::<$ty>::size_of_field_at(buf)
            }
            #[inline(always)]
            fn extract(buf: &'a [u8]) -> <$ty as $crate::protocol::Enliven<'a>>::WithLifetime {
                $crate::protocol::FieldAccess::<$ty>::extract(buf)
            }            
        }
    };
}
pub(crate) use field_access;

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

        let x: <crate::protocol::meta::RowDescription as FieldTypes>::FieldTypes;
    }
}
