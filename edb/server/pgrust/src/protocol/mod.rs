// TODO

// Maybe they can also have the "measurement" / creation types too
// Finish tuplenest/tupleunest
// Create filtering for nested tuples to remove the fixed size stuff for measurement
// Maybe it can also remove the fixed-value itsems as well for creation

mod arrays;
mod datatypes;
mod definition;
mod gen;
mod writer;

/// Metatypes
pub mod meta {
    pub use super::arrays::meta::*;
    pub use super::datatypes::meta::*;
    pub use super::definition::gen::meta::*;
}

pub mod measure {
    pub use super::definition::gen::measure::*;
}

pub mod builder {
    pub use super::definition::gen::builder::*;
}

pub mod messages {
    pub use super::definition::Backend;
}

#[allow(unused)]
pub use arrays::{Array, ArrayIter, ZTArray, ZTArrayIter};
#[allow(unused)]
pub use datatypes::{Encoded, Rest, ZTString};
#[allow(unused)]
pub use definition::gen::data::*;

pub trait Enliven<'a> {
    type WithLifetime;
    type ForMeasure;
    type ForBuilder;
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
pub(crate) struct FieldAccess<T: for<'a> Enliven<'a>> {
    _phantom_data: std::marker::PhantomData<T>,
}

/// Delegate to the concrete `FieldAccess` for each type we want to extract.
macro_rules! field_access {
    ($ty:ty) => {
        impl<'a> $crate::protocol::FieldAccessNonConst<'a, <$ty as Enliven<'a>>::WithLifetime>
            for <$ty as Enliven<'a>>::WithLifetime
        {
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

macro_rules! match_message {
    ($buf:expr, $messages:path {
        $(( $i1:path $(as $i2:ident )?) => $impl:block),* $(,)?
    }) => {
        $(
            if <$i1>::is(&$buf) {
                $(let $i2 = <$i1>::new($buf);)?
                $impl;
            } else
        )*
        {}
    };
}
pub(crate) use match_message;

#[cfg(test)]
mod tests {
    use definition::r#gen::builder;

    use super::*;

    #[test]
    fn test_sasl_response() {
        let buf = [b'p', 0, 0, 0, 5, 2];
        assert!(SASLResponse::is(&buf));
        let message = SASLResponse::new(&buf);
        assert_eq!(message.mlen(), 5);
        assert_eq!(message.response().len(), 1);
    }

    #[test]
    fn test_sasl_response_measure() {
        let measure = measure::SASLResponse {
            response: &[1, 2, 3, 4, 5],
        };
        assert_eq!(measure.measure(), 10)
    }

    #[test]
    fn test_sasl_initial_response() {
        let buf = [
            b'p', 0, 0, 0, 0x36, 
            // Mechanism
            b'S', b'C', b'R', b'A', b'M', b'-', b'S', b'H', b'A', b'-', b'2', b'5', b'6', 0, 
            // Data
            0, 0, 0, 32, 
            b'n', b',', b',', b'n', b'=', b',', b'r', b'=', b'p',
            b'E', b'k', b'P', b'L', b'Q', b'u', b'2', b'9', b'G', b'E', b'v', b'w', b'N', b'e', b'V', b'J',
            b't', b'7', b'2', b'a', b'r', b'Q', b'I',
        ];

        assert!(SASLInitialResponse::is(&buf));
        let message = SASLInitialResponse::new(&buf);
        assert_eq!(message.mlen(), 0x36);
        assert_eq!(message.mechanism(), "SCRAM-SHA-256");
        assert_eq!(message.response().as_ref(), b"n,,n=,r=pEkPLQu29GEvwNeVJt72arQI");
    }

    #[test]
    fn test_sasl_initial_response_builder() {
        let buf = builder::SASLInitialResponse {
            mlen: 0x36,
            mechanism: "SCRAM-SHA-256",
            response: b"n,,n=,r=pEkPLQu29GEvwNeVJt72arQI",
        }.to_vec();

        let message = SASLInitialResponse::new(&buf);
        assert_eq!(message.mlen(), 0x36);
        assert_eq!(message.mechanism(), "SCRAM-SHA-256");
        assert_eq!(message.response().as_ref(), b"n,,n=,r=pEkPLQu29GEvwNeVJt72arQI");
    }

    #[test]
    fn test_startup_message() {
        let buf = [
            0, 0, 0, 41, 0, 0x03, 0, 0, 0x75, 0x73, 0x65, 0x72, 0, 0x70, 0x6f, 0x73, 0x74, 0x67,
            0x72, 0x65, 0x73, 0, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0, 0x70, 0x6f,
            0x73, 0x74, 0x67, 0x72, 0x65, 0x73, 0, 0,
        ];
        let message = StartupMessage::new(&buf);
        assert_eq!(message.mlen() as usize, buf.len());
        assert_eq!(message.protocol(), 196608);
        let arr = message.params();
        let mut vals = vec![];
        for entry in arr {
            vals.push(entry.name().to_owned());
            vals.push(entry.value().to_owned());
        }
        assert_eq!(vals, vec!["user", "postgres", "database", "postgres"]);
    }

    #[test]
    fn test_row_description() {
        let buf = [
            b'T', 0, 0, 0, 48, // header
            0, 2, // # of fields
            b'f', b'1', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // field 1
            b'f', b'2', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // field 2
        ];
        assert!(RowDescription::is(&buf));
        let message = RowDescription::new(&buf);
        assert_eq!(message.mlen() as usize, buf.len() - 1);
        assert_eq!(message.fields().len(), 2);
        let mut iter = message.fields().into_iter();
        let f1 = iter.next().unwrap();
        assert_eq!(f1.name(), "f1");
        let f2 = iter.next().unwrap();
        assert_eq!(f2.name(), "f2");
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_row_description_measure() {
        let measure = measure::RowDescription {
            fields: &[
                measure::RowField { name: "F1" },
                measure::RowField { name: "F2" },
            ],
        };
        assert_eq!(49, measure.measure())
    }

    #[test]
    fn test_row_description_builder() {
        let builder = builder::RowDescription {
            mlen: 49,
            fields: &[
                builder::RowField {
                    name: "F1",
                    column_attr_number: 1,
                    ..Default::default()
                },
                builder::RowField {
                    name: "F2",
                    data_type_oid: 1234,
                    format_code: 1,
                    ..Default::default()
                },
            ],
        };

        let vec = builder.to_vec();
        assert_eq!(49, vec.len());

        // Read it back
        assert!(RowDescription::is(&vec));
        let message = RowDescription::new(&vec);
        assert_eq!(message.fields().len(), 2);
        let mut iter = message.fields().into_iter();
        let f1 = iter.next().unwrap();
        assert_eq!(f1.name(), "F1");
        assert_eq!(f1.column_attr_number(), 1);
        let f2 = iter.next().unwrap();
        assert_eq!(f2.name(), "F2");
        assert_eq!(f2.data_type_oid(), 1234);
        assert_eq!(f2.format_code(), 1);
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_message_polymorphism_sync() {
        let sync = builder::Sync::default();
        let buf = sync.to_vec();
        assert_eq!(buf.len(), 5);
        // Read it as a Message
        let message = Message::new(&buf);
        assert_eq!(message.mlen(), 4);
        assert_eq!(message.mtype(), b'S');
        assert_eq!(message.data(), &[]);
        // And also a Sync
        assert!(Sync::is(&buf));
        let message = Sync::new(&buf);
        assert_eq!(message.mlen(), 4);
        assert_eq!(message.mtype(), b'S');
    }

    #[test]
    fn test_message_polymorphism_rest() {
        let mlen = measure::AuthenticationGSSContinue {
            data: &[1, 2, 3, 4, 5],
        }
        .measure() as _;
        let auth = builder::AuthenticationGSSContinue {
            mlen,
            data: &[1, 2, 3, 4, 5],
        };
        let buf = auth.to_vec();
        // Read it as a Message
        assert!(Message::is(&buf));
        let message = Message::new(&buf);
        assert_eq!(message.mlen(), 14);
        assert_eq!(message.mtype(), b'R');
        assert_eq!(message.data(), &[0, 0, 0, 8, 1, 2, 3, 4, 5]);
        // And also a AuthenticationGSSContinue
        assert!(AuthenticationGSSContinue::is(&buf));
        let message = AuthenticationGSSContinue::new(&buf);
        assert_eq!(message.mlen(), 14);
        assert_eq!(message.mtype(), b'R');
        assert_eq!(message.data(), &[1, 2, 3, 4, 5]);
    }
}
