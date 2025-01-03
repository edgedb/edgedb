//! A pseudo-Postgres protocol for testing.
use crate::gen::protocol;

protocol!(
    struct Message {
        /// The message type.
        mtype: u8,
        /// The length of the message contents in bytes, including self.
        mlen: len,
        /// The message contents.
        data: Rest,
    }

    /// The `CommandComplete` struct represents a message indicating the successful completion of a command.
    struct CommandComplete: Message {
        /// Identifies the message as a command-completed response.
        mtype: u8 = 'C',
        /// Length of message contents in bytes, including self.
        mlen: len,
        /// The command tag.
        tag: ZTString,
    }

    /// The `Sync` message is used to synchronize the client and server.
    struct Sync: Message {
        /// Identifies the message as a synchronization request.
        mtype: u8 = 'S',
        /// Length of message contents in bytes, including self.
        mlen: len,
    }

    /// The `DataRow` message represents a row of data returned from a query.
    struct DataRow: Message {
        /// Identifies the message as a data row.
        mtype: u8 = 'D',
        /// Length of message contents in bytes, including self.
        mlen: len,
        /// The values in the row.
        values: Array<i16, Encoded>,
    }

    struct QueryType {
        /// The type of the query parameter.
        typ: u8,
        /// The length of the query parameter.
        len: u32,
        /// The metadata of the query parameter.
        meta: Array<u32, u8>,
    }

    struct Query: Message {
        /// Identifies the message as a query.
        mtype: u8 = 'Q',
        /// Length of message contents in bytes, including self.
        mlen: len,
        /// The query string.
        query: ZTString,
        /// The types of the query parameters.
        types: Array<i16, QueryType>,
    }

    struct Key {
        /// The key.
        key: [u8; 16],
    }

    struct Uuids {
        /// The UUIDs.
        uuids: Array<u32, Uuid>,
    }
);

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_meta() {
        let expected = [
            r#"Message { Field("mtype"): u8, Field("mlen"): len, Field("data"): Rest }"#,
            r#"CommandComplete { Parent: "Message", Field("mtype"): u8, Field("mlen"): len, Field("tag"): ZTString }"#,
            r#"Sync { Parent: "Message", Field("mtype"): u8, Field("mlen"): len }"#,
            r#"DataRow { Parent: "Message", Field("mtype"): u8, Field("mlen"): len, Field("values"): Array { Length: i16, Item: Encoded } }"#,
            r#"QueryType { Field("typ"): u8, Field("len"): u32, Field("meta"): Array { Length: u32, Item: u8 } }"#,
            r#"Query { Parent: "Message", Field("mtype"): u8, Field("mlen"): len, Field("query"): ZTString, Field("types"): Array { Length: i16, Item: QueryType { Field("typ"): u8, Field("len"): u32, Field("meta"): Array { Length: u32, Item: u8 } } } }"#,
            r#"Key { Field("key"): FixedArray { Length: 16, Item: u8 } }"#,
            r#"Uuids { Field("uuids"): Array { Length: u32, Item: Uuid } }"#,
        ];

        for (i, meta) in meta::ALL.iter().enumerate() {
            assert_eq!(expected[i], format!("{meta:?}"));
        }
    }

    #[test]
    fn test_query() {
        let buf = builder::Query {
            query: "SELECT * from foo",
            types: &[builder::QueryType {
                typ: 1,
                len: 4,
                meta: &[1, 2, 3, 4],
            }],
        }
        .to_vec();

        let query = data::Query::new(&buf).expect("Failed to parse query");
        assert_eq!(
            r#"Query { mtype: 81, mlen: 37, query: "SELECT * from foo", types: [QueryType { typ: 1, len: 4, meta: [1, 2, 3, 4] }] }"#,
            format!("{query:?}")
        );
    }

    #[test]
    fn test_fixed_array() {
        let buf = builder::Key {
            key: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        }
        .to_vec();

        let key = data::Key::new(&buf).expect("Failed to parse key");
        assert_eq!(
            key.key(),
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
    }

    #[test]
    fn test_uuid() {
        let buf = builder::Uuids {
            uuids: &[Uuid::NAMESPACE_DNS],
        }
        .to_vec();

        let uuids = data::Uuids::new(&buf).expect("Failed to parse uuids");
        assert_eq!(uuids.uuids().get(0), Some(Uuid::NAMESPACE_DNS));
    }
}
