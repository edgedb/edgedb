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
);
