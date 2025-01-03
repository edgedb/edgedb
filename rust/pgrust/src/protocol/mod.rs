pub mod edgedb;
pub mod postgres;

#[cfg(test)]
mod tests {
    use super::*;
    use db_proto::{match_message, Encoded, StructBuffer, StructMeta};
    use postgres::{builder, data::*, measure, meta};
    use rand::Rng;
    /// We want to ensure that no malformed messages will cause unexpected
    /// panics, so we try all sorts of combinations of message mutation to
    /// ensure we don't.
    ///
    /// This isn't a 100% foolproof test.
    fn fuzz_test<S: StructMeta>(s: S::Struct<'_>) {
        let buf = S::to_vec(&s);

        // Re-create, won't panic
        fuzz_test_buf::<S>(&buf);

        // Truncating at any given length won't panic
        for i in 0..buf.len() {
            let mut buf = S::to_vec(&s);
            buf.truncate(i);
            fuzz_test_buf::<S>(&buf);
        }

        // Removing any particular value won't panic
        for i in 0..buf.len() {
            let mut buf = S::to_vec(&s);
            buf.remove(i);
            fuzz_test_buf::<S>(&buf);
        }

        // Zeroing any particular value won't panic
        for i in 0..buf.len() {
            let mut buf = S::to_vec(&s);
            buf[i] = 0;
            fuzz_test_buf::<S>(&buf);
        }

        // Corrupt each byte by incrementing (mod 256)
        for i in 0..buf.len() {
            let mut buf = S::to_vec(&s);
            buf[i] = buf[i].wrapping_add(1);
            fuzz_test_buf::<S>(&buf);
        }

        // Corrupt each byte by decrementing (mod 256)
        for i in 0..buf.len() {
            let mut buf = S::to_vec(&s);
            buf[i] = buf[i].wrapping_sub(1);
            fuzz_test_buf::<S>(&buf);
        }

        // Replace four-byte chunks at 1-byte offsets with "-2" in big-endian, one at a time
        // This shakes out any negative length issues for i32 lengths
        let negative_two_i32: i32 = -2;
        let bytes_i32 = negative_two_i32.to_be_bytes();
        for start_index in 0..buf.len().saturating_sub(3) {
            if start_index + 4 <= buf.len() {
                let mut buf = S::to_vec(&s); // Clean buffer for each iteration
                buf[start_index..start_index + 4].copy_from_slice(&bytes_i32);
                eprintln!("Replaced 4-byte chunk at offset {} with -2 (big-endian) in buffer of length {}", start_index, buf.len());
                fuzz_test_buf::<S>(&buf);
            }
        }

        // Replace two-byte chunks at 1-byte offsets with "-2" in big-endian, one at a time
        // This shakes out any negative length issues for i16 lengths
        let negative_two_i16: i16 = -2;
        let bytes_i16 = negative_two_i16.to_be_bytes();
        for start_index in 0..buf.len().saturating_sub(1) {
            if start_index + 2 <= buf.len() {
                let mut buf = S::to_vec(&s); // Clean buffer for each iteration
                buf[start_index..start_index + 2].copy_from_slice(&bytes_i16);
                eprintln!("Replaced 2-byte chunk at offset {} with -2 (big-endian) in buffer of length {}", start_index, buf.len());
                fuzz_test_buf::<S>(&buf);
            }
        }

        let run_count = if std::env::var("EXTENSIVE_FUZZ").is_ok() {
            100000
        } else {
            10
        };

        // Insert a random byte at a random position
        for i in 0..run_count {
            let mut buf = S::to_vec(&s);
            let random_byte: u8 = rand::thread_rng().gen();
            let random_position = rand::thread_rng().gen_range(0..=buf.len());
            buf.insert(random_position, random_byte);
            eprintln!(
                "Test {}: Inserted byte 0x{:02X} at position {} in buffer of length {}",
                i + 1,
                random_byte,
                random_position,
                buf.len()
            );
            fuzz_test_buf::<S>(&buf);
        }

        // Corrupt random parts of the buffer. This is non-deterministic.
        for i in 0..run_count {
            let mut buf = S::to_vec(&s);
            let rand: [u8; 4] = rand::thread_rng().gen();
            let n = rand::thread_rng().gen_range(0..buf.len() - 4);
            let range = n..n + 4;
            eprintln!(
                "Test {}: Corrupting buffer of length {} at range {:?} with bytes {:?}",
                i + 1,
                buf.len(),
                range,
                rand
            );
            buf.get_mut(range).unwrap().copy_from_slice(&rand);
            fuzz_test_buf::<S>(&buf);
        }

        // Corrupt 1..4 random bytes at random positions
        for i in 0..run_count {
            let mut buf = S::to_vec(&s);
            let num_bytes_to_corrupt = rand::thread_rng().gen_range(1..=4);
            let mut positions = Vec::new();

            for _ in 0..num_bytes_to_corrupt {
                let random_position = rand::thread_rng().gen_range(0..buf.len());
                if !positions.contains(&random_position) {
                    positions.push(random_position);
                    let random_byte: u8 = rand::thread_rng().gen();
                    buf[random_position] = random_byte;
                }
            }

            eprintln!(
                "Test {}: Corrupted {} byte(s) at position(s) {:?} in buffer of length {}",
                i + 1,
                positions.len(),
                positions,
                buf.len()
            );
            fuzz_test_buf::<S>(&buf);
        }

        // Attempt to parse randomly generated structs. This is non-deterministic.
        for i in 0..run_count {
            let buf: [u8; 16] = rand::thread_rng().gen();
            eprintln!(
                "Test {}: Attempting to parse random buffer: {:02X?}",
                i + 1,
                buf
            );
            fuzz_test_buf::<S>(&buf);
        }
    }

    fn fuzz_test_buf<S: StructMeta>(buf: &[u8]) {
        // Use std::fmt::Debug which will walk each field
        if let Ok(m) = S::new(buf) {
            let _ = format!("{:?}", m);
        }
    }

    #[test]
    fn test_sasl_response() {
        let buf = [b'p', 0, 0, 0, 5, 2];
        assert!(SASLResponse::is_buffer(&buf));
        let message = SASLResponse::new(&buf).unwrap();
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
            b'p', 0, 0, 0, 0x36, // Mechanism
            b'S', b'C', b'R', b'A', b'M', b'-', b'S', b'H', b'A', b'-', b'2', b'5', b'6', 0,
            // Data
            0, 0, 0, 32, b'n', b',', b',', b'n', b'=', b',', b'r', b'=', b'p', b'E', b'k', b'P',
            b'L', b'Q', b'u', b'2', b'9', b'G', b'E', b'v', b'w', b'N', b'e', b'V', b'J', b't',
            b'7', b'2', b'a', b'r', b'Q', b'I',
        ];

        assert!(SASLInitialResponse::is_buffer(&buf));
        let message = SASLInitialResponse::new(&buf).unwrap();
        assert_eq!(message.mlen(), 0x36);
        assert_eq!(message.mechanism(), "SCRAM-SHA-256");
        assert_eq!(
            message.response().as_ref(),
            b"n,,n=,r=pEkPLQu29GEvwNeVJt72arQI"
        );

        fuzz_test::<meta::SASLInitialResponse>(message);
    }

    #[test]
    fn test_sasl_initial_response_builder() {
        let buf = builder::SASLInitialResponse {
            mechanism: "SCRAM-SHA-256",
            response: b"n,,n=,r=pEkPLQu29GEvwNeVJt72arQI",
        }
        .to_vec();

        let message = SASLInitialResponse::new(&buf).unwrap();
        assert_eq!(message.mlen(), 0x36);
        assert_eq!(message.mechanism(), "SCRAM-SHA-256");
        assert_eq!(
            message.response().as_ref(),
            b"n,,n=,r=pEkPLQu29GEvwNeVJt72arQI"
        );

        fuzz_test::<meta::SASLInitialResponse>(message);
    }

    #[test]
    fn test_startup_message() {
        let buf = [
            0, 0, 0, 41, 0, 0x03, 0, 0, 0x75, 0x73, 0x65, 0x72, 0, 0x70, 0x6f, 0x73, 0x74, 0x67,
            0x72, 0x65, 0x73, 0, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0, 0x70, 0x6f,
            0x73, 0x74, 0x67, 0x72, 0x65, 0x73, 0, 0,
        ];
        let message = StartupMessage::new(&buf).unwrap();
        assert_eq!(message.mlen(), buf.len());
        assert_eq!(message.protocol(), 196608);
        let arr = message.params();
        let mut vals = vec![];
        for entry in arr {
            vals.push(entry.name().to_owned().unwrap());
            vals.push(entry.value().to_owned().unwrap());
        }
        assert_eq!(vals, vec!["user", "postgres", "database", "postgres"]);

        fuzz_test::<meta::StartupMessage>(message);
    }

    #[test]
    fn test_row_description() {
        let buf = [
            b'T', 0, 0, 0, 48, // header
            0, 2, // # of fields
            b'f', b'1', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // field 1
            b'f', b'2', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // field 2
        ];
        assert!(RowDescription::is_buffer(&buf));
        let message = RowDescription::new(&buf).unwrap();
        assert_eq!(message.mlen(), buf.len() - 1);
        assert_eq!(message.fields().len(), 2);
        let mut iter = message.fields().into_iter();
        let f1 = iter.next().unwrap();
        assert_eq!(f1.name(), "f1");
        let f2 = iter.next().unwrap();
        assert_eq!(f2.name(), "f2");
        assert_eq!(None, iter.next());
        fuzz_test::<meta::RowDescription>(message);
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
        assert!(RowDescription::is_buffer(&vec));
        let message = RowDescription::new(&vec).unwrap();
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

        fuzz_test::<meta::RowDescription>(message);
    }

    #[test]
    fn test_message_polymorphism_sync() {
        let sync = builder::Sync::default();
        let buf = sync.to_vec();
        assert_eq!(buf.len(), 5);
        // Read it as a Message
        let message = Message::new(&buf).unwrap();
        assert_eq!(message.mlen(), 4);
        assert_eq!(message.mtype(), b'S');
        assert_eq!(message.data(), &[]);
        // And also a Sync
        assert!(Sync::is_buffer(&buf));
        let message = Sync::new(&buf).unwrap();
        assert_eq!(message.mlen(), 4);
        assert_eq!(message.mtype(), b'S');

        fuzz_test::<meta::Sync>(message);
    }

    #[test]
    fn test_message_polymorphism_rest() {
        let auth = builder::AuthenticationGSSContinue {
            data: &[1, 2, 3, 4, 5],
        };
        let buf = auth.to_vec();
        assert_eq!(14, buf.len());
        // Read it as a Message
        assert!(Message::is_buffer(&buf));
        let message = Message::new(&buf).unwrap();
        assert_eq!(message.mlen(), 13);
        assert_eq!(message.mtype(), b'R');
        assert_eq!(message.data(), &[0, 0, 0, 8, 1, 2, 3, 4, 5]);
        // And also a AuthenticationGSSContinue
        assert!(AuthenticationGSSContinue::is_buffer(&buf));
        let message = AuthenticationGSSContinue::new(&buf).unwrap();
        assert_eq!(message.mlen(), 13);
        assert_eq!(message.mtype(), b'R');
        assert_eq!(message.data(), &[1, 2, 3, 4, 5]);

        fuzz_test::<meta::AuthenticationGSSContinue>(message);
    }

    #[test]
    fn test_query_messages() {
        let data: Vec<u8> = vec![
            0x54, 0x00, 0x00, 0x00, 0x21, 0x00, 0x01, 0x3f, b'c', b'o', b'l', b'u', b'm', b'n',
            0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x04,
            0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x44, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x01, b'1', b'C', 0x00, 0x00, 0x00, 0x0d, b'S', b'E', b'L', b'E', b'C',
            b'T', b' ', b'1', 0x00, 0x5a, 0x00, 0x00, 0x00, 0x05, b'I',
        ];

        let mut buffer = StructBuffer::<meta::Message>::default();
        buffer.push(&data, |message| {
            match_message!(message, Backend {
                (RowDescription as row) => {
                    assert_eq!(row.fields().len(), 1);
                    let field = row.fields().into_iter().next().unwrap();
                    assert_eq!(field.name(), "?column?");
                    assert_eq!(field.data_type_oid(), 23);
                    assert_eq!(field.format_code(), 0);
                    eprintln!("{row:?}");
                    fuzz_test::<meta::RowDescription>(row);
                },
                (DataRow as row) => {
                    assert_eq!(row.values().len(), 1);
                    assert_eq!(row.values().into_iter().next().unwrap(), "1");
                    eprintln!("{row:?}");
                    fuzz_test::<meta::DataRow>(row);
                },
                (CommandComplete as complete) => {
                    assert_eq!(complete.tag(), "SELECT 1");
                    eprintln!("{complete:?}");
                },
                (ReadyForQuery as ready) => {
                    assert_eq!(ready.status(), b'I');
                    eprintln!("{ready:?}");
                },
                unknown => {
                    panic!("Unknown message type: {:?}", unknown);
                }
            });
        });
    }

    #[test]
    fn test_encode_data_row() {
        builder::DataRow {
            values: &[Encoded::Value(b"1")],
        }
        .to_vec();
    }

    #[test]
    fn test_parse() {
        let buf = [
            b'P', // message type
            0, 0, 0, 25, // message length
            b'S', b't', b'm', b't', 0, // statement name
            b'S', b'E', b'L', b'E', b'C', b'T', b' ', b'$', b'1', 0, // query string
            0, 1, // number of parameter data types
            0, 0, 0, 23, // OID
        ];

        assert!(Parse::is_buffer(&buf));
        let message = Parse::new(&buf).unwrap();
        assert_eq!(message.mlen(), 25);
        assert_eq!(message.statement(), "Stmt");
        assert_eq!(message.query(), "SELECT $1");
        assert_eq!(message.param_types().len(), 1);
        assert_eq!(message.param_types().get(0).unwrap(), 23); // OID

        fuzz_test::<meta::Parse>(message);
    }

    #[test]
    fn test_function_call() {
        let buf = builder::FunctionCall {
            function_id: 100,
            format_codes: &[0],
            args: &[Encoded::Value(b"123")],
            result_format_code: 0,
        }
        .to_vec();

        assert!(FunctionCall::is_buffer(&buf));
        let message = FunctionCall::new(&buf).unwrap();
        assert_eq!(message.function_id(), 100);
        assert_eq!(message.format_codes().len(), 1);
        assert_eq!(message.format_codes().get(0).unwrap(), 0);
        assert_eq!(message.args().len(), 1);
        assert_eq!(
            message.args().into_iter().next().unwrap(),
            b"123".as_slice()
        );
        assert_eq!(message.result_format_code(), 0);

        fuzz_test::<meta::FunctionCall>(message);
    }

    #[test]
    fn test_datarow() {
        let buf = [
            0x44, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff,
        ];
        assert!(DataRow::is_buffer(&buf));
        let message = DataRow::new(&buf).unwrap();
        assert_eq!(message.values().len(), 1);
        assert_eq!(message.values().into_iter().next().unwrap(), Encoded::Null);
    }

    #[test]
    fn test_edgedb_sasl() {
        use crate::protocol::edgedb::*;

        assert_eq!(
            builder::AuthenticationRequiredSASLMessage {
                methods: &["SCRAM-SHA-256"]
            }
            .to_vec(),
            vec![
                82, 0, 0, 0, 29, 0, 0, 0, 10, 0, 0, 0, 1, 0, 0, 0, 13, 83, 67, 82, 65, 77, 45, 83,
                72, 65, 45, 50, 53, 54
            ]
        );
    }
}
