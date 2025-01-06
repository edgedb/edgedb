use super::{ParseError, StructLength};
use std::{collections::VecDeque, marker::PhantomData};

/// A buffer that accumulates bytes of sized structs and feeds them to provided sink function when messages
/// are complete. This buffer handles partial messages and multiple messages in a single push.
#[derive(Default)]
pub struct StructBuffer<M: StructLength> {
    _phantom: PhantomData<M>,
    accum: VecDeque<u8>,
}

impl<M: StructLength> StructBuffer<M> {
    /// Pushes bytes into the buffer, potentially feeding output to the function.
    ///
    /// # Lifetimes
    /// - `'a`: The lifetime of the input byte slice.
    /// - `'b`: The lifetime of the mutable reference to `self`.
    /// - `'c`: A lifetime used in the closure's type, representing the lifetime of the `M::Struct` instances passed to it.
    ///
    /// The constraint `'a: 'b` ensures that the input bytes live at least as long as the mutable reference to `self`.
    ///
    /// The `for<'c>` syntax in the closure type is a higher-ranked trait bound. It indicates that the closure
    /// must be able to handle `M::Struct` with any lifetime `'c`. This is crucial because:
    ///
    /// 1. It allows the `push` method to create `M::Struct` instances with lifetimes that are not known
    ///    at the time the closure is defined.
    /// 2. It ensures that the `M::Struct` instances passed to the closure are only valid for the duration
    ///    of each call to the closure, not for the entire lifetime of the `push` method.
    /// 3. It prevents the closure from storing or returning these `M::Struct` instances, as their lifetime
    ///    is limited to the scope of each closure invocation.
    pub fn push<'a: 'b, 'b>(
        &'b mut self,
        bytes: &'a [u8],
        mut f: impl for<'c> FnMut(Result<M::Struct<'c>, ParseError>),
    ) {
        if self.accum.is_empty() {
            // Fast path: try to process the input directly
            let mut offset = 0;
            while offset < bytes.len() {
                if let Some(len) = M::length_of_buf(&bytes[offset..]) {
                    if offset + len <= bytes.len() {
                        f(M::new(&bytes[offset..offset + len]));
                        offset += len;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            if offset == bytes.len() {
                return;
            }
            self.accum.extend(&bytes[offset..]);
        } else {
            self.accum.extend(bytes);
        }

        // Slow path: process accumulated data
        let contiguous = self.accum.make_contiguous();
        let mut total_processed = 0;
        while let Some(len) = M::length_of_buf(&contiguous[total_processed..]) {
            if total_processed + len <= contiguous.len() {
                let message_bytes = &contiguous[total_processed..total_processed + len];
                f(M::new(message_bytes));
                total_processed += len;
            } else {
                break;
            }
        }
        if total_processed > 0 {
            self.accum.rotate_left(total_processed);
            self.accum.truncate(self.accum.len() - total_processed);
        }
    }

    /// Pushes bytes into the buffer, potentially feeding output to the function.
    ///
    /// # Lifetimes
    /// - `'a`: The lifetime of the input byte slice.
    /// - `'b`: The lifetime of the mutable reference to `self`.
    /// - `'c`: A lifetime used in the closure's type, representing the lifetime of the `M::Struct` instances passed to it.
    ///
    /// The constraint `'a: 'b` ensures that the input bytes live at least as long as the mutable reference to `self`.
    ///
    /// The `for<'c>` syntax in the closure type is a higher-ranked trait bound. It indicates that the closure
    /// must be able to handle `M::Struct` with any lifetime `'c`. This is crucial because:
    ///
    /// 1. It allows the `push` method to create `M::Struct` instances with lifetimes that are not known
    ///    at the time the closure is defined.
    /// 2. It ensures that the `M::Struct` instances passed to the closure are only valid for the duration
    ///    of each call to the closure, not for the entire lifetime of the `push` method.
    /// 3. It prevents the closure from storing or returning these `M::Struct` instances, as their lifetime
    ///    is limited to the scope of each closure invocation.
    pub fn push_fallible<'a: 'b, 'b, E>(
        &'b mut self,
        bytes: &'a [u8],
        mut f: impl for<'c> FnMut(Result<M::Struct<'c>, ParseError>) -> Result<(), E>,
    ) -> Result<(), E> {
        if self.accum.is_empty() {
            // Fast path: try to process the input directly
            let mut offset = 0;
            while offset < bytes.len() {
                if let Some(len) = M::length_of_buf(&bytes[offset..]) {
                    if offset + len <= bytes.len() {
                        f(M::new(&bytes[offset..offset + len]))?;
                        offset += len;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            if offset == bytes.len() {
                return Ok(());
            }
            self.accum.extend(&bytes[offset..]);
        } else {
            self.accum.extend(bytes);
        }

        // Slow path: process accumulated data
        let contiguous = self.accum.make_contiguous();
        let mut total_processed = 0;
        while let Some(len) = M::length_of_buf(&contiguous[total_processed..]) {
            if total_processed + len <= contiguous.len() {
                let message_bytes = &contiguous[total_processed..total_processed + len];
                f(M::new(message_bytes))?;
                total_processed += len;
            } else {
                break;
            }
        }
        if total_processed > 0 {
            self.accum.rotate_left(total_processed);
            self.accum.truncate(self.accum.len() - total_processed);
        }
        Ok(())
    }

    pub fn into_inner(self) -> VecDeque<u8> {
        self.accum
    }

    pub fn is_empty(&self) -> bool {
        self.accum.is_empty()
    }

    pub fn len(&self) -> usize {
        self.accum.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Encoded, ParseError};

    use super::StructBuffer;
    use crate::test_protocol::{builder, data::*, meta};

    /// Create a test data buffer containing three messages
    fn test_data() -> (Vec<u8>, Vec<usize>) {
        let mut test_data = vec![];
        let mut lengths = vec![];
        test_data.append(&mut builder::Sync::default().to_vec());
        let len = test_data.len();
        lengths.push(len);
        test_data.append(&mut builder::CommandComplete { tag: "TAG" }.to_vec());
        lengths.push(test_data.len() - len);
        let len = test_data.len();
        test_data.append(
            &mut builder::DataRow {
                values: &[Encoded::Value(b"1")],
            }
            .to_vec(),
        );
        lengths.push(test_data.len() - len);
        (test_data, lengths)
    }

    fn process_chunks(buf: &[u8], chunk_lengths: &[usize]) {
        assert_eq!(
            chunk_lengths.iter().sum::<usize>(),
            buf.len(),
            "Sum of chunk lengths must equal total buffer length"
        );

        let mut accumulated_messages: Vec<Vec<u8>> = Vec::new();
        let mut buffer = StructBuffer::<meta::Message>::default();
        let mut f = |msg: Result<Message, ParseError>| {
            let msg = msg.unwrap();
            eprintln!("Message: {msg:?}");
            accumulated_messages.push(msg.to_vec());
        };

        let mut start = 0;
        for &length in chunk_lengths {
            let end = start + length;
            let chunk = &buf[start..end];
            eprintln!("Chunk: {chunk:?}");

            buffer.push(chunk, &mut f);
            start = end;
        }

        assert_eq!(accumulated_messages.len(), 3);

        let mut out = vec![];
        for message in accumulated_messages {
            out.append(&mut message.to_vec());
        }

        assert_eq!(&out, buf);
    }

    #[test]
    fn test_message_buffer_chunked() {
        let (test_data, chunk_lengths) = test_data();
        process_chunks(&test_data, &chunk_lengths);
    }

    #[test]
    fn test_message_buffer_byte_by_byte() {
        let (test_data, _) = test_data();
        let chunk_lengths: Vec<usize> = vec![1; test_data.len()];
        process_chunks(&test_data, &chunk_lengths);
    }

    #[test]
    fn test_message_buffer_incremental_chunks() {
        let (test_data, _) = test_data();
        for i in 0..test_data.len() {
            let chunk_lengths = vec![i, test_data.len() - i];
            process_chunks(&test_data, &chunk_lengths);
        }
    }
}
