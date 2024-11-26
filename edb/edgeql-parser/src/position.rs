use std::fmt;
use std::str::{from_utf8, Utf8Error};

use unicode_width::UnicodeWidthStr;

/// Span of an element in source code
#[derive(Debug, Clone, Copy, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Span {
    /// Byte offset in the original file
    ///
    /// Technically you can read > 4Gb file on 32bit machine so it may
    /// not fit in usize
    pub start: u64,

    /// Byte offset in the original file
    ///
    /// Technically you can read > 4Gb file on 32bit machine so it may
    /// not fit in usize
    pub end: u64,
}
/// Original position of element in source code
#[derive(PartialOrd, Ord, PartialEq, Eq, Clone, Copy, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Pos {
    /// One-based line number
    pub line: usize,
    /// One-based column number
    pub column: usize,
    /// Byte offset in the original file
    ///
    /// Technically you can read > 4Gb file on 32bit machine so it may
    /// not fit in usize
    pub offset: u64,
}

/// This contains position in all forms that EdgeDB needs
#[derive(Clone, Copy, Debug)]
pub struct InflatedPos {
    /// Zero-based line number
    pub line: u64,
    /// Zero-based column number
    pub column: u64,
    /// Zero-based Utf16 column offset
    ///
    /// (this is required by language server protocol, LSP)
    pub utf16column: u64,
    /// Bytes offset in the orignal (utf-8 encoded) byte buffer
    pub offset: u64,
    /// Character offset in the whole string
    pub char_offset: u64,
}

/// Error calculating InflatedPos
#[derive(Debug, thiserror::Error)]
pub enum InflatingError {
    #[error(transparent)]
    Utf8(Utf8Error),
    #[error("offset out of range")]
    OutOfRange,
}

impl fmt::Debug for Pos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pos({}:{})", self.line, self.column)
    }
}

impl fmt::Display for Pos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.line, self.column)
    }
}

impl Span {
    pub fn combine(self, right: Span) -> Span {
        Span {
            start: self.start,
            end: right.end,
        }
    }
}

fn new_lines_in_fragment(data: &[u8]) -> u64 {
    let mut was_lf = false;
    let mut lines = 0;
    for byte in data {
        match byte {
            b'\n' if was_lf => {
                was_lf = false;
            }
            b'\n' => {
                lines += 1;
            }
            b'\r' => {
                lines += 1;
                was_lf = true;
            }
            _ => {
                was_lf = false;
            }
        }
    }
    lines
}

impl InflatedPos {
    pub fn from_offset(data: &[u8], offset: u64) -> Result<InflatedPos, InflatingError> {
        let res = Self::from_offsets(data, &[offset as usize])?;
        Ok(res.into_iter().next().unwrap())
    }

    pub fn from_offsets(
        data: &[u8],
        offsets: &[usize],
    ) -> Result<Vec<InflatedPos>, InflatingError> {
        let mut result = Vec::with_capacity(offsets.len());
        // TODO(tailhook) optimize calculation if offsets are growing
        for &offset in offsets {
            if offset > data.len() {
                return Err(InflatingError::OutOfRange);
            }
            let prefix = &data[..offset];
            let prefix_s = from_utf8(prefix).map_err(InflatingError::Utf8)?;
            let line_offset;
            let line;
            if let Some(loff) = prefix_s.rfind(['\r', '\n']) {
                line_offset = loff + 1;
                let mut lines = &prefix[..loff];
                if data[loff] == b'\n' && loff > 0 && data[loff - 1] == b'\r' {
                    lines = &lines[..lines.len() - 1];
                }
                line = new_lines_in_fragment(lines) + 1;
            } else {
                line = 0;
                line_offset = 0;
            };
            let col_s = &prefix_s[line_offset..offset];
            result.push(InflatedPos {
                line,
                column: UnicodeWidthStr::width(col_s) as u64,
                utf16column: col_s.chars().map(|c| c.len_utf16() as u64).sum(),
                offset: offset as u64,
                char_offset: prefix_s.chars().count() as u64,
            });
        }
        Ok(result)
    }

    pub fn deflate(self) -> Pos {
        Pos {
            line: self.line as usize + 1,
            column: self.column as usize + 1,
            offset: self.offset,
        }
    }
}

#[cfg(test)]
mod test {
    use super::{new_lines_in_fragment, InflatedPos};

    fn mkpos(s: &str, off: usize) -> InflatedPos {
        InflatedPos::from_offsets(s.as_bytes(), &[off]).unwrap()[0]
    }

    #[test]
    fn ascii_line() {
        let line = "Lorem ipsum dolor sit amet, consectetur adipiscing elit,";
        for off in 0..line.len() {
            let pos = mkpos(line, off);
            let off = off as u64;
            assert_eq!(pos.line, 0);
            assert_eq!(pos.column, off);
            assert_eq!(pos.utf16column, off);
            assert_eq!(pos.offset, off);
            assert_eq!(pos.char_offset, off);
        }
    }

    #[test]
    fn ascii_multi_line() {
        let line = "line1\nline2";
        for off in 6..line.len() {
            let pos = mkpos(line, off);
            let off = off as u64;
            assert_eq!(pos.line, 1);
            assert_eq!(pos.column, off - 6);
            assert_eq!(pos.utf16column, off - 6);
            assert_eq!(pos.offset, off);
            assert_eq!(pos.char_offset, off);
        }
    }

    #[test]
    fn line_endings() {
        fn count(s: &str) -> u64 {
            new_lines_in_fragment(s.as_bytes())
        }
        assert_eq!(count("line1\nline2\nline3"), 2);
        assert_eq!(count("line1\rline2\rline3"), 2);
        assert_eq!(count("line1\r\nline2\r\nline3"), 2);
        assert_eq!(count("line1\rline2\r\nline3\n"), 3);
        assert_eq!(count("line1\nline2\rline3\r\n"), 3);
        assert_eq!(count("line1\n\rline2\r\rline3\r"), 5);
    }

    #[test]
    fn char_offsets() {
        let pos = mkpos("bomb = '💣'", 12);
        assert_eq!(pos.line, 0);
        assert_eq!(pos.column, 10); // bomb is 2 char width
        assert_eq!(pos.utf16column, 10); // and also 2 utf8 codepoints
        assert_eq!(pos.offset, 12);
        assert_eq!(pos.char_offset, 9);

        let pos = mkpos("line1\nbomb = '💣'", 18);
        assert_eq!(pos.line, 1);
        assert_eq!(pos.column, 10);
        assert_eq!(pos.utf16column, 10);
        assert_eq!(pos.offset, 18);
        assert_eq!(pos.char_offset, 15);

        let pos = mkpos("bomb = '💣'\nline1", 18);
        assert_eq!(pos.line, 1);
        assert_eq!(pos.column, 4);
        assert_eq!(pos.utf16column, 4);
        assert_eq!(pos.offset, 18);
        assert_eq!(pos.char_offset, 15);

        let pos = mkpos("letter = 'Ф'", 12);
        assert_eq!(pos.line, 0);
        assert_eq!(pos.column, 11);
        assert_eq!(pos.utf16column, 11);
        assert_eq!(pos.offset, 12);
        assert_eq!(pos.char_offset, 11);

        let pos = mkpos("line1\nletter = 'Ф'", 18);
        assert_eq!(pos.line, 1);
        assert_eq!(pos.column, 11);
        assert_eq!(pos.utf16column, 11);
        assert_eq!(pos.offset, 18);
        assert_eq!(pos.char_offset, 17);

        let pos = mkpos("letter = 'Ф'\nline1", 18);
        assert_eq!(pos.line, 1);
        assert_eq!(pos.column, 4);
        assert_eq!(pos.utf16column, 4);
        assert_eq!(pos.offset, 18);
        assert_eq!(pos.char_offset, 17);

        let pos = mkpos("letter = 'Ｈ'", 13);
        assert_eq!(pos.line, 0);
        assert_eq!(pos.column, 12);
        assert_eq!(pos.utf16column, 11);
        assert_eq!(pos.offset, 13);
        assert_eq!(pos.char_offset, 11);

        let pos = mkpos("line1\nletter = 'Ｈ'", 19);
        assert_eq!(pos.line, 1);
        assert_eq!(pos.column, 12);
        assert_eq!(pos.utf16column, 11);
        assert_eq!(pos.offset, 19);
        assert_eq!(pos.char_offset, 17);

        let pos = mkpos("letter = 'Ｈ'\nline1", 19);
        assert_eq!(pos.line, 1);
        assert_eq!(pos.column, 4);
        assert_eq!(pos.utf16column, 4);
        assert_eq!(pos.offset, 19);
        assert_eq!(pos.char_offset, 17);
    }
}
