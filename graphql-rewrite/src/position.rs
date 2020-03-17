use std::fmt;

/// Original position of element in source code
#[derive(PartialOrd, Ord, PartialEq, Eq, Clone, Copy, Default, Hash)]
pub struct Pos {
    /// One-based line number
    pub line: usize,
    /// One-based column number
    pub column: usize,
    /// Zero-based character offset in the buffer
    pub character: usize,
    /// Zero-based token index in the output stream
    pub token: usize,
}

impl fmt::Debug for Pos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pos({}:{},{},{})", self.line, self.column,
            self.character, self.token)
    }
}

impl fmt::Display for Pos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.line, self.column)
    }
}
