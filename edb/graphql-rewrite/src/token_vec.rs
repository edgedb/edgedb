use edb_graphql_parser::position::Pos;
use edb_graphql_parser::tokenizer::Token;

pub struct TokenVec<'a> {
    tokens: &'a Vec<(Token<'a>, Pos)>,
    consumed: usize,
}

impl<'a> TokenVec<'a> {
    pub fn new(tokens: &'a Vec<(Token<'a>, Pos)>) -> TokenVec {
        TokenVec {
            tokens,
            consumed: 0,
        }
    }
    pub fn drain(&mut self, n: usize) -> impl Iterator<Item = &(Token, Pos)> {
        let pos = self.consumed;
        self.consumed += n;
        assert!(n <= self.tokens.len(), "attempt to more tokens than exist");
        self.tokens[pos..][..n].iter()
    }
    pub fn drain_to(&mut self, end: usize) -> impl Iterator<Item = &(Token, Pos)> {
        let n = end
            .checked_sub(self.consumed)
            .expect("drain_to with index smaller than current");
        self.drain(n)
    }
    pub fn len(&self) -> usize {
        self.tokens
            .len()
            .checked_sub(self.consumed)
            .expect("consumed more tokens than exists")
    }
}
