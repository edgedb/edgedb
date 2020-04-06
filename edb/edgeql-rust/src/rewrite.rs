#[derive(Debug, PartialEq)]
pub enum Value {
    Str(String),
    Int(String),
    Float(String),
    BigInt(String),
    Decimal(String),
}

#[derive(Debug, PartialEq)]
pub struct Variable {
    pub value: Value,
}

#[derive(Debug)]
pub struct Entry {
    pub key: String,
    pub variables: Vec<Variable>,
}

#[derive(Debug)]
pub enum Error {
    Tokenizer(String),
}

pub fn rewrite(text: &str)
    -> Result<Entry, Error>
{
    todo!();
}
