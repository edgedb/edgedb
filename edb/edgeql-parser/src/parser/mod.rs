mod expr;
mod util;

use std::ops::Range;

use chumsky::prelude::*;

use crate::ast::*;
use crate::tokenizer::{Kind as Token, Token as TokenData};
use util::*;

use self::expr::expr_stmt;

pub fn parse_block(text: &str) -> ParserResult<Vec<Expr>> {
    let (end_span, tokens) = match tokenize(text) {
        Ok(tokens) => tokens,
        Err(errors) => return ParserResult { ast: None, errors },
    };
    let stream = chumsky::Stream::from_iter(end_span, tokens.into_iter());

    let (ast, errors) = edgeql_block().parse_recovery_verbose(stream);

    let errors = convert_errors(errors);
    ParserResult { ast, errors }
}

pub fn parse_single(text: &str) -> ParserResult<Expr> {
    let (end_span, tokens) = match tokenize(text) {
        Ok(tokens) => tokens,
        Err(errors) => return ParserResult { ast: None, errors },
    };
    let stream = chumsky::Stream::from_iter(end_span, tokens.into_iter());

    let (ast, errors) = single_stmt().parse_recovery_verbose(stream);

    let errors = convert_errors(errors);
    ParserResult { ast, errors }
}

pub struct ParserResult<T> {
    pub ast: Option<T>,
    pub errors: Vec<Error>,
}

pub struct Error {
    pub message: String,
    pub span: Range<u64>,
}

fn edgeql_block<'a>() -> impl Parser<TokenData<'a>, Vec<Expr>, Error = Simple<TokenData<'a>>> {
    single_stmt()
        .separated_by(token(Token::Semicolon))
        .then_ignore(token(Token::Semicolon).repeated())
        .then_ignore(end())
}

fn single_stmt<'a>() -> impl Parser<TokenData<'a>, Expr, Error = Simple<TokenData<'a>>> {
    let stmt = expr_stmt()
        // .or(transaction_stmt)
        // .or(describe_stmt)
        // .or(analyze_stmt)
        // .or(administer_stmt)
        ;

    stmt
    // .or(ddl_stmt)
    // .or(session_stmt)
    // .or(config_stmt)
}
