use std::ops::Range;

use chumsky::error::Cheap;
use chumsky::prelude::*;
use chumsky::{select, Parser};

use cpython::{PyObject, PyResult, Python};

use crate::ast::*;
use crate::into_python::IntoPython;
use crate::position::Pos;
use crate::tokenizer::{Kind as Token, Token as TokenData, TokenStream};

pub fn parse(text: String) -> PyResult<PyObject> {
    let mut token_stream = TokenStream::new(&text);
    let mut tokens = Vec::new();
    for res in &mut token_stream {
        match res {
            Ok(t) => tokens.push(t),
            Err(_) => todo!(),
        }
    }
    let end_pos = token_stream.current_pos();
    let end_span = into_span(end_pos, end_pos);
    let tokens = tokens
        .into_iter()
        .map(|t| (t.token, into_span(t.start, t.end)));

    let stream = chumsky::Stream::from_iter(end_span, tokens);

    let (out, errors) = expr_stmt().parse_recovery_verbose(stream);

    dbg!(errors);
    let out = dbg!(out.unwrap());

    // convert to Python object
    let gil = Python::acquire_gil();
    let py = gil.python();
    out.into_python(py, None)
}

fn into_span(start: Pos, end: Pos) -> Range<usize> {
    (start.offset as usize)..(end.offset as usize)
}

fn token<'a>(knd: Token) -> impl Parser<TokenData<'a>, &'a str, Error = Cheap<TokenData<'a>>> {
    select! {
        TokenData { kind, value } if kind == knd => value
    }
}

fn keyword(kw: &'_ str) -> impl Parser<TokenData<'_>, (), Error = Cheap<TokenData<'_>>> {
    select! {
        TokenData { kind, value } if kind == Token::Keyword && value.to_lowercase() == kw => ()
    }
}

fn ident(ident: &'_ str) -> impl Parser<TokenData<'_>, &'_ str, Error = Cheap<TokenData<'_>>> {
    select! {
        TokenData { kind, value } if kind == Token::Keyword && value == ident => value
    }
}

pub fn expr_stmt<'a>() -> impl Parser<TokenData<'a>, Expr, Error = Cheap<TokenData<'a>>> {
    keyword("select")
        .ignore_then(optionally_aliased_expr())
        .map(|result| Expr {
            kind: ExprKind::Query(Query {
                kind: QueryKind::PipelinedQuery(PipelinedQuery {
                    implicit: false,
                    offset: None,
                    limit: None,
                    kind: PipelinedQueryKind::SelectQuery(SelectQuery {
                        result_alias: result.alias,
                        result: result.expr,
                    }),
                    r#where: None,
                    orderby: None,
                    rptr_passthrough: false,
                }),
                aliases: None,
            }),
        })
}

struct OptionallyAliasedExpr {
    alias: Option<String>,
    expr: Box<Expr>,
}

impl OptionallyAliasedExpr {
    #[allow(dead_code)]
    fn into_aliased(self) -> Option<AliasedExpr> {
        let expr = self.expr;
        self.alias.map(|alias| AliasedExpr { alias, expr })
    }
}

fn optionally_aliased_expr<'a>(
) -> impl Parser<TokenData<'a>, OptionallyAliasedExpr, Error = Cheap<TokenData<'a>>> {
    identifier()
        .then_ignore(token(Token::Assign))
        .or_not()
        .then(expr().map(Box::new))
        .map(|(alias, expr)| OptionallyAliasedExpr { alias, expr })
}

fn identifier<'a>() -> impl Parser<TokenData<'a>, String, Error = Cheap<TokenData<'a>>> {
    token(Token::Ident).map(|n| n.to_string())
}

fn qualified_name<'a>() -> impl Parser<TokenData<'a>, String, Error = Cheap<TokenData<'a>>> {
    token(Token::Ident).map(str::to_string).then_ignore(token(Token::Namespace))
}


fn expr<'a>() -> impl Parser<TokenData<'a>, Expr, Error = Cheap<TokenData<'a>>> {
    recursive(|tree| {
        let argument = token(Token::Argument)
            .map(str::to_string)
            .map(|name| Parameter { name })
            .map(ExprKind::Parameter);

        let source = ident("__source__")
            .map(str::to_string)
            .map(|name| SpecialAnchorKind::Source(Source { name }));
        let subject = ident("__subject__")
            .map(str::to_string)
            .map(|name| SpecialAnchorKind::Subject(Subject { name }));

        let anchors1 = source
            .or(subject)
            .map(Some)
            .map(|kind| AnchorKind::SpecialAnchor(SpecialAnchor { kind }))
            .map(prepend("".to_string()));

        let anchors2 = choice((ident("__new__"), ident("__old__"), ident("__specified__")))
            .map(str::to_string)
            .map(append(AnchorKind::SpecialAnchor(SpecialAnchor {
                // TODO: SpecialAnchorKind::Plain
                kind: Some(SpecialAnchorKind::Source(Source {
                    name: "".to_string(),
                })),
            })));

        let anchors = anchors1
            .or(anchors2)
            .map(|(name, kind)| Anchor { name, kind })
            .map(ExprKind::Anchor);

        let tuple_or_paren = token(Token::OpenParen)
            .ignore_then(
                tree.clone()
                    .then(
                        token(Token::Comma)
                            .ignore_then(tree.separated_by(token(Token::Comma)))
                            .or_not(),
                    )
                    .or_not(),
            )
            .then_ignore(token(Token::CloseParen))
            .map(|x| {
                if let Some((first, following)) = x {
                    if let Some(following) = following {
                        // this is a tuple
                        let elements = [vec![first], following].concat();
                        let elements = elements.into_iter().map(Box::new).collect();
                        ExprKind::Tuple(Tuple { elements })
                    } else {
                        // this is a parenthesized expr
                        first.kind
                    }
                } else {
                    // empty tuple
                    ExprKind::Tuple(Tuple { elements: vec![] })
                }
            });

        choice((argument, constant(), tuple_or_paren, anchors)).map(|kind| Expr { kind })
    })
}

fn constant<'a>() -> impl Parser<TokenData<'a>, ExprKind, Error = Cheap<TokenData<'a>>> {
    let real_constant = choice((
        token(Token::IntConst).map(append(BaseRealConstantKind::IntegerConstant(
            IntegerConstant {},
        ))),
        token(Token::FloatConst).map(append(BaseRealConstantKind::FloatConstant(
            FloatConstant {},
        ))),
        token(Token::BigIntConst).map(append(BaseRealConstantKind::BigintConstant(
            BigintConstant {},
        ))),
        token(Token::DecimalConst).map(append(BaseRealConstantKind::DecimalConstant(
            DecimalConstant {},
        ))),
    ))
    .map(map_second(|kind| {
        BaseConstantKind::BaseRealConstant(BaseRealConstant {
            kind,
            is_negative: false,
        })
    }));

    let string_constant =
        token(Token::Str).map(append(BaseConstantKind::StringConstant(StringConstant {})));

    let boolean_constant = choice((keyword("true").to("true"), keyword("false").to("false"))).map(
        append(BaseConstantKind::BooleanConstant(BooleanConstant {})),
    );

    let bytes_constant = token(Token::BinStr)
        .map(|x| {
            BaseConstantKind::BytesConstant(BytesConstant {
                value: x.bytes().collect(),
            })
        })
        .map(prepend(""));

    choice((
        real_constant,
        string_constant,
        boolean_constant,
        bytes_constant,
    ))
    .map(map_first(str::to_string))
    .map(|(value, kind)| BaseConstant { value, kind })
    .map(ExprKind::BaseConstant)
}

// struct MapSecond<A, B, F> where F: Fn(A) -> B {
//     f: F,
//     _a: PhantomData<A>,
//     _b: PhantomData<B>,
// }

// impl <A, B> FnMut(A) -> B for MapSecond<> {

// }

fn prepend<Y, V: Clone>(val: V) -> impl Fn(Y) -> (V, Y) {
    move |y| (val.clone(), y)
}

fn append<X, V: Clone>(val: V) -> impl Fn(X) -> (X, V) {
    move |x| (x, val.clone())
}

fn map_first<A, B, Y, F>(f: F) -> impl Fn((A, Y)) -> (B, Y)
where
    F: Fn(A) -> B,
{
    move |(a, y)| (f(a), y)
}

fn map_second<X, A, B, F>(f: F) -> impl Fn((X, A)) -> (X, B)
where
    F: Fn(A) -> B,
{
    move |(x, a)| (x, f(a))
}

// trait MapSecond<I: Clone, O1, O2, E>: Parser<I, (O1, O2), Error = E> + Sized {
//     fn map_second<F, U>(self, f: F) -> impl Parser<I, (O1, U), Error = E>
//     where
//         F: Fn(O1) -> U,
//     {
//     }
// }
