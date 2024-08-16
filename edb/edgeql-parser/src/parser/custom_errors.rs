use crate::tokenizer::Kind;
use crate::{keywords::Keyword, position::Span};

use super::{CSTNode, Context, Error, Parser, StackNode, Terminal};

impl<'s> Parser<'s> {
    pub(super) fn custom_error(&self, ctx: &Context, token: &Terminal) -> Option<Error> {
        let ltok = self.get_from_top(0).unwrap();

        if let Some(value) = self.custom_error_from_rule(token, ctx) {
            return Some(value);
        }

        if matches!(token.kind, Kind::Keyword(Keyword("explain"))) {
            return Some({
                Error {
                    message: format!("Unexpected keyword '{}'", token.text.to_uppercase()),
                    span: Span::default(),
                    hint: Some("Use `analyze` to show query performance details".to_string()),
                    details: None,
                }
            });
        }

        if let Kind::Keyword(kw) = token.kind {
            if kw.is_reserved() && !Cond::Production("Expr").check(ltok, ctx) {
                // Another token followed by a reserved keyword:
                // likely an attempt to use keyword as identifier
                return Some(unexpected_reserved_keyword(&token.text, token.span));
            }
        };

        None
    }

    fn custom_error_from_rule(&self, token: &Terminal, ctx: &Context) -> Option<Error> {
        let last = self.get_from_top(0).unwrap();

        let (i, rule) = self.get_rule(ctx)?;
        // Look at the parsing stack and use tokens and
        // non-terminals to infer the parser rule when the
        // error occurred.

        match rule {
            ParserRule::ListOfArguments
                // The stack is like <NodeName> LPAREN <AnyIdentifier>
                if i == 1
                    && Cond::AnyOf(vec![
                        Cond::Production("AnyIdentifier"),
                        Cond::keyword("with"),
                        Cond::keyword("select"),
                        Cond::keyword("for"),
                        Cond::keyword("insert"),
                        Cond::keyword("update"),
                        Cond::keyword("delete"),
                    ])
                    .check(last, ctx)
            => {
                return Some(Error {
                    message: "Missing parentheses around statement used as an expression"
                        .to_string(),
                    span: super::get_span_of_nodes(&[last.value]).unwrap_or_default(),
                    hint: None,
                    details: None,
                });
            }

            ParserRule::ArraySlice
                if matches!(token.kind, Kind::Ident | Kind::IntConst)
                && !Cond::Terminal(Kind::Colon).check(last, ctx)
            => {
                // The offending token was something that could
                // make an expression
                return Some(Error::new(format!(
                    "It appears that a ':' is missing in {rule} before {}",
                    token.text
                )));
            },

            ParserRule::Definition if token.kind == Kind::Ident => {
                // Something went wrong in a definition, so check
                // if the last successful token is a keyword.
                if Cond::Production("Identifier").check(last, ctx)
                // TODO: && ltok.value.upper() == "INDEX"
                {
                    return Some(Error::new(format!(
                        "Expected 'ON', but got '{}' instead",
                        token.text
                    )));
                }
            },

            ParserRule::ForIterator => {
                let span = if i >= 4 {
                    let span_start = self.get_from_top(i - 4).unwrap();
                    let span = super::get_span_of_nodes(&[span_start.value]).unwrap_or_default();
                    span.combine(token.span)
                } else {
                    token.span
                };
                return Some(Error {
                    message: "Missing parentheses around complex expression in \
                              a FOR iterator clause".to_string(),
                    span,
                    hint: None,
                    details: None,
                });
            },

            _ => {}
        }
        None
    }

    /// Look at the parsing stack and use tokens and non-terminals
    /// to infer the parser rule when the error occurred.
    fn get_rule(&self, ctx: &Context) -> Option<(usize, ParserRule)> {
        // If the last valid token was a closing brace/parent/bracket,
        // so we need to find a match for it before deciding what rule
        // context we're in.
        let mut need_match = self.compare_stack(
            &[Cond::AnyOf(vec![
                Cond::Terminal(Kind::CloseBrace),
                Cond::Terminal(Kind::CloseParen),
                Cond::Terminal(Kind::CloseBracket),
            ])],
            0,
            ctx,
        );
        let mut found_union = false;

        let ltok = self.get_from_top(0).unwrap();

        let mut nextel = None;
        let mut curr_el = Some(self.stack_top);
        let mut i = 0;
        while let Some(el) = curr_el {
            // We'll need the element right before "{", "[", or "(".
            let prevel = el.parent;

            match el.value {
                CSTNode::Terminal(Terminal {
                    kind: Kind::OpenBrace,
                    ..
                }) => {
                    if need_match && Cond::Terminal(Kind::CloseBrace).check(ltok, ctx) {
                        // This is matched, while we're looking
                        // for unmatched braces.
                        need_match = false;
                    } else if Cond::Production("OptExtending").check_opt(prevel, ctx) {
                        // This is some SDL/DDL
                        return Some((i, ParserRule::Definition));
                    } else if prevel.map_or(false, |prevel| {
                        Cond::Production("Expr").check(prevel, ctx)
                            || (Cond::Terminal(Kind::Colon).check(prevel, ctx)
                                && Cond::Production("ShapePointer").check_opt(prevel.parent, ctx))
                    }) {
                        // This is some kind of shape.
                        return Some((i, ParserRule::Shape));
                    } else {
                        return None;
                    }
                }

                CSTNode::Terminal(Terminal {
                    kind: Kind::OpenParen,
                    ..
                }) => {
                    if need_match && Cond::Terminal(Kind::CloseParen).check(ltok, ctx) {
                        // This is matched, while we're looking
                        // for unmatched parentheses.
                        need_match = false
                    } else if Cond::Production("NodeName").check_opt(prevel, ctx) {
                        return Some((i, ParserRule::ListOfArguments));
                    } else if Cond::AnyOf(vec![
                        Cond::keyword("for"),
                        Cond::keyword("select"),
                        Cond::keyword("update"),
                        Cond::keyword("delete"),
                        Cond::keyword("insert"),
                        Cond::keyword("for"),
                    ])
                    .check_opt(nextel, ctx)
                    {
                        // A parenthesized subquery expression,
                        // we should leave the error as is.
                        return None;
                    } else {
                        return Some((i, ParserRule::Tuple));
                    }
                }

                CSTNode::Terminal(Terminal {
                    kind: Kind::OpenBracket,
                    ..
                }) => {
                    // This is either an array literal or
                    // array index.

                    if need_match && Cond::Terminal(Kind::CloseBracket).check(ltok, ctx) {
                        // This is matched, while we're looking
                        // for unmatched brackets.
                        need_match = false
                    } else if Cond::Production("Expr").check_opt(prevel, ctx) {
                        return Some((i, ParserRule::ArraySlice));
                    } else {
                        return Some((i, ParserRule::Array));
                    }
                }

                _ => {}
            }

            // Check if we're in the `FOR x IN bad_tokens` situation
            if self.compare_stack(&[Cond::keyword("union")], i, ctx) {
                found_union = true;
            }
            if !found_union
                && self.compare_stack(
                    &[
                        Cond::keyword("for"),
                        Cond::Production("OptionalOptional"),
                        Cond::Production("Identifier"),
                        Cond::keyword("in"),
                    ],
                    i,
                    ctx,
                )
            {
                return Some((i + 3, ParserRule::ForIterator));
            }

            // Also keep track of the element right after current.
            nextel = Some(el);
            curr_el = el.parent;
            i += 1;
        }

        None
    }

    /// Looks at the stack and compares it with the expected nodes.
    /// Does not compare [top_offset] number of nodes from the top of the start.
    ///
    /// Example of matching with top_offset=1, expected=[X, Y, Z]
    /// ```plain
    /// stack top -> A     (offset 1)
    ///              B - Z
    ///              C - Y
    ///              D - X
    ///              E
    /// ```
    fn compare_stack(&self, expected: &[Cond], top_offset: usize, ctx: &Context) -> bool {
        let mut current = self.get_from_top(top_offset);

        for validator in expected.iter().rev() {
            let Some(cur) = current else {
                return false;
            };
            if !validator.check(cur, ctx) {
                return false;
            }

            current = cur.parent;
        }
        true
    }
}

fn unexpected_reserved_keyword(text: &str, span: Span) -> Error {
    let text_upper = text.to_uppercase();
    Error {
        message: format!("Unexpected keyword '{text_upper}'"),
        span,
        details: Some(
            "This name is a reserved keyword and cannot be \
            used as an identifier"
                .to_string(),
        ),
        hint: Some(format!(
            "Use a different identifier or quote the name \
            with backticks: `{text}`"
        )),
    }
}

/// Condition for a stack node. An easier way to match stack node kinds.
enum Cond {
    Terminal(Kind),
    Production(&'static str),
    AnyOf(Vec<Cond>),
}

impl Cond {
    fn keyword(kw: &'static str) -> Self {
        Cond::Terminal(Kind::Keyword(Keyword(kw)))
    }

    fn check(&self, node: &StackNode, ctx: &Context) -> bool {
        match self {
            Cond::Terminal(kind) => matches!(
                node.value,
                CSTNode::Terminal(Terminal { kind: k, .. }) if k == kind
            ),
            Cond::Production(non_term) => match node.value {
                CSTNode::Production(prod) => {
                    let (pn, _) = &ctx.spec.production_names[prod.id];
                    if non_term == pn {
                        return true;
                    }

                    // When looking for a production, it might have happened
                    // that it was inlined and superseded by one of its
                    // arguments. That's why we save the id of the parent into
                    // child's `inlined_ids` and check all of them here.
                    if let Some(inlined_ids) = prod.inlined_ids {
                        for prod_id in inlined_ids {
                            let (pn, _) = &ctx.spec.production_names[*prod_id];
                            if non_term == pn {
                                return true;
                            }
                        }
                    }
                    false
                }
                _ => false,
            },
            Cond::AnyOf(options) => options.iter().any(|v| v.check(node, ctx)),
        }
    }

    fn check_opt(&self, node: Option<&StackNode>, ctx: &Context) -> bool {
        node.map_or(false, |x| self.check(x, ctx))
    }
}

#[derive(Debug)]
enum ParserRule {
    ForIterator,
    Definition,
    Shape,
    ArraySlice,
    Array,
    Tuple,
    ListOfArguments,
}

impl std::fmt::Display for ParserRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserRule::ForIterator => f.write_str("for iterator"),
            ParserRule::Definition => f.write_str("definition"),
            ParserRule::Shape => f.write_str("shape"),
            ParserRule::ArraySlice => f.write_str("array slice"),
            ParserRule::Array => f.write_str("array"),
            ParserRule::Tuple => f.write_str("tuple"),
            ParserRule::ListOfArguments => f.write_str("list of arguments"),
        }
    }
}

pub fn post_process(errors: Vec<Error>) -> Vec<Error> {
    let mut new_errors: Vec<Error> = Vec::with_capacity(errors.len());
    for error in errors {
        // Enrich combination of 'Unexpected keyword' + 'Missing identifier'
        if error.message == "Missing identifier" {
            if let Some(last) = new_errors.last() {
                if last.message.starts_with("Unexpected keyword '")
                    && last.span.end == error.span.start
                {
                    let last = new_errors.pop().unwrap();
                    let text = last.message.strip_prefix("Unexpected keyword '").unwrap();
                    let text = text.strip_suffix('\'').unwrap();

                    new_errors.push(unexpected_reserved_keyword(text, last.span));
                    continue;
                }
            }
        }

        new_errors.push(error);
    }

    new_errors
}
