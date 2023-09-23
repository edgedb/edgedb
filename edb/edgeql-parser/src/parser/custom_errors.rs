use crate::keywords::Keyword;
use crate::tokenizer::Kind;

use super::{CSTNode, Error, Parser, StackNode, Terminal};

impl<'s> Parser<'s> {
    pub(super) fn get_exception(&self, token: &Terminal) -> Option<Error> {
        self.print_stack();

        let ltok = self.get_from_top(0).unwrap();

        // Look at the parsing stack and use tokens and
        // non-terminals to infer the parser rule when the
        // error occurred.
        let (i, rule) = dbg!(self.get_rule()?);

        match rule {
            ParserRule::Shape if matches!(token.kind, Kind::Ident) && Cond::Production.check(ltok) => {
                // TODO:    isinstance(ltok, parsing.Nonterm)
                // Make sure that the previous element in the stack
                // is some kind of Nonterminal, because if it's
                // not, this is probably not an issue of a missing
                // COMMA.
                return Some(Error::new(format!("It appears that a ',' is missing in {rule} before {}", token.text)));
            }

            ParserRule::ListOfArguments
                // The stack is like <NodeName> LPAREN <AnyIdentifier>
                if i == 1
                    && Cond::AnyOf(vec![
                        Cond::Production, // gr_exprs.AnyIdentifier,
                        Cond::Terminal(Kind::Keyword(Keyword("with"))),
                        Cond::Terminal(Kind::Keyword(Keyword("select"))),
                        Cond::Terminal(Kind::Keyword(Keyword("for"))),
                        Cond::Terminal(Kind::Keyword(Keyword("insert"))),
                        Cond::Terminal(Kind::Keyword(Keyword("update"))),
                        Cond::Terminal(Kind::Keyword(Keyword("delete"))),
                    ])
                    .check(ltok)
            => {
                return Some(Error {
                    message: "Missing parenthesis around statement used as an expression"
                        .to_string(),
                    span: super::get_span_of_nodes(&[ltok.value]).unwrap_or_default(),
                });
            }

            ParserRule::ArraySlice if matches!(token.kind, Kind::Ident | Kind::IntConst) && !Cond::Terminal(Kind::Colon).check(ltok) => {
                // The offending token was something that could
                // make an expression
                return Some(Error::new(format!(
                    "It appears that a ':' is missing in {rule} before {}",
                    token.text
                )));
            },

            ParserRule::ListOfArguments | ParserRule::Tuple | ParserRule::Array if matches!(
                token.kind,
                Kind::Ident
                    | Kind::IntConst
                    | Kind::FloatConst
                    | Kind::BigIntConst
                    | Kind::DecimalConst
                    | Kind::BinStr
                    | Kind::Str
                    | Kind::Keyword(Keyword("true"))
                    | Kind::Keyword(Keyword("false"))
            ) && !Cond::Terminal(Kind::Comma).check(ltok) =>
            {
                // The offending token was something that could
                // make an expression

                return Some(Error::new(format!(
                    "It appears that a ',' is missing in {rule} before {}",
                    token.text
                )));
            }

            ParserRule::Definition if token.kind == Kind::Ident => {
                // Something went wrong in a definition, so check
                // if the last successful token is a keyword.
                if Cond::Production.check(ltok)
                // TODO: gr_exprs.Identifier
                // TODO: && ltok.value.upper() == "INDEX"
                {
                    return Some(Error::new(format!(
                        "Expected 'ON', but got {} instead",
                        token.text
                    )));
                }
            },

            ParserRule::ForIterator => {
                // if i > 0 {
                //     context = pctx.merge_context([
                //         self.parser._stack[-i][0].context, context,
                //     ])
                // }
    
                return Some(Error {
                    message: "Missing parentheses around complex expression in a FOR iterator clause"
                        .to_string(),
                    span: super::get_span_of_nodes(&[ltok.value]).unwrap_or_default(),
                });
            },

            _ => {}
        }

        if matches!(token.kind, Kind::Keyword(Keyword("explain"))) {
            let _hint = "Use `analyze` to show query performance details";
            return Some(Error::new(format!("Unexpected keyword {}", token.text)));
        }

        let is_reserved = match token.kind {
            Kind::Keyword(kw) => kw.is_reserved(),
            _ => false,
        };

        if is_reserved && !Cond::Production.check(ltok)
        // TODO: gr_exprs.Expr
        {
            // Another token followed by a reserved keyword:
            // likely an attempt to use keyword as identifier
            let token_text = &token.text;
            let msg = format!("Unexpected keyword {token_text}");
            let _details = format!(
                "Token {token_text} is a reserved keyword and cannot be used as an identifier"
            );
            let _hint = format!(
                "Use a different identifier or quote the name with backticks: `{token_text}`"
            );
            return Some(Error {
                message: msg,
                span: token.span,
            });
        }

        None
    }

    /// Look at the parsing stack and use tokens and non-terminals
    /// to infer the parser rule when the error occurred.
    fn get_rule(&self) -> Option<(usize, ParserRule)> {
        // Check if we're in the `FOR x IN <bad_token>` situation
        if self.compare_stack(
            &[
                Cond::Terminal(Kind::Keyword(Keyword("for"))),
                Cond::Production, // TODO gr_exprs.Identifier,
                Cond::Terminal(Kind::Keyword(Keyword("in"))),
                Cond::Terminal(Kind::Less),
                Cond::Production, // TODO gr_exprs.FullTypeExpr,
                Cond::Terminal(Kind::Greater),
            ],
            1,
        ) {
            return Some((4, ParserRule::ForIterator));
        }

        if self.compare_stack(
            &[
                Cond::Terminal(Kind::Keyword(Keyword("for"))),
                Cond::Production, // TODO gr_exprs.Identifier,
                Cond::Terminal(Kind::Keyword(Keyword("in"))),
                Cond::Production, // gr_exprs.AtomicExpr
            ],
            1,
        ) {
            return Some((2, ParserRule::ForIterator));
        }

        if self.compare_stack(
            &[
                Cond::Terminal(Kind::Keyword(Keyword("for"))),
                Cond::Production, // TODO gr_exprs.Identifier,
                Cond::Terminal(Kind::Keyword(Keyword("in"))),
                Cond::Production, // gr_exprs.BaseAtomicExpr
            ],
            0,
        ) {
            return Some((1, ParserRule::ForIterator));
        }

        if self.compare_stack(
            &[
                Cond::Terminal(Kind::Keyword(Keyword("for"))),
                Cond::Production, // TODO gr_exprs.Identifier,
                Cond::Terminal(Kind::Keyword(Keyword("in"))),
            ],
            0,
        ) {
            return Some((0, ParserRule::ForIterator));
        }

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
        );

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
                    if need_match && Cond::Terminal(Kind::CloseBrace).check(ltok) {
                        // This is matched, while we're looking
                        // for unmatched braces.
                        need_match = false;
                    } else if Cond::Production.check_opt(prevel) {
                        // TODO: gr_commondl.OptExtending
                        // This is some SDL/DDL
                        return Some((i, ParserRule::Definition));
                    } else if prevel.map_or(false, |prevel| {
                        Cond::Production.check(prevel) // gr_exprs.Expr
                            ||                            (
                                Cond::Terminal(Kind::Colon).check(prevel)
                                &&
                                Cond::Production.check_opt(prevel.parent) // gr_exprs.ShapePointer
                            )
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
                    if need_match && Cond::Terminal(Kind::CloseParen).check(ltok) {
                        // This is matched, while we're looking
                        // for unmatched parentheses.
                        need_match = false
                    } else if Cond::Production.check_opt(prevel) {
                        // gr_exprs.NodeName
                        return Some((i, ParserRule::ListOfArguments));
                    } else if Cond::AnyOf(vec![
                        Cond::Terminal(Kind::Keyword(Keyword("for"))),
                        Cond::Terminal(Kind::Keyword(Keyword("select"))),
                        Cond::Terminal(Kind::Keyword(Keyword("update"))),
                        Cond::Terminal(Kind::Keyword(Keyword("delete"))),
                        Cond::Terminal(Kind::Keyword(Keyword("insert"))),
                        Cond::Terminal(Kind::Keyword(Keyword("for"))),
                    ])
                    .check_opt(nextel)
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

                    if need_match && Cond::Terminal(Kind::CloseBracket).check(ltok) {
                        // This is matched, while we're looking
                        // for unmatched brackets.
                        need_match = false
                    } else if Cond::Production.check_opt(prevel)
                    // gr_exprs.Expr
                    {
                        return Some((i, ParserRule::ArraySlice));
                    } else {
                        return Some((i, ParserRule::Array));
                    }
                }

                _ => {}
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
    fn compare_stack<'a>(&self, expected: &[Cond], top_offset: usize) -> bool {
        let mut current = self.get_from_top(top_offset);

        for validator in expected.iter().rev() {
            let Some(cur) = current else {
                return false;
            };
            if !validator.check(cur) {
                return false;
            }

            current = cur.parent;
        }
        true
    }
}

/// Condition for a stack node. An easier way to match stack node kinds.
enum Cond {
    Terminal(Kind),
    Production,
    AnyOf(Vec<Cond>),
}

impl Cond {
    fn check<'a>(&self, node: &StackNode<'a>) -> bool {
        match self {
            Cond::Terminal(kind) => match node.value {
                CSTNode::Terminal(Terminal { kind: k, .. }) if k == kind => true,
                _ => false,
            },
            Cond::Production => match node.value {
                CSTNode::Production(_) => true,
                _ => false,
            },
            Cond::AnyOf(options) => options.iter().any(|v| v.check(node)),
        }
    }

    fn check_opt<'a>(&self, node: Option<&StackNode<'a>>) -> bool {
        node.map_or(false, |x| self.check(x))
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
