
use std::collections::hash_map::RandomState;
use std::collections::{HashSet};
use std::ops::Range;

use chumsky::error::Simple;
use chumsky::prelude::*;
use chumsky::{select, Parser};
use indexmap::IndexMap;

use crate::ast::*;
use crate::keywords::{CURRENT_RESERVED_KEYWORDS, PARTIAL_RESERVED_KEYWORDS, UNRESERVED_KEYWORDS};
use crate::tokenizer::{Kind as Token, Token as TokenData};
use super::util::*;


pub fn expr_stmt<'a>() -> impl Parser<TokenData<'a>, Expr, Error = Simple<TokenData<'a>>> {
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
) -> impl Parser<TokenData<'a>, OptionallyAliasedExpr, Error = Simple<TokenData<'a>>> {
    identifier()
        .then_ignore(token(Token::Assign))
        .or_not()
        .then(expr().map(Box::new))
        .map(|(alias, expr)| OptionallyAliasedExpr { alias, expr })
}

fn identifier<'a>() -> impl Parser<TokenData<'a>, String, Error = Simple<TokenData<'a>>> {
    let unreserved: HashSet<_, RandomState> = HashSet::from_iter(UNRESERVED_KEYWORDS);
    let unreserved_keyword = select! {
        TokenData { kind: Token::Keyword, value } if unreserved.contains(&value) => value
    };

    token(Token::Ident)
        .or(unreserved_keyword)
        .map(|n| n.to_string())
}

fn ptr_identifier<'a>() -> impl Parser<TokenData<'a>, String, Error = Simple<TokenData<'a>>> {
    let partial_reserved: HashSet<_, RandomState> = HashSet::from_iter(PARTIAL_RESERVED_KEYWORDS);
    let partial_reserved_keyword = select! {
        TokenData { kind: Token::Keyword, value } if partial_reserved.contains(&value) => value
    };

    token(Token::Ident)
        .or(partial_reserved_keyword)
        .map(|n| n.to_string())
}

fn any_identifier<'a>() -> impl Parser<TokenData<'a>, String, Error = Simple<TokenData<'a>>> {
    let reserved: HashSet<_, RandomState> = HashSet::from_iter(CURRENT_RESERVED_KEYWORDS);
    let reserved_keyword = select! {
        TokenData { kind: Token::Keyword, value } if reserved.contains(&value) => value
    }
    .try_map(|value, span| {
        if value.starts_with("__") && value.ends_with("__") {
            // There are a few reserved keywords like __std__ and __subject__
            // that can be used in paths but are prohibited to be used
            // anywhere else. So just as the tokenizer prohibits using
            // __names__ in general, we enforce the rule here for the
            // few remaining reserved __keywords__.

            // TODO: add error message "identifiers surrounded by double underscores are forbidden",
            Err(Simple::expected_input_found(span, [], None))
        } else {
            Ok(value)
        }
    });

    token(Token::Ident)
        .or(reserved_keyword)
        .map(|n| n.to_string())
}

/// Has at least one element.
fn qualified_name<'a>() -> impl Parser<TokenData<'a>, Vec<String>, Error = Simple<TokenData<'a>>> {
    let coloned_idents = any_identifier().separated_by(token(Token::Namespace));

    token(Token::Ident)
        .map(str::to_string)
        .or(ident("__std__"))
        .then_ignore(token(Token::Namespace))
        .then(coloned_idents)
        .map(|(first, following)| [vec![first], following].concat())
}

/// Has at least one element.
fn base_name<'a>() -> impl Parser<TokenData<'a>, Vec<String>, Error = Simple<TokenData<'a>>> {
    identifier().map(|x| vec![x]).or(qualified_name())
}

fn node_name<'a>() -> impl Parser<TokenData<'a>, ObjectRef, Error = Simple<TokenData<'a>>> {
    base_name().map(|mut chunks| {
        let name = chunks.pop().unwrap();
        let module = if chunks.is_empty() {
            None
        } else {
            Some(chunks.join("::"))
        };
        ObjectRef {
            name,
            module,
            itemclass: None,
        }
    })
}

/// Generic short of fully-qualified name.
///
/// This name is safe to be used in most DDL and SDL definitions.
fn ptr_node_name<'a>() -> impl Parser<TokenData<'a>, ObjectRef, Error = Simple<TokenData<'a>>> {
    ptr_name().map(|mut parts| {
        let name = parts.pop().unwrap();
        ObjectRef {
            name,
            module: if parts.is_empty() {
                None
            } else {
                Some(parts.join("::"))
            },
            itemclass: None,
        }
    })
}

#[allow(dead_code)]
fn ptr_name<'a>() -> impl Parser<TokenData<'a>, Vec<String>, Error = Simple<TokenData<'a>>> {
    ptr_identifier().map(|x| vec![x]).or(qualified_name())
}

#[allow(dead_code)]
fn module_name<'a>() -> impl Parser<TokenData<'a>, Vec<String>, Error = Simple<TokenData<'a>>> {
    let dotted_idents = any_identifier()
        .separated_by(token(Token::Dot))
        .map(|x| x.join("."));

    dotted_idents.separated_by(token(Token::Namespace))
}

/// NOTE: A non-qualified name that can be an identifier or
/// PARTIAL_RESERVED_KEYWORD.
///
/// This name is used as part of paths after the DOT as well as in
/// definitions after LINK/POINTER. It can be an identifier including
/// PARTIAL_RESERVED_KEYWORD and does not need to be quoted or
/// parenthesized.
fn path_node_name<'a>() -> impl Parser<TokenData<'a>, ObjectRef, Error = Simple<TokenData<'a>>> {
    ptr_identifier().map(|name| ObjectRef {
        name,
        module: None,
        itemclass: None,
    })
}

fn expr<'a>() -> impl Parser<TokenData<'a>, Expr, Error = Simple<TokenData<'a>>> {
    recursive(|tree| {
        let argument = token(Token::Argument)
            .map(str::to_string)
            .map(|name| Parameter { name })
            .map(ExprKind::Parameter)
            .boxed();

        let source = ident("__source__").map(|name| SpecialAnchorKind::Source(Source { name }));
        let subject = ident("__subject__").map(|name| SpecialAnchorKind::Subject(Subject { name }));

        let anchors1 = source
            .or(subject)
            .map(|kind| AnchorKind::SpecialAnchor(SpecialAnchor { kind: Some(kind) }))
            .map(prepend("".to_string()));

        let anchors2 = choice((ident("__new__"), ident("__old__"), ident("__specified__"))).map(
            append(AnchorKind::SpecialAnchor(SpecialAnchor { kind: None })),
        );

        let anchors = anchors1
            .or(anchors2)
            .map(|(name, kind)| Anchor { name, kind })
            .map(ExprKind::Anchor);

        let tuple_or_paren = token(Token::OpenParen)
            .ignore_then(
                tree.clone()
                    .then(
                        token(Token::Comma)
                            .ignore_then(tree.clone().separated_by(token(Token::Comma)))
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

        let func_call_arg_expr = tree
            .clone()
            .map(|x| (None, x))
            .or(any_identifier()
                .map(Some)
                .then_ignore(token(Token::Assign))
                .then(tree.clone()))
            .or(argument
                .clone()
                .then(token(Token::Assign))
                .then(tree.clone())
                .try_map(|_, span| {
                    // TODO: add error message
                    // if kids[0].val[1].isdigit():
                    //     raise errors.EdgeQLSyntaxError(
                    //         f"numeric named arguments are not supported",
                    //         context=kids[0].context)
                    // else:
                    //     raise errors.EdgeQLSyntaxError(
                    //         f"named arguments do not need a '$' prefix, "
                    //         f"rewrite as '{kids[0].val[1:]} := ...'",
                    //         context=kids[0].context)
                    Err(Simple::expected_input_found(span, [], None))
                }));
        // TODO: FuncCallArgExpr OptFilterClause_OptSortClause
        let func_expr = node_name()
            .then_ignore(token(Token::OpenParen))
            .then(
                func_call_arg_expr
                    .separated_by(token(Token::Comma))
                    .allow_trailing(),
            )
            .then_ignore(token(Token::CloseParen))
            .try_map(|(name, func_args), span| {
                let func = if let Some(module) = name.module {
                    FunctionCallFunc::Tuple((module, name.name))
                } else {
                    FunctionCallFunc::str(name.name)
                };

                let mut last_named_seen = None;
                let mut args = Vec::new();
                let mut kwargs = IndexMap::new();
                for (arg_name, arg) in func_args {
                    if let Some(arg_name) = arg_name {
                        if kwargs.contains_key(&arg_name) {
                            // TODO: error msg "duplicate named argument `{arg_name}`"
                            return Err(Simple::expected_input_found(span, [], None));
                        }

                        last_named_seen = Some(arg_name.clone());
                        kwargs.insert(arg_name, Box::new(arg));
                    } else {
                        if last_named_seen.is_some() {
                            // TODO: error msg
                            // "positional argument after named argument `{last_named_seen}`"
                            return Err(Simple::expected_input_found(span, [], None));
                        }
                        args.push(Box::new(arg));
                    }
                }

                Ok(ExprKind::FunctionCall(FunctionCall {
                    func,
                    args,
                    kwargs,
                    window: None,
                }))
            });

        let path_step = path_step();

        // TODO for BaseAtomicExpr:
        // - { ... }
        // - NamedTuple
        // - Collection
        // - Set
        // - DOT_FCONST

        let base_atomic_expr = choice((
            argument,
            constant(),
            tuple_or_paren,
            func_expr,
            anchors,
            path_step,
        ));

        let term = base_atomic_expr.map(|kind| Expr { kind });

        // TODO for Expr:
        // def reduce_Path(self, *kids):
        //     self.val = kids[0].val

        // TypeIntersection

        // def reduce_Expr_Shape(self, *kids):
        //     self.val = qlast.Shape(expr=kids[0].val, elements=kids[1].val)

        // indirection
        let term = term.boxed();
        let term = term
            .clone()
            .then(
                choice((
                    // term '[' term ']'
                    // term '[' term ':' ']'
                    // term '[' term ':' term ']'
                    term.clone()
                        .then(
                            token(Token::Colon)
                                .ignore_then(term.clone().or_not())
                                .or_not(),
                        )
                        .map(|(start, following)| {
                            let start = Box::new(start);

                            if let Some(following) = following {
                                let start = Some(start);
                                let stop = following.map(Box::new);

                                IndirectionIndirection::Slice(Slice { start, stop })
                            } else {
                                IndirectionIndirection::Index(Index { index: start })
                            }
                        }),
                    // term '[' ':' term ']'
                    token(Token::Colon).ignore_then(term).map(|stop| {
                        IndirectionIndirection::Slice(Slice {
                            start: None,
                            stop: Some(Box::new(stop)),
                        })
                    }),
                ))
                .delimited_by(
                    token(Token::OpenBracket).boxed(),
                    token(Token::CloseBracket).boxed(),
                )
                .or_not(),
            )
            .map(|(mut expr, indirection_step)| {
                if let Some(step) = indirection_step {
                    if let ExprKind::Indirection(existing) = &mut expr.kind {
                        existing.indirection.push(step);
                        expr
                    } else {
                        Expr {
                            kind: ExprKind::Indirection(Indirection {
                                arg: Box::new(expr),
                                indirection: vec![step],
                            }),
                        }
                    }
                } else {
                    expr
                }
            });

        // TODO: P_SINGLE
        // TODO: P_OPTIONAL
        // TODO: P_MULTI
        // TODO: P_REQUIRED
        // TODO: P_AT
        // TODO: P_DOUBLECOLON
        // TODO: P_GLOBAL
        // TODO: P_DETACHED
        // TODO: P_DOT
        // TODO: P_PAREN
        // TODO: P_BRACKET
        // TODO: P_BRACE
        // TODO: P_TYPECAST
        // TODO: P_POW_OP

        let term = unary_op_parser(term, keyword("distinct"));

        let term = unary_op_parser(term, keyword("exists"));

        // unary op
        let term = term.boxed();
        let term = choice((token(Token::Add), token(Token::Sub)))
            .map(str::to_string)
            .then(term.clone().map(Box::new))
            .map(|(op, mut operand)| {
                // Special case for -<real_const> so that type inference based
                // on literal size works correctly in the case of INT_MIN and
                // friends.
                if op == "-" {
                    if let ExprKind::BaseConstant(BaseConstant {
                        kind: BaseConstantKind::BaseRealConstant(c),
                        ..
                    }) = &mut operand.kind
                    {
                        c.is_negative = true;
                        return operand.kind;
                    }
                }

                ExprKind::UnaryOp(UnaryOp { op, operand })
            })
            .map(|kind| Expr { kind })
            .or(term);

        // TODO: introspect

        let term = binary_op_parser(term, token(Token::Coalesce));

        let term = binary_op_parser(
            term,
            token_choice([Token::Mul, Token::Div, Token::FloorDiv, Token::Modulo]),
        );

        let term = binary_op_parser(term, token_choice([Token::Add, Token::Concat, Token::Sub]));

        // TODO: type expr
        // let term = binary_op_parser(
        //     term,
        //     keyword("is")
        //         .then(keyword("not").or_not())
        //         .map(|(is, not)| if not.is_some() { "is not" } else { is }),
        // );

        let term = binary_op_parser(
            term,
            token_choice([
                Token::GreaterEq,
                Token::LessEq,
                Token::NotEq,
                Token::NotDistinctFrom,
                Token::DistinctFrom,
            ]),
        );

        let term = binary_op_parser(
            term,
            keyword("not")
                .or_not()
                .then(keyword("in"))
                .map(|(not, in_)| if not.is_some() { "not in" } else { in_ }),
        );

        let term = binary_op_parser(
            term,
            keyword("not")
                .or_not()
                .then(keyword("like").or(keyword("ilike")))
                .map(|(not, like)| {
                    if not.is_some() {
                        format!("not {like}")
                    } else {
                        like.to_string()
                    }
                }),
        );

        let term = binary_op_parser(term, token_choice([Token::Less, Token::Greater]));

        let term = binary_op_parser(term, token(Token::Eq));

        let term = unary_op_parser(term, keyword("not"));

        let term = binary_op_parser(term, keyword("and"));

        let term = binary_op_parser(term, keyword("or"));

        let term = term.boxed();
        let term = term
            .clone()
            .then(
                keyword("if")
                    .ignore_then(term.clone())
                    .then_ignore(keyword("then"))
                    .then(term)
                    .repeated(),
            )
            .map(|(first, following)| {
                let mut exprs = vec![first];
                exprs.extend(following.into_iter().flat_map(|(a, b)| [a, b]));
                while exprs.len() >= 3 {
                    let else_expr = Box::new(exprs.pop().unwrap());
                    let condition = Box::new(exprs.pop().unwrap());
                    let if_expr = Box::new(exprs.pop().unwrap());

                    let kind = ExprKind::IfElse(IfElse {
                        condition,
                        if_expr,
                        else_expr,
                    });
                    exprs.push(Expr { kind });
                }
                assert!(exprs.len() == 1);
                exprs.remove(0)
            });

        let term = binary_op_parser(term, keyword("intersect"));

        let term = binary_op_parser(term, choice((keyword("union"), keyword("except"))));

        term
    })
}

fn unary_op_parser<'a, 'b: 'a, Term, Op>(
    term: Term,
    op: Op,
) -> impl Parser<TokenData<'b>, Expr, Error = Simple<TokenData<'b>>> + 'a
where
    Term: Parser<TokenData<'b>, Expr, Error = Simple<TokenData<'b>>> + 'a,
    Op: Parser<TokenData<'b>, &'b str, Error = Simple<TokenData<'b>>> + 'a,
{
    let term = term.boxed();
    op.map(str::to_string)
        .then(term.clone().map(Box::new))
        .map(|(op, operand)| UnaryOp { op, operand })
        .map(ExprKind::UnaryOp)
        .map(|kind| Expr { kind })
        .or(term)
}

pub fn binary_op_parser<'a, 'b: 'a, Term, Op, S: ToString + 'a>(
    term: Term,
    op: Op,
) -> impl Parser<TokenData<'b>, Expr, Error = Simple<TokenData<'b>>> + 'a
where
    Term: Parser<TokenData<'b>, Expr, Error = Simple<TokenData<'b>>> + 'a,
    Op: Parser<TokenData<'b>, S, Error = Simple<TokenData<'b>>> + 'a,
{
    let term = term
        // .map_with_span(|e, s| (e, s))
        .boxed();

    (term.clone())
        .then(op.map(|o| o.to_string()).then(term).repeated())
        .foldl(|left, (op, right)| Expr {
            kind: ExprKind::BinOp(BinOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                rebalanced: false,
                kind: None,
            }),
        })
}

fn path_step<'a>() -> impl Parser<TokenData<'a>, ExprKind, Error = Simple<TokenData<'a>>> {
    let path_step_name = path_node_name()
        .or(ident("__type__").map(|name| ObjectRef {
            name,
            module: None,
            itemclass: None,
        }))
        .boxed();

    let step_dot = token(Token::Dot)
        .ignore_then(
            path_step_name
                .clone()
                .or(token(Token::IntConst)
                    .map(str::to_string)
                    .map(|x| ObjectRef {
                        name: x,
                        module: None,
                        itemclass: None,
                    })),
        )
        .map(|ptr| Ptr {
            ptr,
            direction: Some(">".to_string()).clone(),
            r#type: None,
        });

    let step_backward = token(Token::BackwardLink)
        .ignore_then(path_step_name)
        .map(|ptr| Ptr {
            ptr,
            direction: Some("<".to_string()),
            r#type: None,
        });

    let step_property = token(Token::At)
        .ignore_then(path_node_name())
        .map(move |ptr| Ptr {
            ptr,
            direction: Some(">".to_string()),
            r#type: Some("property".to_string()),
        });

    choice((
        choice((step_dot, step_backward, step_property)).map(|ptr| PathSteps::Ptr(ptr)),
        type_intersection().map(|ti| PathSteps::TypeIntersection(ti)),
        node_name().map(|object_ref| PathSteps::ObjectRef(object_ref)),
    ))
    .map(|step| Path {
        steps: vec![step],
        partial: true,
    })
    .map(ExprKind::Path)
}

fn type_intersection<'a>(
) -> impl Parser<TokenData<'a>, TypeIntersection, Error = Simple<TokenData<'a>>> {
    // '[' IS NodeName ']'
    token(Token::OpenBracket)
        .ignore_then(keyword("is"))
        .ignore_then(full_type_expr())
        .then_ignore(token(Token::CloseBracket))
        .map(|r#type| TypeIntersection { r#type })
}

fn full_type_expr<'a>() -> impl Parser<TokenData<'a>, TypeExpr, Error = Simple<TokenData<'a>>> {
    recursive(|full_type_expr| {
        let pseudo_type_name = choice((
            keyword("anytype").to(PseudoObjectRefKind::AnyType(AnyType {})),
            keyword("anytuple").to(PseudoObjectRefKind::AnyTuple(AnyTuple {})),
        ))
        .map(|kind| BaseObjectRefKind::PseudoObjectRef(PseudoObjectRef { kind }));

        let simple_type_name = pseudo_type_name
            .or(ptr_node_name().map(BaseObjectRefKind::ObjectRef))
            .map(|kind| TypeName {
                maintype: BaseObjectRef { kind },
                subtypes: None,
                dimensions: None,
            });

        // TODO: Raise a DeprecationWarning once we have facility for that.
        let base_string_constant = token(Token::Str)
            .map(|str| BaseConstant {
                value: str.to_string(),
                kind: BaseConstantKind::StringConstant(StringConstant {}),
            })
            .map(|val| TypeExpr {
                kind: Some(TypeExprKind::TypeExprLiteral(TypeExprLiteral { val })),
                name: None,
            });

        let sub_type = choice((
            full_type_expr.clone().map(prepend(None)),
            identifier()
                .map(Some)
                .then_ignore(token(Token::Colon))
                .then(full_type_expr.clone()),
            base_string_constant.map(prepend(None)),
        ))
        .map(|(name, mut type_expr)| {
            if let Some(name) = name {
                type_expr.name = Some(name);
            }
            type_expr
        });

        let collection_type_name = node_name()
            .then_ignore(token(Token::OpenBracket))
            .then(
                sub_type
                    .separated_by(token(Token::Comma))
                    .allow_trailing()
                    .try_map(validate_sub_types),
            )
            .then_ignore(token(Token::CloseBracket))
            .map(|(maintype, subtypes)| TypeName {
                maintype: BaseObjectRef {
                    kind: BaseObjectRefKind::ObjectRef(maintype),
                },
                subtypes: Some(subtypes),
                dimensions: None,
            });

        let type_name = simple_type_name
            .or(collection_type_name)
            .map(|type_name| TypeExpr {
                name: None,
                kind: Some(TypeExprKind::TypeName(type_name)),
            });

        let paren = full_type_expr.delimited_by(
            token(Token::OpenParen).boxed(),
            token(Token::CloseParen).boxed(),
        );

        let term = type_name.or(paren);

        let term = binary_type_op_parser(term, token(Token::Ampersand));

        let term = binary_type_op_parser(term, token(Token::Pipe));

        // TODO: typeof

        term
    })
}

pub fn binary_type_op_parser<'a, 'b: 'a, Term, Op, S: ToString + 'a>(
    term: Term,
    op: Op,
) -> impl Parser<TokenData<'b>, TypeExpr, Error = Simple<TokenData<'b>>> + 'a
where
    Term: Parser<TokenData<'b>, TypeExpr, Error = Simple<TokenData<'b>>> + 'a,
    Op: Parser<TokenData<'b>, S, Error = Simple<TokenData<'b>>> + 'a,
{
    let term = term
        // .map_with_span(|e, s| (e, s))
        .boxed();

    (term.clone())
        .then(op.map(|o| o.to_string()).then(term).repeated())
        .foldl(|left, (op, right)| TypeExpr {
            name: None,
            kind: Some(TypeExprKind::TypeOp(TypeOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })),
        })
}

fn validate_sub_types<'a>(
    sub_types: Vec<TypeExpr>,
    span: Range<usize>,
) -> Result<Vec<TypeExpr>, Simple<TokenData<'a>>> {
    let mut has_nonstrval = false;
    let mut has_strval = false;
    let mut has_items = false;
    for el in &sub_types {
        match &el.kind {
            Some(TypeExprKind::TypeExprLiteral(_)) => {
                has_strval = true;
            }
            Some(TypeExprKind::TypeName(_)) => {
                if el.name.is_some() {
                    has_items = true;
                } else {
                    has_nonstrval = true;
                }
            }
            _ => {}
        }
    }

    if (has_nonstrval || has_items) && has_strval {
        // Prohibit cases like `tuple<a: int64, 'aaaa'>` and
        // `enum<bbbb, 'aaaa'>`
        return Err(Simple::custom(
            span,
            "mixing string type literals and type names is not supported",
        ));
    }

    if has_items && has_nonstrval {
        // Prohibit cases like `tuple<a: int64, int32>`
        return Err(Simple::custom(
            span,
            "mixing named and unnamed subtype declarations is not supported",
        ));
    }
    Ok(sub_types)
}

fn constant<'a>() -> impl Parser<TokenData<'a>, ExprKind, Error = Simple<TokenData<'a>>> {
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
