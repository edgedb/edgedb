use combine::{parser, ParseResult, Parser};
use combine::combinator::{many1, eof, optional, position};

use crate::common::{Directive};
use crate::common::{directives, arguments, default_value, parse_type};
use crate::tokenizer::{TokenStream};
use crate::helpers::{punct, ident, name};
use crate::query::error::{ParseError};
use crate::query::ast::*;

pub fn field<'a, S>(input: &mut TokenStream<'a>)
    -> ParseResult<Field<'a, S>, TokenStream<'a>>
    where S: Text<'a>
{
    (
        position(),
        name::<'a, S>(),
        optional(punct(":").with(name::<'a, S>())),
        parser(arguments),
        parser(directives),
        optional(parser(selection_set)),
    ).map(|(position, name_or_alias, opt_name, arguments, directives, sel)| {
        let (name, alias) = match opt_name {
            Some(name) => (name, Some(name_or_alias)),
            None => (name_or_alias, None),
        };
        Field {
            position, name, alias, arguments, directives,
            selection_set: sel.unwrap_or_else(|| {
                SelectionSet {
                    span: (position, position),
                    items: Vec::new(),
                }
            }),
        }
    })
    .parse_stream(input)
}

pub fn selection<'a, S>(input: &mut TokenStream<'a>)
    -> ParseResult<Selection<'a, S>, TokenStream<'a>>
    where S: Text<'a>
{
    parser(field).map(Selection::Field)
    .or(punct("...").with((
                position(),
                optional(ident("on").with(name::<'a, S>()).map(TypeCondition::On)),
                parser(directives),
                parser(selection_set),
            ).map(|(position, type_condition, directives, selection_set)| {
                InlineFragment { position, type_condition,
                                 selection_set, directives }
            })
            .map(Selection::InlineFragment)
        .or((position(),
             name::<'a, S>(),
             parser(directives),
            ).map(|(position, fragment_name, directives)| {
                FragmentSpread { position, fragment_name, directives }
            })
            .map(Selection::FragmentSpread))
    ))
    .parse_stream(input)
}

pub fn selection_set<'a, S>(input: &mut TokenStream<'a>)
    -> ParseResult<SelectionSet<'a, S>, TokenStream<'a>>
    where S: Text<'a>,
{
    (
        position().skip(punct("{")),
        many1(parser(selection)),
        position().skip(punct("}")),
    ).map(|(start, items, end)| SelectionSet { span: (start, end), items })
    .parse_stream(input)
}

pub fn query<'a, T: Text<'a>>(input: &mut TokenStream<'a>)
    -> ParseResult<Operation<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    position()
    .skip(ident("query"))
    .and(parser(operation_common))
    .map(|(position, (name, (vdefs, ins_point), directives, selection_set))|
        Operation {
            kind: OperationKind::Query,
            variable_definitions: vdefs,
            insert_variables: ins_point,
            position, name, selection_set, directives,
        })
    .parse_stream(input)
}

/// A set of attributes common to a Query and a Mutation
#[allow(type_alias_bounds)]
type OperationCommon<'a, T: Text<'a>> = (
    Option<T::Value>,
    (Vec<VariableDefinition<'a, T>>, InsertVars),
    Vec<Directive<'a, T>>,
    SelectionSet<'a, T>,
);

pub fn operation_common<'a, T: Text<'a>>(input: &mut TokenStream<'a>)
    -> ParseResult<OperationCommon<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    optional(name::<'a, T>())
    .and(position()
        .and(optional(
            punct("(")
            .with(many1(
                (
                    position(),
                    punct("$").with(name::<'a, T>()).skip(punct(":")),
                    parser(parse_type),
                    optional((
                        position(),
                        punct("=")
                        .with(parser(default_value)),
                        position()
                    )),
                ).map(|(position, name, var_type, def)| {
                    VariableDefinition {
                        position, name, var_type,
                        default_value: def.map(|(s, value, e)| DefaultValue {
                            span: (s, e),
                            value,
                        }),
                    }
                })))
            .and(position())
            .skip(punct(")"))
        )).map(|(position, vars)| {
            vars
            .map(|(v, position)| {
                (v, InsertVars {
                    kind: InsertVarsKind::Normal,
                    position,
                })
            })
            .unwrap_or_else(|| {
                (Vec::new(), InsertVars {
                    kind: InsertVarsKind::Parens,
                    position,
                })
            })
        }))
    .and(parser(directives))
    .and(parser(selection_set))
    .map(|(((a, b), c), d)| (a, b, c, d))
    .parse_stream(input)
}

pub fn mutation<'a, T: Text<'a>>(input: &mut TokenStream<'a>)
    -> ParseResult<Operation<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    position()
    .skip(ident("mutation"))
    .and(parser(operation_common))
    .map(|(position, (name, (vdefs, ins_point), directives, selection_set))|
        Operation {
            kind: OperationKind::Mutation,
            variable_definitions: vdefs,
            insert_variables: ins_point,
            position, name, selection_set, directives,
        })
    .parse_stream(input)
}

pub fn subscription<'a, T: Text<'a>>(input: &mut TokenStream<'a>)
    -> ParseResult<Operation<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    position()
    .skip(ident("subscription"))
    .and(parser(operation_common))
    .map(|(position, (name, (vdefs, ins_point), directives, selection_set))|
        Operation {
            kind: OperationKind::Subscription,
            variable_definitions: vdefs,
            insert_variables: ins_point,
            position, name, selection_set, directives,
        })
    .parse_stream(input)
}

pub fn operation_definition<'a, S>(input: &mut TokenStream<'a>)
    -> ParseResult<Operation<'a, S>, TokenStream<'a>>
    where S: Text<'a>,
{
    position().and(parser(selection_set))
        .map(|(position, selection_set)| Operation {
            kind: OperationKind::ImplicitQuery,
            position: selection_set.span.0,
            name: None,
            variable_definitions: Vec::new(),
            insert_variables: InsertVars {
                kind: InsertVarsKind::Query,
                position,
            },
            directives: Vec::new(),
            selection_set,
        })
    .or(parser(query))
    .or(parser(mutation))
    .or(parser(subscription))
    .parse_stream(input)
}

pub fn fragment_definition<'a, T: Text<'a>>(input: &mut TokenStream<'a>)
    -> ParseResult<FragmentDefinition<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    (
        position().skip(ident("fragment")),
        name::<'a, T>(),
        ident("on").with(name::<'a, T>()).map(TypeCondition::On),
        parser(directives),
        parser(selection_set)
    ).map(|(position, name, type_condition, directives, selection_set)| {
        FragmentDefinition {
            position, name, type_condition, directives, selection_set,
        }
    })
    .parse_stream(input)
}

pub fn definition<'a, S>(input: &mut TokenStream<'a>)
    -> ParseResult<Definition<'a, S>, TokenStream<'a>>
    where S: Text<'a>,
{
    parser(operation_definition).map(Definition::Operation)
    .or(parser(fragment_definition).map(Definition::Fragment))
    .parse_stream(input)
}

/// Parses a piece of query language and returns an AST
pub fn parse_query<'a, S>(s: &'a str) -> Result<Document<'a, S>, ParseError>
    where S: Text<'a>,
{
    let mut tokens = TokenStream::new(s);
    let (doc, _) = many1(parser(definition))
        .map(|d| Document { definitions: d })
        .skip(eof())
        .parse_stream(&mut tokens)
        .map_err(|e| e.into_inner().error)?;

    Ok(doc)
}

#[cfg(test)]
mod test {
    use crate::position::Pos;
    use crate::query::grammar::*;
    use super::parse_query;

    fn ast(s: &str) -> Document<String> {
        parse_query::<String>(&s).unwrap().to_owned()
    }

    #[test]
    fn one_field() {
        assert_eq!(ast("{ a }"), Document {
            definitions: vec![
                Definition::Operation(Operation {
                    kind: OperationKind::ImplicitQuery,
                    position: Pos { line: 1, column: 1,
                                    character: 0, token: 0 },
                    name: None,
                    variable_definitions: Vec::new(),
                    insert_variables: InsertVars {
                        kind: InsertVarsKind::Query,
                        position: Pos { line: 1, column: 1,
                                        character: 0, token: 0},
                    },
                    directives: Vec::new(),
                    selection_set: SelectionSet {
                        span: (Pos { line: 1, column: 1,
                                     character: 0, token: 0 },
                               Pos { line: 1, column: 5,
                                     character: 4, token: 2 }),
                        items: vec![
                            Selection::Field(Field {
                                position: Pos { line: 1, column: 3,
                                                character: 2, token: 1},
                                alias: None,
                                name: "a".into(),
                                arguments: Vec::new(),
                                directives: Vec::new(),
                                selection_set: SelectionSet {
                                    span: (Pos { line: 1, column: 3,
                                                 character: 2, token: 1},
                                           Pos { line: 1, column: 3,
                                                 character: 2, token: 1}),
                                    items: Vec::new()
                                },
                            }),
                        ],
                    }
                })
            ],
        });
    }

    #[test]
    fn builtin_values() {
        assert_eq!(ast("{ a(t: true, f: false, n: null) }"),
            Document {
                definitions: vec![
                    Definition::Operation(Operation {
                        kind: OperationKind::ImplicitQuery,
                        position: Pos { line: 1, column: 1,
                                        character: 0, token: 0},
                        name: None,
                        variable_definitions: Vec::new(),
                        insert_variables: InsertVars {
                            kind: InsertVarsKind::Query,
                            position: Pos { line: 1, column: 1,
                                            character: 0, token: 0},
                        },
                        directives: Vec::new(),
                        selection_set: SelectionSet {
                            span: (Pos { line: 1, column: 1,
                                         character: 0, token: 0},
                                   Pos { line: 1, column: 33,
                                         character: 32, token: 13}),
                            items: vec![
                                Selection::Field(Field {
                                    position: Pos { line: 1, column: 3,
                                                    character: 2, token: 1},
                                    alias: None,
                                    name: "a".into(),
                                    arguments: vec![
                                        ("t".into(),
                                            Value::Boolean(true)),
                                        ("f".into(),
                                            Value::Boolean(false)),
                                        ("n".into(),
                                            Value::Null),
                                    ],
                                    directives: Vec::new(),
                                    selection_set: SelectionSet {
                                        span: (Pos { line: 1, column: 3,
                                                     character: 2, token: 1},
                                               Pos { line: 1, column: 3,
                                                     character: 2, token: 1}),
                                        items: Vec::new()
                                    },
                                }),
                            ],
                        }
                    })
                ],
            });
    }

    #[test]
    fn one_field_roundtrip() {
        assert_eq!(ast("{ a }").to_string(), "{\n  a\n}\n");
    }

    #[test]
    fn large_integer() {
        assert_eq!(ast("{ a(x: 10000000000000000000000000000) }").to_string(),
            "{\n  a(x: 10000000000000000000000000000)\n}\n");
    }
}
