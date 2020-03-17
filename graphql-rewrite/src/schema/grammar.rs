use combine::{parser, ParseResult, Parser};
use combine::easy::{Error, Errors};
use combine::error::StreamError;
use combine::combinator::{many, many1, eof, optional, position, choice};
use combine::combinator::{sep_by1};

use crate::tokenizer::{Kind as T, Token, TokenStream};
use crate::helpers::{punct, ident, kind, name};
use crate::common::{directives, string, default_value, parse_type, Text};
use crate::schema::error::{ParseError};
use crate::schema::ast::*;


pub fn schema<'a, S>(input: &mut TokenStream<'a>)
    -> ParseResult<SchemaDefinition<'a, S>, TokenStream<'a>>
    where S: Text<'a>,
{
    (
        position().skip(ident("schema")),
        parser(directives),
        punct("{")
            .with(many((
                kind(T::Name).skip(punct(":")),
                name::<'a, S>(),
            )))
            .skip(punct("}")),
    )
    .flat_map(|(position, directives, operations): (_, _, Vec<(Token, _)>)| {
        let mut query = None;
        let mut mutation = None;
        let mut subscription = None;
        let mut err = Errors::empty(position);
        for (oper, type_name) in operations {
            match oper.value {
                "query" if query.is_some() => {
                    err.add_error(Error::unexpected_static_message(
                        "duplicate `query` operation"));
                }
                "query" => {
                    query = Some(type_name);
                }
                "mutation" if mutation.is_some() => {
                    err.add_error(Error::unexpected_static_message(
                        "duplicate `mutation` operation"));
                }
                "mutation" => {
                    mutation = Some(type_name);
                }
                "subscription" if subscription.is_some() => {
                    err.add_error(Error::unexpected_static_message(
                        "duplicate `subscription` operation"));
                }
                "subscription" => {
                    subscription = Some(type_name);
                }
                _ => {
                    err.add_error(Error::unexpected_token(oper));
                    err.add_error(
                        Error::expected_static_message("query"));
                    err.add_error(
                        Error::expected_static_message("mutation"));
                    err.add_error(
                        Error::expected_static_message("subscription"));
                }
            }
        }
        if !err.errors.is_empty() {
            return Err(err);
        }
        Ok(SchemaDefinition {
            position, directives, query, mutation, subscription,
        })
    })
    .parse_stream(input)
}

pub fn scalar_type<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<ScalarType<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    (
        position(),
        ident("scalar").with(name::<'a, T>()),
        parser(directives),
    )
        .map(|(position, name, directives)| {
            ScalarType { position, description: None, name, directives }
        })
        .parse_stream(input)
}

pub fn scalar_type_extension<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<ScalarTypeExtension<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    (
        position(),
        ident("scalar").with(name::<'a, T>()),
        parser(directives),
    )
    .flat_map(|(position, name, directives)| {
        if directives.is_empty() {
            let mut e = Errors::empty(position);
            e.add_error(Error::expected_static_message(
                "Scalar type extension should contain at least \
                 one directive."));
            return Err(e);
        }
        Ok(ScalarTypeExtension { position, name, directives })
    })
    .parse_stream(input)
}

pub fn implements_interfaces<'a, X>(input: &mut TokenStream<'a>)
    -> ParseResult<Vec<X::Value>, TokenStream<'a>>
    where X: Text<'a>,
{
    optional(
        ident("implements")
        .skip(optional(punct("&")))
        .with(sep_by1(name::<'a, X>(), punct("&")))
    )
        .map(|opt| opt.unwrap_or_else(Vec::new))
        .parse_stream(input)
}

pub fn input_value<'a, X>(input: &mut TokenStream<'a>)
    -> ParseResult<InputValue<'a, X>, TokenStream<'a>>
    where X: Text<'a>,
{
    (
        position(),
        optional(parser(string)),
        name::<'a, X>(),
        punct(":").with(parser(parse_type)),
        optional(punct("=").with(parser(default_value))),
        parser(directives),
    )
    .map(|(position, description, name, value_type, default_value, directives)|
    {
        InputValue {
            position, description, name, value_type, default_value, directives,
        }
    })
    .parse_stream(input)
}

pub fn arguments_definition<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<Vec<InputValue<'a, T>>, TokenStream<'a>>
    where T: Text<'a>,
{
    optional(punct("(").with(many1(parser(input_value))).skip(punct(")")))
    .map(|v| v.unwrap_or_else(Vec::new))
    .parse_stream(input)
}

pub fn field<'a, S>(input: &mut TokenStream<'a>)
    -> ParseResult<Field<'a, S>, TokenStream<'a>>
    where S: Text<'a>,
{
    (
        position(),
        optional(parser(string)),
        name::<'a, S>(),
        parser(arguments_definition),
        punct(":").with(parser(parse_type)),
        parser(directives),
    )
    .map(|(position, description, name, arguments, field_type, directives)| {
        Field {
            position, description, name, arguments, field_type, directives
        }
    })
    .parse_stream(input)
}

pub fn fields<'a, S>(input: &mut TokenStream<'a>)
    -> ParseResult<Vec<Field<'a, S>>, TokenStream<'a>>
    where S: Text<'a>,
{
    optional(punct("{").with(many1(parser(field))).skip(punct("}")))
    .map(|v| v.unwrap_or_else(Vec::new))
    .parse_stream(input)
}


pub fn object_type<'a, S>(input: &mut TokenStream<'a>)
    -> ParseResult<ObjectType<'a, S>, TokenStream<'a>>
    where S: Text<'a>,
{
    (
        position(),
        ident("type").with(name::<'a, S>()),
        parser(implements_interfaces::<S>),
        parser(directives),
        parser(fields),
    )
        .map(|(position, name, interfaces, directives, fields)| {
            ObjectType {
                position, name, directives, fields,
                implements_interfaces: interfaces,
                description: None,  // is filled in described_definition
            }
        })
        .parse_stream(input)
}

pub fn object_type_extension<'a, S>(input: &mut TokenStream<'a>)
    -> ParseResult<ObjectTypeExtension<'a, S>, TokenStream<'a>>
    where S: Text<'a>,
{
    (
        position(),
        ident("type").with(name::<'a, S>()),
        parser(implements_interfaces::<S>),
        parser(directives),
        parser(fields),
    )
        .flat_map(|(position, name, interfaces, directives, fields)| {
            if interfaces.is_empty() && directives.is_empty() &&
                fields.is_empty()
            {
                let mut e = Errors::empty(position);
                e.add_error(Error::expected_static_message(
                    "Object type extension should contain at least \
                     one interface, directive or field."));
                return Err(e);
            }
            Ok(ObjectTypeExtension {
                position, name, directives, fields,
                implements_interfaces: interfaces,
            })
        })
        .parse_stream(input)
}

pub fn interface_type<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<InterfaceType<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    (
        position(),
        ident("interface").with(name::<'a, T>()),
        parser(directives),
        parser(fields),
    )
        .map(|(position, name, directives, fields)| {
            InterfaceType {
                position, name, directives, fields,
                description: None,  // is filled in described_definition
            }
        })
        .parse_stream(input)
}

pub fn interface_type_extension<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<InterfaceTypeExtension<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    (
        position(),
        ident("interface").with(name::<'a, T>()),
        parser(directives),
        parser(fields),
    )
        .flat_map(|(position, name, directives, fields)| {
            if directives.is_empty() && fields.is_empty() {
                let mut e = Errors::empty(position);
                e.add_error(Error::expected_static_message(
                    "Interface type extension should contain at least \
                     one directive or field."));
                return Err(e);
            }
            Ok(InterfaceTypeExtension {
                position, name, directives, fields,
            })
        })
        .parse_stream(input)
}

pub fn union_members<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<Vec<T::Value>, TokenStream<'a>>
    where T: Text<'a>,
{
    optional(punct("|"))
    .with(sep_by1(name::<'a, T>(), punct("|")))
    .parse_stream(input)
}

pub fn union_type<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<UnionType<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    (
        position(),
        ident("union").with(name::<'a, T>()),
        parser(directives),
        optional(punct("=").with(parser(union_members::<T>))),
    )
    .map(|(position, name, directives, types)| {
        UnionType {
            position, name, directives,
            types: types.unwrap_or_else(Vec::new),
            description: None,  // is filled in described_definition
        }
    })
    .parse_stream(input)
}

pub fn union_type_extension<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<UnionTypeExtension<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    (
        position(),
        ident("union").with(name::<'a, T>()),
        parser(directives),
        optional(punct("=").with(parser(union_members::<T>))),
    )
    .flat_map(|(position, name, directives, types)| {
        if directives.is_empty() && types.is_none() {
            let mut e = Errors::empty(position);
            e.add_error(Error::expected_static_message(
                "Union type extension should contain at least \
                 one directive or type."));
            return Err(e);
        }
        Ok(UnionTypeExtension {
            position, name, directives,
            types: types.unwrap_or_else(Vec::new),
        })
    })
    .parse_stream(input)
}

pub fn enum_values<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<Vec<EnumValue<'a, T>>, TokenStream<'a>>
    where T: Text<'a>,
{
    punct("{")
    .with(many1(
        (
            position(),
            optional(parser(string)),
            name::<'a, T>(),
            parser(directives),
        )
        .map(|(position, description, name, directives)| {
            EnumValue { position, description, name, directives }
        })
    ))
    .skip(punct("}"))
    .parse_stream(input)
}

pub fn enum_type<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<EnumType<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    (
        position(),
        ident("enum").with(name::<'a, T>()),
        parser(directives),
        optional(parser(enum_values)),
    )
    .map(|(position, name, directives, values)| {
        EnumType {
            position, name, directives,
            values: values.unwrap_or_else(Vec::new),
            description: None,  // is filled in described_definition
        }
    })
    .parse_stream(input)
}

pub fn enum_type_extension<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<EnumTypeExtension<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    (
        position(),
        ident("enum").with(name::<'a, T>()),
        parser(directives),
        optional(parser(enum_values)),
    )
    .flat_map(|(position, name, directives, values)| {
        if directives.is_empty() && values.is_none() {
            let mut e = Errors::empty(position);
            e.add_error(Error::expected_static_message(
                "Enum type extension should contain at least \
                 one directive or value."));
            return Err(e);
        }
        Ok(EnumTypeExtension {
            position, name, directives,
            values: values.unwrap_or_else(Vec::new),
        })
    })
    .parse_stream(input)
}

pub fn input_fields<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<Vec<InputValue<'a, T>>, TokenStream<'a>>
    where T: Text<'a>,
{
    optional(punct("{").with(many1(parser(input_value))).skip(punct("}")))
    .map(|v| v.unwrap_or_else(Vec::new))
    .parse_stream(input)
}

pub fn input_object_type<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<InputObjectType<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    (
        position(),
        ident("input").with(name::<'a, T>()),
        parser(directives),
        parser(input_fields),
    )
        .map(|(position, name, directives, fields)| {
            InputObjectType {
                position, name, directives, fields,
                description: None,  // is filled in described_definition
            }
        })
        .parse_stream(input)
}

pub fn input_object_type_extension<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<InputObjectTypeExtension<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    (
        position(),
        ident("input").with(name::<'a, T>()),
        parser(directives),
        parser(input_fields),
    )
        .flat_map(|(position, name, directives, fields)| {
            if directives.is_empty() && fields.is_empty() {
                let mut e = Errors::empty(position);
                e.add_error(Error::expected_static_message(
                    "Input object type extension should contain at least \
                     one directive or field."));
                return Err(e);
            }
            Ok(InputObjectTypeExtension {
                position, name, directives, fields,
            })
        })
        .parse_stream(input)
}

pub fn directive_locations<'a>(input: &mut TokenStream<'a>)
    -> ParseResult<Vec<DirectiveLocation>, TokenStream<'a>>
{
    optional(
        optional(punct("|"))
        .with(sep_by1(
            kind(T::Name)
                .and_then(|tok| tok.value.parse::<DirectiveLocation>()),
            punct("|")))
    )
        .map(|opt| opt.unwrap_or_else(Vec::new))
        .parse_stream(input)
}

pub fn directive_definition<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<DirectiveDefinition<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    (
        position(),
        ident("directive").and(punct("@")).with(name::<'a, T>()),
        parser(arguments_definition),
        ident("on").with(parser(directive_locations)),
    )
        .map(|(position, name, arguments, locations)| {
            DirectiveDefinition {
                position, name, arguments, locations,
                description: None,  // is filled in described_definition
            }
        })
        .parse_stream(input)
}

pub fn described_definition<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<Definition<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    use self::TypeDefinition::*;
    (
        optional(parser(string)),
        choice((
            choice((
                parser(scalar_type).map(Scalar),
                parser(object_type).map(Object),
                parser(interface_type).map(Interface),
                parser(union_type).map(Union),
                parser(enum_type).map(Enum),
                parser(input_object_type).map(InputObject),
            )).map(Definition::TypeDefinition),
            parser(directive_definition).map(Definition::DirectiveDefinition),
        ))
    )
        // We can't set description inside type definition parser, because
        // that means parser will need to backtrace, and that in turn
        // means that error reporting is bad (along with performance)
        .map(|(descr, mut def)| {
            use crate::schema::ast::TypeDefinition::*;
            use crate::schema::ast::Definition::*;
            use crate::schema::ast::Definition::{TypeDefinition as T};
            match def {
                T(Scalar(ref mut s)) => s.description = descr,
                T(Object(ref mut o)) => o.description = descr,
                T(Interface(ref mut i)) => i.description = descr,
                T(Union(ref mut u)) => u.description = descr,
                T(Enum(ref mut e)) => e.description = descr,
                T(InputObject(ref mut o)) => o.description = descr,
                DirectiveDefinition(ref mut d) => d.description = descr,
                SchemaDefinition(_) => unreachable!(),
                TypeExtension(_) => unreachable!(),
            }
            def
        })
        .parse_stream(input)
}

pub fn type_extension<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<TypeExtension<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    ident("extend")
    .with(choice((
        parser(scalar_type_extension).map(TypeExtension::Scalar),
        parser(object_type_extension).map(TypeExtension::Object),
        parser(interface_type_extension).map(TypeExtension::Interface),
        parser(union_type_extension).map(TypeExtension::Union),
        parser(enum_type_extension).map(TypeExtension::Enum),
        parser(input_object_type_extension).map(TypeExtension::InputObject),
    )))
    .parse_stream(input)
}


pub fn definition<'a, T>(input: &mut TokenStream<'a>)
    -> ParseResult<Definition<'a, T>, TokenStream<'a>>
    where T: Text<'a>,
{
    choice((
        parser(schema).map(Definition::SchemaDefinition),
        parser(type_extension).map(Definition::TypeExtension),
        parser(described_definition),
    )).parse_stream(input)
}

/// Parses a piece of schema language and returns an AST
pub fn parse_schema<'a, T>(s: &'a str) -> Result<Document<'a, T>, ParseError>
    where T: Text<'a>,
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
    use crate::schema::grammar::*;
    use super::parse_schema;

    fn ast(s: &str) -> Document<String> {
        parse_schema::<String>(&s).unwrap().to_owned()
    }

    #[test]
    fn one_field() {
        assert_eq!(ast("schema { query: Query }"), Document {
            definitions: vec![
                Definition::SchemaDefinition(
                    SchemaDefinition {
                        position: Pos { line: 1, column: 1,
                                        character: 0, token: 0},
                        directives: vec![],
                        query: Some("Query".into()),
                        mutation: None,
                        subscription: None
                    }
                )
            ],
        });
    }
}
