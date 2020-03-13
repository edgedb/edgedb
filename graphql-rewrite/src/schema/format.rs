use std::fmt;

use crate::format::{Displayable, Formatter, Style, format_directives};
use crate::common::Text;

use crate::schema::ast::*;


impl<'a, T> Document<'a, T> 
    where T: Text<'a>,
{
    /// Format a document according to style
    pub fn format(&self, style: &Style) -> String {
        let mut formatter = Formatter::new(style);
        self.display(&mut formatter);
        formatter.into_string()
    }
}

fn to_string<T: Displayable>(v: &T) -> String {
    let style = Style::default();
    let mut formatter = Formatter::new(&style);
    v.display(&mut formatter);
    formatter.into_string()
}

fn description<'a>(description: &Option<String>, f: &mut Formatter) {
    if let Some(ref descr) = *description {
        f.indent();
        f.write_quoted(descr.as_ref());
        f.endline();
    }
}


impl<'a, T> Displayable for Document<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        for item in &self.definitions {
            item.display(f);
        }
    }
}

impl<'a, T> Displayable for Definition<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        f.margin();
        match *self {
            Definition::SchemaDefinition(ref s) => s.display(f),
            Definition::TypeDefinition(ref t) => t.display(f),
            Definition::TypeExtension(ref e) => e.display(f),
            Definition::DirectiveDefinition(ref d) => d.display(f),
        }
    }
}

impl<'a, T> Displayable for SchemaDefinition<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        f.indent();
        f.write("schema");
        format_directives(&self.directives, f);
        f.write(" ");
        f.start_block();
        if let Some(ref q) = self.query {
            f.indent();
            f.write("query: ");
            f.write(q.as_ref());
            f.endline();
        }
        if let Some(ref m) = self.mutation {
            f.indent();
            f.write("mutation: ");
            f.write(m.as_ref());
            f.endline();
        }
        if let Some(ref s) = self.subscription {
            f.indent();
            f.write("subscription: ");
            f.write(s.as_ref());
            f.endline();
        }
        f.end_block();
    }
}

impl<'a, T> Displayable for TypeDefinition<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        match *self {
            TypeDefinition::Scalar(ref s) => s.display(f),
            TypeDefinition::Object(ref o) => o.display(f),
            TypeDefinition::Interface(ref i) => i.display(f),
            TypeDefinition::Union(ref u) => u.display(f),
            TypeDefinition::Enum(ref e) => e.display(f),
            TypeDefinition::InputObject(ref i) => i.display(f),
        }
    }
}

impl<'a, T> Displayable for ScalarType<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        description(&self.description, f);
        f.indent();
        f.write("scalar ");
        f.write(self.name.as_ref());
        format_directives(&self.directives, f);
        f.endline();
    }
}

impl<'a, T> Displayable for ScalarTypeExtension<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        f.indent();
        f.write("extend scalar ");
        f.write(self.name.as_ref());
        format_directives(&self.directives, f);
        f.endline();
    }
}

fn format_fields<'a, T>(fields: &[Field<'a, T>], f: &mut Formatter) 
    where T: Text<'a>,
{
    if !fields.is_empty() {
        f.write(" ");
        f.start_block();
        for fld in fields {
            fld.display(f);
        }
        f.end_block();
    } else {
        f.endline();
    }
}

impl<'a, T> Displayable for ObjectType<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        description(&self.description, f);
        f.indent();
        f.write("type ");
        f.write(self.name.as_ref());
        if !self.implements_interfaces.is_empty() {
            f.write(" implements ");
            f.write(self.implements_interfaces[0].as_ref());
            for name in &self.implements_interfaces[1..] {
                f.write(" & ");
                f.write(name.as_ref());
            }
        }
        format_directives(&self.directives, f);
        format_fields(&self.fields, f);
    }
}

impl<'a, T> Displayable for ObjectTypeExtension<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        f.indent();
        f.write("extend type ");
        f.write(self.name.as_ref());
        if !self.implements_interfaces.is_empty() {
            f.write(" implements ");
            f.write(self.implements_interfaces[0].as_ref());
            for name in &self.implements_interfaces[1..] {
                f.write(" & ");
                f.write(name.as_ref());
            }
        }
        format_directives(&self.directives, f);
        format_fields(&self.fields, f);
    }
}

impl<'a, T> Displayable for InputValue<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        if let Some(ref descr) = self.description {
            f.write_quoted(descr.as_ref());
            f.write(" ");
        }
        f.write(self.name.as_ref());
        f.write(": ");
        self.value_type.display(f);
        if let Some(ref def) = self.default_value {
            f.write(" = ");
            def.display(f);
        }
        format_directives(&self.directives, f);
    }
}

fn format_arguments<'a, T>(arguments: &[InputValue<'a, T>], f: &mut Formatter) 
    where T: Text<'a>,
{
    if !arguments.is_empty() {
        f.write("(");
        arguments[0].display(f);
        for arg in &arguments[1..] {
            f.write(", ");
            arg.display(f);
        }
        f.write(")");
    }
}

impl<'a, T> Displayable for Field<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        description(&self.description, f);
        f.indent();
        f.write(self.name.as_ref());
        format_arguments(&self.arguments, f);
        f.write(": ");
        self.field_type.display(f);
        format_directives(&self.directives, f);
        f.endline();
    }
}

impl<'a, T> Displayable for InterfaceType<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        description(&self.description, f);
        f.indent();
        f.write("interface ");
        f.write(self.name.as_ref());
        format_directives(&self.directives, f);
        format_fields(&self.fields, f);
    }
}

impl<'a, T> Displayable for InterfaceTypeExtension<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        f.indent();
        f.write("extend interface ");
        f.write(self.name.as_ref());
        format_directives(&self.directives, f);
        format_fields(&self.fields, f);
    }
}

impl<'a, T> Displayable for UnionType<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        description(&self.description, f);
        f.indent();
        f.write("union ");
        f.write(self.name.as_ref());
        format_directives(&self.directives, f);
        if !self.types.is_empty() {
            f.write(" = ");
            f.write(self.types[0].as_ref());
            for typ in &self.types[1..] {
                f.write(" | ");
                f.write(typ.as_ref());
            }
        }
        f.endline();
    }
}

impl<'a, T> Displayable for UnionTypeExtension<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        f.indent();
        f.write("extend union ");
        f.write(self.name.as_ref());
        format_directives(&self.directives, f);
        if !self.types.is_empty() {
            f.write(" = ");
            f.write(self.types[0].as_ref());
            for typ in &self.types[1..] {
                f.write(" | ");
                f.write(typ.as_ref());
            }
        }
        f.endline();
    }
}

impl<'a, T> Displayable for EnumType<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        description(&self.description, f);
        f.indent();
        f.write("enum ");
        f.write(self.name.as_ref());
        format_directives(&self.directives, f);
        if !self.values.is_empty() {
            f.write(" ");
            f.start_block();
            for val in &self.values {
                f.indent();
                if let Some(ref descr) = val.description {
                    f.write_quoted(descr.as_ref());
                    f.write(" ");
                }
                f.write(val.name.as_ref());
                format_directives(&val.directives, f);
                f.endline();
            }
            f.end_block();
        } else {
            f.endline();
        }
    }
}

impl<'a, T> Displayable for EnumTypeExtension<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        f.indent();
        f.write("extend enum ");
        f.write(self.name.as_ref());
        format_directives(&self.directives, f);
        if !self.values.is_empty() {
            f.write(" ");
            f.start_block();
            for val in &self.values {
                f.indent();
                if let Some(ref descr) = val.description {
                    f.write_quoted(descr.as_ref());
                    f.write(" ");
                }
                f.write(val.name.as_ref());
                format_directives(&val.directives, f);
                f.endline();
            }
            f.end_block();
        } else {
            f.endline();
        }
    }
}

fn format_inputs<'a, T>(fields: &[InputValue<'a, T>], f: &mut Formatter) 
    where T: Text<'a>,
{
    if !fields.is_empty() {
        f.write(" ");
        f.start_block();
        for fld in fields {
            f.indent();
            fld.display(f);
            f.endline();
        }
        f.end_block();
    } else {
        f.endline();
    }
}

impl<'a, T> Displayable for InputObjectType<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        description(&self.description, f);
        f.indent();
        f.write("input ");
        f.write(self.name.as_ref());
        format_directives(&self.directives, f);
        format_inputs(&self.fields, f);
    }
}

impl<'a, T> Displayable for InputObjectTypeExtension<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        f.indent();
        f.write("extend input ");
        f.write(self.name.as_ref());
        format_directives(&self.directives, f);
        format_inputs(&self.fields, f);
    }
}

impl<'a, T> Displayable for TypeExtension<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        match *self {
            TypeExtension::Scalar(ref s) => s.display(f),
            TypeExtension::Object(ref o) => o.display(f),
            TypeExtension::Interface(ref i) => i.display(f),
            TypeExtension::Union(ref u) => u.display(f),
            TypeExtension::Enum(ref e) => e.display(f),
            TypeExtension::InputObject(ref i) => i.display(f),
        }
    }
}

impl<'a, T> Displayable for DirectiveDefinition<'a, T> 
    where T: Text<'a>,
{
    fn display(&self, f: &mut Formatter) {
        description(&self.description, f);
        f.indent();
        f.write("directive @");
        f.write(self.name.as_ref());
        format_arguments(&self.arguments, f);
        if !self.locations.is_empty() {
            f.write(" on ");
            let mut first = true;
            for loc in &self.locations {
                if first {
                    first = false;
                } else {
                    f.write(" | ");
                }
                f.write(loc.as_str());
            }
        }
        f.endline();
    }
}

impl_display!(
    'a 
    Document,
    Definition,
    SchemaDefinition,
    TypeDefinition,
    TypeExtension,
    ScalarType,
    ScalarTypeExtension,
    ObjectType,
    ObjectTypeExtension,
    Field,
    InputValue,
    InterfaceType,
    InterfaceTypeExtension,
    UnionType,
    UnionTypeExtension,
    EnumType,
    EnumTypeExtension,
    InputObjectType,
    InputObjectTypeExtension,
    DirectiveDefinition,
);
