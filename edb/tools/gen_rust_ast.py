import typing
import typing_inspect
import dataclasses
import textwrap
from itertools import chain

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.common.ast import base as ast
from edb.common import enum as s_enum
from edb.tools.edb import edbcommands

# a global variable storing union types that are to be generated
union_types: typing.List[typing.Tuple[str, typing.Type]] = []


@edbcommands.command("gen-rust-ast")
def main():
    f = open('edb/edgeql-parser/src/ast.rs', 'w')

    f.write(
        textwrap.dedent(
            '''\
            // DO NOT EDIT. This file was generated with:
            //
            // $ edb gen-rust-ast

            //! Abstract Syntax Tree for EdgeQL
            #![allow(non_camel_case_types)]

            use std::collections::HashMap;
            '''
        )
    )

    # discover all nodes
    ast_classes = {}
    for name, typ in qlast.__dict__.items():
        if not isinstance(typ, type) or not hasattr(typ, '_direct_fields'):
            continue

        if name == 'Base':
            continue

        # re-run field collection to correctly handle forward-references
        typ = typ._collect_direct_fields()

        ast_classes[typ] = ASTClass(name=name, typ=typ)

    for ast_class in ast_classes.values():
        f.write(codegen_struct(ast_class.name, ast_class.typ))

        while len(union_types) > 0:
            (name, union_type) = union_types.pop(0)
            f.write(codegen_union(name, union_type))

    for name, typ in chain(qlast.__dict__.items(), qltypes.__dict__.items()):

        if not isinstance(typ, type) or not issubclass(typ, s_enum.StrEnum):
            continue

        f.write(codegen_enum(name, typ))


@dataclasses.dataclass()
class ASTClass:
    name: str
    typ: typing.Type
    children: typing.List[typing.Type] = dataclasses.field(
        default_factory=list
    )


def codegen_struct(name: str, cls: typing.Type) -> str:
    fields = ''
    for f in typing.cast(typing.List[ast._Field], cls._direct_fields):

        if f.hidden:
            continue

        typ = translate_type(
            f.type, lambda: f'{name}{f.name[0].upper()}{f.name[1:]}'
        )

        f_name = codegen_rust_ident(f.name)

        fields += f'    pub {f_name}: {typ},\n'

    return (
        '\n#[derive(Debug, Clone)]\n'
        + f'pub struct {name} {"{"}\n'
        + fields
        + '}\n'
    )


def codegen_enum(name: str, cls: typing.Type) -> str:
    fields = ''
    for member in cls._member_names_:
        fields += f'    {member},\n'

    return (
        '\n#[derive(Debug, Clone)]\n'
        + f'pub enum {name} {"{"}\n'
        + fields
        + '}\n'
    )


def codegen_rust_ident(name: str) -> str:
    if name in {'type', 'where', 'ref', 'final', 'abstract'}:
        return 'r#' + name
    return name


def codegen_union(name: str, cls: typing.Type) -> str:
    fields = ''
    for arg in typing_inspect.get_args(cls):
        typ = translate_type(arg, lambda: '???')
        fields += f'    {arg.__name__}({typ}),\n'

    annotations = '#[derive(Debug, Clone)]\n'
    return f'\n{annotations}pub enum {name} {"{"}\n{fields}{"}"}\n'


def translate_type(
    typ: typing.Type, name_generator: typing.Callable[[], str]
) -> str:
    params = [
        translate_type(param, name_generator)
        for param in typing_inspect.get_args(typ)
    ]

    if typing_inspect.is_union_type(typ):

        if hasattr(typ, '_name') and typ._name == 'Optional':
            return f'Option<{params[0]}>'

        name = name_generator()
        union_types.append((name, typ))
        return name

    if typing_inspect.is_generic_type(typ) and hasattr(typ, '_name'):

        if typ._name in ('List', 'Sequence'):
            return f'Vec<{params[0]}>'

        if typ._name == 'Dict':
            return f'HashMap<{params[0]}, {params[1]}>'

    if not hasattr(typ, '__name__'):
        return str(typ)

    if typ.__name__ == 'Tuple':
        if params[1] == 'Ellipsis':
            return f'Vec<{params[0]}>'
        else:
            return '(' + ', '.join(params) + ')'

    mappings = {
        'str': 'String',
        'bool': 'bool',
        'int': 'i64',
        'float': 'f64',
        'Expr': 'Box<Expr>',
        'NoneType': '()',
        'bytes': 'Vec<u8>',
    }

    if typ.__name__ in mappings:
        return mappings[typ.__name__]

    if typ.__module__ not in ('edb.edgeql.ast', 'edb.edgeql.qltypes'):
        raise NotImplementedError(f'cannot translate: {typ}')

    # a named AST type: just nest
    return typ.__name__
