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


@dataclasses.dataclass()
class ASTClass:
    name: str
    typ: typing.Type
    children: typing.List[typing.Type] = dataclasses.field(
        default_factory=list
    )
    is_base = False


@dataclasses.dataclass()
class ASTUnion:
    name: str
    variants: typing.Sequence[typing.Type | str]
    for_composition: bool


# a queue for union types that are to be generated
union_types: typing.List[ASTUnion] = []

# all discovered AST classes
ast_classes: typing.Dict[str, ASTClass] = {}

# types that need IntoPython trait implemented
types_into_python: typing.Set[typing.Type] = set()


@edbcommands.command("gen-rust-ast")
def main() -> None:
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
    for name, typ in qlast.__dict__.items():
        if not isinstance(typ, type) or not hasattr(typ, '_direct_fields'):
            continue

        if name == 'Base' or name.startswith('_'):
            continue

        # re-run field collection to correctly handle forward-references
        typ = typ._collect_direct_fields()  # type: ignore

        ast_classes[typ.__name__] = ASTClass(name=name, typ=typ)

    # build inheritance graph
    for ast_class in ast_classes.values():
        for base in ast_class.typ.__bases__:
            if base.__name__ == 'Base':
                ast_class.is_base = True
            if base.__name__ not in ast_classes:
                continue
            ast_classes[base.__name__].children.append(ast_class.typ)

    # dry run to populate `types_into_python`
    for ast_class in ast_classes.values():
        codegen_struct(ast_class)

        while len(union_types) > 0:
            codegen_union(union_types.pop(0))

    # generate structs
    for ast_class in ast_classes.values():
        f.write(codegen_struct(ast_class))

        while len(union_types) > 0:
            f.write(codegen_union(union_types.pop(0)))

    # generate enums
    for name, typ in chain(qlast.__dict__.items(), qltypes.__dict__.items()):

        if not isinstance(typ, type) or not issubclass(typ, s_enum.StrEnum):
            continue

        f.write(codegen_enum(name, typ))


def codegen_struct(cls: ASTClass) -> str:
    field_names = set()
    fields = ''
    doc_comment = ''

    for f in typing.cast(typing.List[ast._Field], cls.typ._direct_fields):

        if f.hidden:
            continue

        union_name = f'{cls.name}{title_case(f.name)}'

        typ = translate_type(f.type, union_name, False)
        if hasattr(cls.typ, '__rust_box__') and f.name in cls.typ.__rust_box__:
            typ = f'Box<{typ}>'

        f_name = quote_rust_ident(f.name)
        field_names.add(f_name)

        fields += f'    pub {f_name}: {typ},\n'

    if len(cls.children) > 0:

        for i in range(0, 10):
            kind_name = 'kind' if i == 0 else f'kind{i}'
            if kind_name not in field_names:
                break

        name = f'{cls.name}Kind'
        variants: typing.Sequence[typing.Type | str] = cls.children

        if not cls.typ.__abstract_node__:
            variants = list(variants) + ['Plain']

        union_types.append(
            ASTUnion(name=name, variants=variants, for_composition=True)
        )
        fields += f'    pub {kind_name}: {name},\n'

    if cls.is_base:
        doc_comment = '/// Base class\n'

    derives = ''
    if cls.typ in types_into_python:
        derives += ', IntoPython'
    return (
        f'\n{doc_comment}'
        + f'#[derive(Debug, Clone{derives})]\n'
        + f'pub struct {cls.name} {"{"}\n'
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


def quote_rust_ident(name: str) -> str:
    if name in {'type', 'where', 'ref', 'final', 'abstract'}:
        return 'r#' + name
    return name


def title_case(name: str) -> str:
    return name[0].upper() + name[1:]


def codegen_union(union: ASTUnion) -> str:
    fields = ''
    for arg in union.variants:
        if isinstance(arg, str):
            fields += f'    {arg},\n'
        else:
            typ = translate_type(arg, '???', union.for_composition)
            fields += f'    {arg.__name__}({typ}),\n'

    annotations = '#[derive(Debug, Clone)]\n'
    return f'\n{annotations}pub enum {union.name} {"{"}\n{fields}{"}"}\n'


def translate_type(
    typ: typing.Type, union_name: str, for_composition: bool
) -> str:
    params = [
        translate_type(param, union_name, for_composition)
        for param in typing_inspect.get_args(typ)
    ]

    if typing_inspect.is_union_type(typ):

        if hasattr(typ, '_name') and typ._name == 'Optional':
            return f'Option<{params[0]}>'

        union_types.append(
            ASTUnion(
                name=union_name,
                variants=typing_inspect.get_args(typ),
                for_composition=for_composition,
            )
        )
        return union_name

    if typing_inspect.is_generic_type(typ) and hasattr(typ, '_name'):

        if typ._name in ('List', 'Sequence'):
            return f'Vec<{params[0]}>'

        if typ._name == 'Dict':
            return f'HashMap<{params[0]}, {params[1]}>'

    if not hasattr(typ, '__name__'):
        return str(typ)

    if typ.__name__ == 'Tuple' and typ.__module__ == 'typing':
        if len(params) > 0 and params[1] == 'Ellipsis':
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

    if for_composition or typ.__name__ not in ast_classes:
        return typ.__name__

    types_into_python.add(typ)
    ancestor = find_covering_ancestor(
        typ, set(f.name for f in typ._fields.values() if not f.hidden)
    )
    return ancestor.__name__


def find_covering_ancestor(typ: typing.Type, fields: typing.Set[str]):
    # In Rust, a type will not inherit fields from parent types.
    # This means that we need to omit some ancestor of this type, which
    # would include all fields of the type.
    # We loose a bit of type checking strictness here.
    for parent in typ.__mro__:
        if not hasattr(parent, '_direct_fields'):
            continue

        fields = fields.difference((f.name for f in parent._direct_fields))
        if len(fields) == 0:
            return parent
    raise AssertionError()
