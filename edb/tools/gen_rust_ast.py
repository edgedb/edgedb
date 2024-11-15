import collections
import typing
import dataclasses
import textwrap
from itertools import chain

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import parser
from edb.common.ast import base as ast
from edb.common import enum as s_enum
from edb.common import typing_inspect
from edb.tools.edb import edbcommands
import edb._edgeql_parser as rust_parser


@dataclasses.dataclass()
class ASTClass:
    name: str
    typ: typing.Type
    children: typing.List[typing.Type] = dataclasses.field(default_factory=list)


@dataclasses.dataclass()
class ASTUnion:
    name: str
    variants: typing.Sequence[typing.Type | str]
    for_composition: bool


# a queue for union types that are to be generated
union_types: typing.List[ASTUnion] = []

# all discovered AST classes
ast_classes: typing.Dict[str, ASTClass] = {}


@edbcommands.command("gen-rust-ast")
def main() -> None:
    gen_rust_ast()
    gen_rust_reductions()


def gen_rust_ast() -> None:
    f = open('edb/edgeql-parser/src/ast.rs', 'w')

    f.write(
        textwrap.dedent(
            '''\
            // DO NOT EDIT. This file was generated with:
            //
            // $ edb gen-rust-ast

            //! Abstract Syntax Tree for EdgeQL
            #![allow(non_camel_case_types)]

            use indexmap::IndexMap;
            '''
        )
    )

    # discover all nodes
    for name, typ in qlast.__dict__.items():
        if not isinstance(typ, type) or not hasattr(typ, '_direct_fields'):
            continue

        if not issubclass(typ, qlast.Base):
            continue

        # re-run field collection to correctly handle forward-references
        typ = typ._collect_direct_fields()  # type: ignore

        ast_classes[typ.__name__] = ASTClass(name=name, typ=typ)

    # build inheritance graph
    for ast_class in ast_classes.values():
        for base in ast_class.typ.__bases__:
            if base.__name__ not in ast_classes:
                continue
            ast_classes[base.__name__].children.append(ast_class.typ)

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


def gen_rust_reductions() -> None:
    f = open('edb/edgeql-parser/src/reductions.rs', 'w')

    f.write(
        textwrap.dedent(
            '''\
            // DO NOT EDIT. This file was generated with:
            //
            // $ edb gen-rust-ast

            //! Reductions for productions in EdgeQL
            #![allow(non_camel_case_types)]
            '''
        )
    )

    parser.preload_spec()
    productions = rust_parser.get_productions()

    prod_to_reductions = collections.defaultdict(list)
    for prod, reduction_method in productions:
        if reduction_method.__doc__ is not None:
            reduction_name = get_reduction_name(reduction_method.__doc__)
            prod_to_reductions[prod.__name__].append(reduction_name)

    f.write(codegen_reduction_enum(prod_to_reductions))

    for prod in sorted(prod_to_reductions.keys()):
        f.write(codegen_production_reductions_enum(prod, prod_to_reductions[prod]))

    f.write(codegen_reduction_from_id_func(productions))


def codegen_struct(cls: ASTClass) -> str:
    if cls.typ.__abstract_node__:
        return codegen_union(ASTUnion(
            name=cls.name, variants=cls.children, for_composition=True
        ))

    fields = collections.OrderedDict()
    for parent in reversed(cls.typ.__mro__):
        lst = getattr(parent, '_direct_fields', [])
        for field in lst:
            fields[field.name] = field

    field_names = set()
    fields_text = ''
    doc_comment = ''
    for f in typing.cast(typing.List[ast._Field], fields.values()):

        if f.hidden:
            continue

        union_name = f'{cls.name}{title_case(f.name)}'

        print(f'struct {cls.name}, field {f.name}, type: {f.type}')
        typ = translate_type(f.type, union_name, False)
        if hasattr(cls.typ, '__rust_box__') and f.name in cls.typ.__rust_box__:
            typ = f'Box<{typ}>'

        f_name = quote_rust_ident(f.name)
        field_names.add(f_name)

        fields_text += f'    pub {f_name}: {typ},\n'

    return (
        f'\n{doc_comment}'
        + f'#[derive(Debug, Clone)]\n'
        # + f'#[cfg_attr(feature = "python", derive(IntoPython))]\n'
        + f'pub struct {cls.name} {"{"}\n'
        + fields_text
        + '}\n'
    ).replace('{\n}', r'{}')


def codegen_enum(name: str, cls: typing.Type) -> str:
    fields = ''
    for member in cls._member_names_:
        fields += f'    {member},\n'

    if cls.__module__ == 'edb.edgeql.ast':
        cls_path = f'qlast.{cls.__name__}'
    elif cls.__module__ == 'edb.edgeql.qltypes':
        cls_path = f'qltypes.{cls.__name__}'
    else:
        raise LookupError(
            'we only support generating AST from qlast and qltypes modules'
        )

    return (
        '\n#[derive(Debug, Clone)]\n'
        # + f'#[cfg_attr(feature = "python", derive(IntoPython))]\n'
        # + f'#[cfg_attr(feature = "python", py_enum({cls_path}))]\n'
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
            print(f'union {union.name}, variant {arg}')
            typ = translate_type(arg, '???', union.for_composition)
            fields += f'    {arg.__name__}({typ}),\n'

    # attr = 'py_child' if union.for_composition else 'py_union'

    return (
        '\n#[derive(Debug, Clone)]\n'
        # f'#[cfg_attr(feature = "python", derive(IntoPython))]\n'
        # f'#[cfg_attr(feature = "python", {attr})]\n'
        f'pub enum {union.name} {"{"}\n{fields}{"}"}\n'
    )


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
            return f'IndexMap<{params[0]}, {params[1]}>'

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

    return typ.__name__


def codegen_reduction_enum(
    prod_to_reductions: typing.Dict[str, typing.List[str]],
) -> str:
    members = ''
    for production in sorted(prod_to_reductions.keys()):
        members += f'    {production}({production}),\n'

    return (
        '\n#[derive(Debug, Clone)]\n'
        + f'pub enum Reduction {"{"}\n'
        + members
        + '}\n'
    )


def codegen_production_reductions_enum(
    prod_name: str,
    reductions: typing.List[str],
) -> str:
    members = ''
    for reduction in reductions:
        members += f'    {reduction},\n'

    return (
        '\n#[derive(Debug, Clone)]\n'
        + f'pub enum {prod_name} {"{"}\n'
        + members
        + '}\n'
    )


def codegen_reduction_from_id_func(
    productions: typing.List[
        typing.Tuple[typing.Type, typing.Callable]
    ],
) -> str:
    matches = ''
    for id, (prod, reduction_method) in enumerate(productions):
        matches += f'        {id} => '

        if reduction_method.__doc__ is not None:
            prod_name = prod.__name__
            reduction_name = get_reduction_name(reduction_method.__doc__)

            reduction_member = f'{prod_name}::{reduction_name}'
            matches += f'Reduction::{prod_name}({reduction_member}),\n'
        else:
            matches += 'unreachable!(),\n'

    return (
        '\npub fn reduction_from_id(id: usize) -> Reduction {\n'
        + '    match id {\n'
        + matches
        + '        _ => unreachable!(),\n'
        + '    }\n'
        + '}\n'
    )


def get_reduction_name(docstring: str) -> str:
    prepared_name = docstring.replace(r'%reduce', '')
    prepared_name = prepared_name.replace('<e>', 'epsilon')
    prepared_name = prepared_name.replace('[', '').replace(']', '')
    prepared_name = prepared_name.replace('\\', '')
    return '_'.join(prepared_name.split())
