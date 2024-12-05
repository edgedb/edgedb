# Generates an inheritance graph of Python classes.
#
# Usage:
# $ edb ast-inheritance-graph | fdp -T svg -o ast-fdp.svg
#
# Requirements:
# - graphviz

import typing
import dataclasses
import enum

import click

from edb.edgeql import ast as qlast
from edb.ir import ast as irast
from edb.pgsql import ast as pgast
from edb.tools.edb import edbcommands


class ASTModule(str, enum.Enum):
    ql = "ql"
    ir = "ir"
    pg = "pg"


@dataclasses.dataclass()
class ASTClass:
    name: str
    typ: typing.Type
    bases: typing.Set[str]
    children: typing.Set[str]


@edbcommands.command("ast-inheritance-graph")
@click.argument('ast', type=click.Choice(ASTModule))  # type: ignore
def main(ast: ASTModule) -> None:
    ast_mod: typing.Any
    if ast == ASTModule.ql:
        ast_mod = qlast
    elif ast == ASTModule.ir:
        ast_mod = irast
    elif ast == ASTModule.pg:
        ast_mod = pgast
    else:
        raise AssertionError()

    # discover all nodes
    ast_classes: typing.Dict[str, ASTClass] = {}
    for name, typ in ast_mod.__dict__.items():
        if not isinstance(typ, type):
            continue

        if not issubclass(typ, ast_mod.Base) or name in {
            'Base',
            'ImmutableBase',
        }:
            continue

        if typ.__rust_ignore__:  # type: ignore
            continue

        # re-run field collection to correctly handle forward-references
        typ = typ._collect_direct_fields()  # type: ignore

        ast_classes[typ.__name__] = ASTClass(
            name=name,
            typ=typ,
            children=set(),
            bases=set(),
        )

    for ast_class in ast_classes.values():
        for base in ast_class.typ.__bases__:
            if base.__name__ not in ast_classes:
                continue
            ast_class.bases.add(base.__name__)
            ast_classes[base.__name__].children.add(ast_class.name)

    inheritance_graph(ast_classes)
    enum_graph(ast_classes)


def inheritance_graph(ast_classes: typing.Dict[str, ASTClass]):
    print('digraph I {')
    for ast_class in ast_classes.values():
        if ast_class.typ.__abstract_node__:
            print(f'  {ast_class.name} [color = red];')
        for base in ast_class.bases:
            print(f'  {ast_class.name} -> {base};')

    print('}')


def enum_graph(ast_classes: typing.Dict[str, ASTClass]):
    print('digraph M {')

    def dfs(node, start):
        ast_class = ast_classes[node]
        if ast_class.typ.__abstract_node__:
            print(f'  {node}_{start} [color = red];')

        for child in ast_class.children:
            print(f'  {node}_{start} -> {child}_{start};')
            dfs(child, start)

    for ast_class in ast_classes.values():
        if len(ast_class.bases) != 0 or len(ast_class.children) == 0:
            continue
        dfs(ast_class.name, ast_class.name)

    print('}')
