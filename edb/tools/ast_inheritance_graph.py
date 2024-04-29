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


@edbcommands.command("ast-inheritance-graph")
@click.argument('ast', type=click.Choice(ASTModule))  # type: ignore
def main(ast: ASTModule) -> None:
    print('digraph G {')

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

        if typ.__abstract_node__:  # type: ignore
            print(f'  {name} [color = red];')

        if typ.__rust_ignore__:  # type: ignore
            continue

        # re-run field collection to correctly handle forward-references
        typ = typ._collect_direct_fields()  # type: ignore

        ast_classes[typ.__name__] = ASTClass(name=name, typ=typ)

    # build inheritance graph
    for ast_class in ast_classes.values():
        for base in ast_class.typ.__bases__:
            if base.__name__ not in ast_classes:
                continue
            print(f'  {ast_class.name} -> {base.__name__};')

    print('}')
