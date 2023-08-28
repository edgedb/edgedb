#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations
from typing import *

import importlib
import multiprocessing
import types

from edb import errors
from edb.common import parsing

import edb._edgeql_parser as rust_parser

from . import grammar as qlgrammar

from .. import ast as qlast
from .. import tokenizer as qltokenizer


def append_module_aliases(tree, aliases):
    modaliases = []
    for alias, module in aliases.items():
        decl = qlast.ModuleAliasDecl(module=module, alias=alias)
        modaliases.append(decl)

    if not tree.aliases:
        tree.aliases = modaliases
    else:
        tree.aliases = modaliases + tree.aliases

    return tree


def parse_fragment(
    source: Union[qltokenizer.Source, str],
    filename: Optional[str] = None,
) -> qlast.Expr:
    res = parse(qlgrammar.fragment, source, filename=filename)
    assert isinstance(res, qlast.Expr)
    return res


def parse_query(
    source: Union[qltokenizer.Source, str],
    module_aliases: Optional[Mapping[Optional[str], str]] = None,
) -> qlast.Query:
    """Parse some EdgeQL potentially adding some module aliases.

    This will parse EdgeQL queries and expressions. If the source is an
    expression, the result will be wrapped into a SelectQuery.
    """

    tree = parse_fragment(source)
    if not isinstance(tree, qlast.Query):
        tree = qlast.SelectQuery(result=tree)

    if module_aliases:
        append_module_aliases(tree, module_aliases)

    return tree


def parse_command(
    source: Union[qltokenizer.Source, str],
    module_aliases: Optional[Mapping[Optional[str], str]] = None,
) -> qlast.Command:
    """Parse some EdgeQL command potentially adding some module aliases."""

    tree = parse(qlgrammar.single, source)
    assert isinstance(tree, qlast.Command)

    if module_aliases:
        append_module_aliases(tree, module_aliases)

    return tree


def parse_block(source: qltokenizer.Source | str) -> list[qlast.Base]:
    return parse(qlgrammar.block, source)


def parse_migration_body_block(
    source: str,
) -> tuple[qlast.NestedQLBlock, list[qlast.SetField]]:
    # For parser-internal technical reasons, we don't have a
    # production that means "just the *inside* of a migration block
    # (without braces)", so we just hack around this by adding braces.
    # This is only really workable because we only use this in a place
    # where the source contexts don't matter anyway.
    return parse(qlgrammar.migration_body, f"{{{source}}}")


def parse_extension_package_body_block(
    source: str,
) -> tuple[qlast.NestedQLBlock, list[qlast.SetField]]:
    # For parser-internal technical reasons, we don't have a
    # production that means "just the *inside* of a migration block
    # (without braces)", so we just hack around this by adding braces.
    # This is only really workable because we only use this in a place
    # where the source contexts don't matter anyway.
    return parse(qlgrammar.extension_package_body, f"{{{source}}}")


def parse_sdl(expr: str):
    return parse(qlgrammar.sdldocument, expr)


def parse(
    grammar: types.ModuleType,
    source: Union[str, qltokenizer.Source],
    filename: Optional[str] = None,
):
    if isinstance(source, str):
        source = qltokenizer.Source.from_string(source)

    result, productions = rust_parser.parse(grammar.__name__, source.tokens())

    if len(result.errors()) > 0:
        # TODO: emit multiple errors

        # Heuristic to pick the error:
        # - first encountered,
        # - Unexpected before Missing,
        # - original order.
        errs: List[Tuple[str, Tuple[int, Optional[int]]]] = result.errors()
        errs.sort(key=lambda e: (e[1][0], -ord(e[0][1])))
        error = errs[0]

        message, span = error
        position = qltokenizer.inflate_position(source.text(), span)

        pcontext = parsing.ParserContext(
            'query',
            source.text(),
            start=position[2],
            end=position[3] or position[2],
            context_lines=10,
        )
        raise errors.EdgeQLSyntaxError(
            message, position=position, context=pcontext)

    return _cst_to_ast(
        result.out(),
        productions,
        source,
        filename,
    ).val


def _cst_to_ast(
    cst: rust_parser.CSTNode,
    productions: list[Callable],
    source: qltokenizer.Source,
    filename: Optional[str],
) -> Any:
    # Converts CST into AST by calling methods from the grammar classes.
    #
    # This function was originally written as a simple recursion.
    # Then I had to unfold it, because it was hitting recursion limit.
    # Stack here contains all remaining things to do:
    # - CST node means the node has to be processed and pushed onto the
    #   result stack,
    # - production means that all args of production have been processed
    #   are are ready to be passed to the production method. The result is
    #   obviously pushed onto the result stack

    stack: List[rust_parser.CSTNode | rust_parser.Production] = [cst]
    result: List[Any] = []

    while len(stack) > 0:
        node = stack.pop()

        if isinstance(node, rust_parser.CSTNode):
            # this would be the body of the original recursion function

            if terminal := node.terminal():
                # Terminal is simple: just convert to parsing.Token
                context = parsing.ParserContext(
                    name=filename,
                    buffer=source.text(),
                    start=terminal.start(),
                    end=terminal.end(),
                )
                result.append(
                    parsing.Token(
                        terminal.text(), terminal.value(), context
                    )
                )

            elif production := node.production():
                # Production needs to first process all args, then
                # call the appropriate method.
                # (this is all in reverse, because stacks)
                stack.append(production)
                args = list(production.args())
                args.reverse()
                stack.extend(args)
            else:
                raise NotImplementedError(node)

        elif isinstance(node, rust_parser.Production):
            # production args are done, get them out of result stack
            len_args = len(node.args())
            split_at = len(result) - len_args
            args = result[split_at:]
            result = result[0:split_at]

            # find correct method to call
            production_id = node.id()
            production = productions[production_id]

            sym = production.lhs.nontermType()
            assert len(args) == len(production.rhs)
            production.method(sym, *args)

            # push into result stack
            result.append(sym)

    return result.pop()


def _load_parser(grammar: str) -> None:
    specmod = importlib.import_module(grammar)
    parsing.load_parser_spec(specmod, allow_rebuild=True)


def preload(
    allow_rebuild: bool = True,
    paralellize: bool = False,
    grammars: Optional[list[types.ModuleType]] = None,
) -> None:
    if grammars is None:
        grammars = [
            qlgrammar.block,
            qlgrammar.fragment,
            qlgrammar.sdldocument,
            qlgrammar.extension_package_body,
            qlgrammar.migration_body,
        ]

    if not paralellize:
        try:
            for grammar in grammars:
                spec = parsing.load_parser_spec(
                    grammar, allow_rebuild=allow_rebuild)
                rust_parser.cache_spec(grammar.__name__, spec)
        except parsing.ParserSpecIncompatibleError as e:
            raise errors.InternalServerError(e.args[0]) from None
    else:
        parsers_to_rebuild = []

        for grammar in grammars:
            try:
                spec = parsing.load_parser_spec(grammar, allow_rebuild=False)
                rust_parser.cache_spec(grammar.__name__, spec)
            except parsing.ParserSpecIncompatibleError:
                parsers_to_rebuild.append(grammar)

        if len(parsers_to_rebuild) == 0:
            pass
        elif len(parsers_to_rebuild) == 1:
            spec = parsing.load_parser_spec(
                parsers_to_rebuild[0], allow_rebuild=True)
            rust_parser.cache_spec(parsers_to_rebuild[0].__name__, spec)
        else:
            with multiprocessing.Pool(len(parsers_to_rebuild)) as pool:
                pool.map(
                    _load_parser,
                    [mod.__name__ for mod in parsers_to_rebuild],
                )

            for grammar in parsers_to_rebuild:
                spec = parsing.load_parser_spec(grammar, allow_rebuild=False)
                rust_parser.cache_spec(grammar.__name__, spec)
