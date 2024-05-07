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
from typing import Any, Callable, Optional, Tuple, Type, Union, Mapping, List
import pathlib

from edb import errors
from edb.common import parsing

import edb._edgeql_parser as rust_parser

from .grammar import tokens

from .. import ast as qlast
from .. import tokenizer as qltokenizer


SPEC_LOADED = False


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
    res = parse(tokens.T_STARTFRAGMENT, source, filename=filename)
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


def parse_block(
    source: qltokenizer.Source | str,
    module_aliases: Optional[Mapping[Optional[str], str]] = None,
) -> list[qlast.Base]:
    trees = parse(tokens.T_STARTBLOCK, source)
    if module_aliases:
        for tree in trees:
            append_module_aliases(tree, module_aliases)
    return trees


def parse_migration_body_block(
    source: str,
) -> tuple[qlast.NestedQLBlock, list[qlast.SetField]]:
    # For parser-internal technical reasons, we don't have a
    # production that means "just the *inside* of a migration block
    # (without braces)", so we just hack around this by adding braces.
    # This is only really workable because we only use this in a place
    # where the source contexts don't matter anyway.
    return parse(tokens.T_STARTMIGRATION, f"{{{source}}}")


def parse_extension_package_body_block(
    source: str,
) -> tuple[qlast.NestedQLBlock, list[qlast.SetField]]:
    # For parser-internal technical reasons, we don't have a
    # production that means "just the *inside* of a migration block
    # (without braces)", so we just hack around this by adding braces.
    # This is only really workable because we only use this in a place
    # where the source contexts don't matter anyway.
    return parse(tokens.T_STARTEXTENSION, f"{{{source}}}")


def parse_sdl(expr: str):
    return parse(tokens.T_STARTSDLDOCUMENT, expr)


def parse(
    start_token: Type[tokens.Token],
    source: Union[str, qltokenizer.Source],
    filename: Optional[str] = None,
):
    if not SPEC_LOADED:
        preload_spec()

    if isinstance(source, str):
        source = qltokenizer.Source.from_string(source)

    start_name = start_token.__name__[2:]
    result, productions = rust_parser.parse(start_name, source.tokens())

    if len(result.errors) > 0:
        # TODO: emit multiple errors

        # Heuristic to pick the error:
        # - the only Unexpected, if it is a keyword
        # - first encountered,
        # - Unexpected before Missing,
        # - original order.
        errs = result.errors
        unexpected = [e for e in errs if e[0].startswith('Unexpected')]
        if (
            len(unexpected) == 1
            and unexpected[0][0].startswith('Unexpected keyword')
        ):
            error = unexpected[0]
        else:
            errs.sort(key=lambda e: (e[1][0], -ord(e[0][1])))
            error = errs[0]

        message, span, hint, details = error
        position = qltokenizer.inflate_position(source.text(), span)

        parsing_span = parsing.Span(
            'query',
            source.text(),
            start=position[2],
            end=position[3] or position[2],
            context_lines=10,
        )
        raise errors.EdgeQLSyntaxError(
            message,
            position=position,
            hint=hint,
            details=details,
            span=parsing_span
        )

    assert isinstance(result.out, rust_parser.CSTNode)
    return _cst_to_ast(
        result.out,
        productions,
        source,
        filename,
    ).val


def _cst_to_ast(
    cst: rust_parser.CSTNode,
    productions: List[Tuple[Type, Callable]],
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

            if terminal := node.terminal:
                # Terminal is simple: just convert to parsing.Token
                span = parsing.Span(
                    name=filename,
                    buffer=source.text(),
                    start=terminal.start,
                    end=terminal.end,
                )
                result.append(
                    parsing.Token(
                        terminal.text, terminal.value, span
                    )
                )

            elif production := node.production:
                # Production needs to first process all args, then
                # call the appropriate method.
                # (this is all in reverse, because stacks)
                stack.append(production)
                args = list(production.args)
                args.reverse()
                stack.extend(args)
            else:
                raise NotImplementedError(node)

        elif isinstance(node, rust_parser.Production):
            # production args are done, get them out of result stack
            len_args = len(node.args)
            split_at = len(result) - len_args
            args = result[split_at:]
            result = result[0:split_at]

            # find correct method to call
            production_id = node.id
            non_term_type, method = productions[production_id]
            sym = non_term_type()
            method(sym, *args)

            # push into result stack
            result.append(sym)

    return result.pop()


def preload_spec() -> None:
    global SPEC_LOADED
    path = get_spec_filepath()
    rust_parser.preload_spec(path)
    SPEC_LOADED = True


def get_spec_filepath():
    "Returns an absolute path to the serialized grammar spec file"

    edgeql_dir = pathlib.Path(__file__).parent.parent
    return str(edgeql_dir / 'grammar.bc')
