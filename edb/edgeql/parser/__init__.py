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

import multiprocessing
import json

from edb import errors
from edb.common import parsing

from . import parser as qlparser
from .. import ast as qlast
from .. import tokenizer as qltokenizer

EdgeQLParserBase = qlparser.EdgeQLParserSpec


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
    parser = qlparser.EdgeQLExpressionSpec().get_parser()
    res = parser.parse(source, filename=filename)
    assert isinstance(res, qlast.Expr)
    return res


def parse_single(
    source: Union[qltokenizer.Source, str],
    filename: Optional[str] = None,
) -> qlast.Statement:
    parser = qlparser.EdgeQLSingleSpec().get_parser()
    res = parser.parse(source, filename=filename)
    assert isinstance(res, (qlast.Query | qlast.Command))
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

    tree = parse_single(source)
    assert isinstance(tree, qlast.Command)

    if module_aliases:
        append_module_aliases(tree, module_aliases)

    return tree


def parse_block(source: Union[qltokenizer.Source, str]) -> List[qlast.Base]:
    parser = qlparser.EdgeQLBlockSpec().get_parser()
    return parser.parse(source)


def parse_migration_body_block(
    source: str,
) -> tuple[qlast.NestedQLBlock, list[qlast.SetField]]:
    # For parser-internal technical reasons, we don't have a
    # production that means "just the *inside* of a migration block
    # (without braces)", so we just hack around this by adding braces.
    # This is only really workable because we only use this in a place
    # where the source contexts don't matter anyway.
    source = '{' + source + '}'

    parser = qlparser.EdgeQLMigrationBodySpec().get_parser()
    return parser.parse(source)


def parse_extension_package_body_block(
    source: str,
) -> tuple[qlast.NestedQLBlock, list[qlast.SetField]]:
    # For parser-internal technical reasons, we don't have a
    # production that means "just the *inside* of a migration block
    # (without braces)", so we just hack around this by adding braces.
    # This is only really workable because we only use this in a place
    # where the source contexts don't matter anyway.
    source = '{' + source + '}'

    parser = qlparser.EdgeQLExtensionPackageBodySpec().get_parser()
    return parser.parse(source)


def parse_sdl(expr: str):
    parser = qlparser.EdgeSDLSpec().get_parser()
    return parser.parse(expr)


def _load_parser(parser: qlparser.EdgeQLParserSpec) -> None:
    parser.get_parser_spec(allow_rebuild=True)


def preload(
    allow_rebuild: bool = True,
    paralellize: bool = False,
    parsers: Optional[List[qlparser.EdgeQLParserSpec]] = None,
) -> None:
    if parsers is None:
        parsers = [
            qlparser.EdgeQLBlockSpec(),
            qlparser.EdgeQLSingleSpec(),
            qlparser.EdgeQLExpressionSpec(),
            qlparser.EdgeSDLSpec(),
        ]

    if not paralellize:
        try:
            for parser in parsers:
                parser.get_parser_spec(allow_rebuild)
        except parsing.ParserSpecIncompatibleError as e:
            raise errors.InternalServerError(e.args[0]) from None
    else:
        parsers_to_rebuild = []

        for parser in parsers:
            try:
                parser.get_parser_spec(allow_rebuild=False)
            except parsing.ParserSpecIncompatibleError:
                parsers_to_rebuild.append(parser)

        if len(parsers_to_rebuild) == 0:
            pass
        elif len(parsers_to_rebuild) == 1:
            parsers_to_rebuild[0].get_parser_spec(allow_rebuild=True)
        else:
            with multiprocessing.Pool(len(parsers_to_rebuild)) as pool:
                pool.map(_load_parser, parsers_to_rebuild)

            preload(parsers=parsers, allow_rebuild=False)


def process_spec(parser: parsing.ParserSpec) -> Tuple[str, List[Any]]:
    # Converts a ParserSpec into JSON. Called from edgeql-parser Rust crate.

    spec = parser.get_parser_spec()
    assert spec.pureLR

    token_map: Dict[str, str] = {
        v._token: c for (_, c), v in parsing.TokenMeta.token_map.items()
    }

    # productions
    productions: List[Any] = []
    production_ids: Dict[Any, int] = {}
    inlines: List[Tuple[int, int]] = []

    def get_production_id(prod: Any) -> int:
        if prod in production_ids:
            return production_ids[prod]

        id = len(productions)
        productions.append(prod)
        production_ids[prod] = id

        inline = getattr(prod.method, 'inline_index', None)
        if inline is not None:
            assert isinstance(inline, int)
            inlines.append((id, inline))

        return id

    actions = []
    for st_actions in spec.actions():
        out_st_actions = []
        for tok, acts in st_actions.items():
            act = cast(Any, acts[0])

            str_tok = token_map.get(str(tok), str(tok))
            if 'ShiftAction' in str(type(act)):
                action_obj: Any = int(act.nextState)
            else:
                prod = act.production
                action_obj = {
                    'production_id': get_production_id(prod),
                    'non_term': str(prod.lhs),
                    'cnt': len(prod.rhs),
                }

            out_st_actions.append((str_tok, action_obj))

        actions.append(out_st_actions)

    # goto
    goto = []
    for st_goto in spec.goto():
        out_goto = []
        for nterm, action in st_goto.items():
            out_goto.append((str(nterm), action))

        goto.append(out_goto)

    res = {
        'actions': actions,
        'goto': goto,
        'start': str(spec.start_sym()),
        'inlines': inlines,
    }
    res_json = json.dumps(res)
    return (res_json, productions)
