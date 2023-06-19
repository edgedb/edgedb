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

from edb import _edgeql_parser as eql_parser

EdgeQLParserBase = qlparser.EdgeQLParserBase


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
    filename: Optional[str]=None,
) -> qlast.Expr:
    parser = qlparser.EdgeQLExpressionParser().get_cheese()
    res = parser.parse(source, filename=filename)
    assert isinstance(res, qlast.Expr)
    return res

def parse_single(
    source: Union[qltokenizer.Source, str],
    filename: Optional[str]=None,
) -> qlast.Statement:
    parser = qlparser.EdgeQLSingleParser().get_cheese()
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
    parser = qlparser.EdgeQLBlockParser().get_cheese()
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

    parser = qlparser.EdgeQLMigrationBodyParser().get_cheese()
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

    parser = qlparser.EdgeQLExtensionPackageBodyParser().get_cheese()
    return parser.parse(source)


def parse_sdl(expr: str):
    parser = qlparser.EdgeSDLParser().get_cheese()
    return parser.parse(expr)


def _load_parser(parser: qlparser.EdgeQLParserBase) -> None:
    parser.get_parser_spec(allow_rebuild=True)


def preload(
    allow_rebuild: bool = True,
    paralellize: bool = False,
    parsers: Optional[List[qlparser.EdgeQLParserBase]] = None,
) -> None:
    if parsers is None:
        parsers = [
            qlparser.EdgeQLBlockParser(),
            qlparser.EdgeQLSingleParser(),
            qlparser.EdgeQLExpressionParser(),
            qlparser.EdgeSDLParser(),
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


def parse_cheese(prs: parsing.Parser, query: str) -> qlast.Base:
    spec = prs.get_parser_spec()
    jspec = _process_spec(spec)

    try:
        cst = json.loads(eql_parser.parse_cheese(jspec, query))
    except eql_parser.TokenizerError as e:
        message, position = e.args
        raise errors.EdgeQLSyntaxError(
            message, position=position) from e

    # print(cst)

    mod = prs.get_parser_spec_module()
    return _cst_to_ast(cst, mod)


def _process_spec(spec):
    import json

    # print(spec)
    actions = spec.actions()

    rmap = {v._token: c for (_, c), v in parsing.TokenMeta.token_map.items()}

    # XXX: TOKENS
    table = []
    for st_actions in actions:
        out_st_actions = []
        for tok, act in st_actions.items():
            act = act[0]  # XXX: LR! NOT GLR??

            stok = rmap.get(str(tok), str(tok))
            if 'ShiftAction' in str(type(act)):
                oact = int(act.nextState)
            else:
                production = act.production
                oact = dict(
                    nonterm=str(production.lhs),
                    production=production.qualified.split('.')[-1],
                    cnt=len(production.rhs),
                )
            out_st_actions.append((stok, oact))

        table.append(out_st_actions)

    # goto
    goto = []
    for st_goto in spec.goto():
        out_goto = []
        for nterm, act in st_goto.items():
            out_goto.append((str(nterm), act))

        goto.append(out_goto)

    obj = dict(actions=table, goto=goto, start=str(spec.start_sym()))

    return json.dumps(obj)

def _cst_to_ast(cst, mod) -> qlast.Base:
    token_map = {}
    for (_, token), cls in mod.TokenMeta.token_map.items():
        token_map[token] = cls

    return _cst_to_ast_re(cst, mod, token_map).val


def _cst_to_ast_re(cst, mod, token_map):
    if "nonterm" in cst:
        args = [_cst_to_ast_re(a, mod, token_map) for a in cst["args"]]

        cls = mod.__dict__[cst["nonterm"]]
        obj = cls()
        method = cls.__dict__[cst["production"]]
        method(obj, *args)
        return obj

    elif "kind" in cst:
        cls = token_map[cst["kind"]]

        obj = cls(cst["text"], cst.get("value"))
        return obj

    assert False, cst