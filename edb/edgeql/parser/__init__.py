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

from . import parser as qlparser
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


def parse_fragment(source: Union[qltokenizer.Source, str]) -> qlast.Base:
    if isinstance(source, str):
        source = qltokenizer.Source.from_string(source)
    parser = qlparser.EdgeQLExpressionParser()
    return parser.parse(source)


def parse(
    source: Union[qltokenizer.Source, str],
    module_aliases: Optional[Mapping[Optional[str], str]] = None,
) -> qlast.Base:
    tree = parse_fragment(source)

    if not isinstance(tree, qlast.Command):
        tree = qlast.SelectQuery(result=tree)

    if module_aliases:
        append_module_aliases(tree, module_aliases)

    return tree


def parse_block(source: Union[qltokenizer.Source, str]) -> List[qlast.Base]:
    if isinstance(source, str):
        source = qltokenizer.Source.from_string(source)
    parser = qlparser.EdgeQLBlockParser()
    return parser.parse(source)


def parse_sdl(expr: str):
    parser = qlparser.EdgeSDLParser()
    return parser.parse(expr)


def preload():
    qlparser.EdgeQLBlockParser().get_parser_spec()
    qlparser.EdgeQLExpressionParser().get_parser_spec()
    qlparser.EdgeSDLParser().get_parser_spec()
