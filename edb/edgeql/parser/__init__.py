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

from . import parser as ql_parser
from .. import ast as qlast

if TYPE_CHECKING:
    from edb import _edgeql_rust


def parse_fragment(expr):
    parser = ql_parser.EdgeQLExpressionParser()
    return parser.parse(expr)


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


def parse(expr, module_aliases=None):
    tree = parse_fragment(expr)

    if not isinstance(tree, qlast.Command):
        tree = qlast.SelectQuery(result=tree)

    if module_aliases:
        append_module_aliases(tree, module_aliases)

    return tree


def parse_block_tokens(eql_tokens: List[_edgeql_rust.Token]):
    parser = ql_parser.EdgeQLBlockParser()
    return parser.parse(eql_tokens)


def parse_block(expr: str):
    parser = ql_parser.EdgeQLBlockParser()
    return parser.parse(expr)


def parse_sdl(expr, module_aliases=None):
    parser = ql_parser.EdgeSDLParser()
    return parser.parse(expr)


def preload():
    ql_parser.EdgeQLBlockParser().get_parser_spec()
    ql_parser.EdgeQLExpressionParser().get_parser_spec()
    ql_parser.EdgeSDLParser().get_parser_spec()
