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


from .parser import EdgeQLExpressionParser, EdgeQLBlockParser
from .. import ast as qlast


def parse_fragment(expr):
    parser = EdgeQLExpressionParser()
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

    if not isinstance(tree, qlast.Statement):
        tree = qlast.SelectQuery(result=tree)

    if module_aliases:
        append_module_aliases(tree, module_aliases)

    return tree


def parse_block(expr):
    parser = EdgeQLBlockParser()
    return parser.parse(expr)


def preload():
    EdgeQLBlockParser().get_parser_spec()
    EdgeQLExpressionParser().get_parser_spec()
