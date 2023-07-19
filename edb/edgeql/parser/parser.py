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

from edb import errors
from edb.common import parsing

from .. import tokenizer

import edb._edgeql_parser as ql_parser


class EdgeQLParserSpec(parsing.ParserSpec):
    def get_parser(self):
        return EdgeQLParser(self)


class EdgeQLSingleSpec(EdgeQLParserSpec):
    def get_parser_spec_module(self):
        from .grammar import single

        return single


class EdgeQLExpressionSpec(EdgeQLParserSpec):
    def get_parser_spec_module(self):
        from .grammar import fragment

        return fragment


class EdgeQLBlockSpec(EdgeQLParserSpec):
    def get_parser_spec_module(self):
        from .grammar import block

        return block


class EdgeQLMigrationBodySpec(EdgeQLParserSpec):
    def get_parser_spec_module(self):
        from .grammar import migration_body

        return migration_body


class EdgeQLExtensionPackageBodySpec(EdgeQLParserSpec):
    def get_parser_spec_module(self):
        from .grammar import extension_package_body

        return extension_package_body


class EdgeSDLSpec(EdgeQLParserSpec):
    def get_parser_spec_module(self):
        from .grammar import sdldocument

        return sdldocument


class EdgeQLParser:
    spec: EdgeQLParserSpec

    filename: Optional[str]
    source: tokenizer.Source

    def __init__(self, p: EdgeQLParserSpec):
        self.spec = p
        self.filename = None

        mod = self.spec.get_parser_spec_module()
        self.token_map = {}
        for (_, token), cls in mod.TokenMeta.token_map.items():
            self.token_map[token] = cls

    def get_parser_spec(self, allow_rebuild=False):
        return self.spec.get_parser_spec(allow_rebuild=allow_rebuild)

    def parse(
        self,
        source: Union[str, tokenizer.Source],
        filename: Optional[str] = None,
    ):
        if isinstance(source, str):
            source = tokenizer.Source.from_string(source)

        self.filename = filename
        self.source = source

        parser_name = self.spec.__class__.__name__
        result, productions = ql_parser.parse(parser_name, source.tokens())

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
            position = tokenizer.inflate_position(source.text(), span)

            raise errors.EdgeQLSyntaxError(message, position=position)

        return self._cst_to_ast(result.out(), productions).val

    def _cst_to_ast(
        self, cst: ql_parser.CSTNode, productions: List[Callable]
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

        stack: List[ql_parser.CSTNode | ql_parser.Production] = [cst]
        result: List[Any] = []

        while len(stack) > 0:
            node = stack.pop()

            if isinstance(node, ql_parser.CSTNode):
                # this would be the body of the original recursion function

                if terminal := node.terminal():
                    # Terminal is simple: just convert to parsing.Token
                    context = parsing.ParserContext(
                        name=self.filename,
                        buffer=self.source.text(),
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

            elif isinstance(node, ql_parser.Production):
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
