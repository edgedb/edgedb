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


from edgedb.lang.common import debug, parsing
from edgedb.lang.edgeql.errors import EdgeQLSyntaxError

from .grammar import lexer


class EdgeQLParserBase(parsing.Parser):
    def get_debug(self):
        return debug.flags.edgeql_parser

    def get_exception(self, native_err, context, token=None):
        if isinstance(native_err, EdgeQLSyntaxError):
            return native_err
        return EdgeQLSyntaxError(native_err.args[0], context=context)

    def get_lexer(self):
        return lexer.EdgeQLLexer()


class EdgeQLExpressionParser(EdgeQLParserBase):
    def get_parser_spec_module(self):
        from .grammar import single
        return single


class EdgeQLBlockParser(EdgeQLParserBase):
    def get_parser_spec_module(self):
        from .grammar import block
        return block
