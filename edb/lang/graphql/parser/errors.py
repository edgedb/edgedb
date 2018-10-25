#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


from edb.lang.common.parsing import ParserError
from edb.lang.graphql.errors import GraphQLError


class GraphQLParserError(GraphQLError, ParserError):
    @classmethod
    def from_parsed(cls, msg, node):
        return GraphQLParserError(msg.format(node), context=node.context)


class GraphQLUniquenessError(GraphQLParserError):
    @classmethod
    def from_ast(cls, node, entity=None):
        if entity is None:
            entity = node.__class__.__name__.lower()

        return GraphQLUniquenessError(
            f"{entity} with name {node.name!r} already exists",
            context=node.context)


class InvalidStringTokenError(GraphQLParserError):
    pass
