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


from __future__ import annotations

from pygments.lexer import RegexLexer, include
from pygments import token


__all__ = ['GraphQLLexer']


class GraphQLLexer(RegexLexer):
    name = 'GraphQL'
    aliases = ['graphql']
    filenames = ['*.gql', '*.graphql']

    tokens = {
        'root': [
            include('comments'),
            (r'@\w+', token.Name.Decorator),
            (r'\$\w+', token.Name.Variable),
            include('keywords'),
            include('numbers'),
            include('strings'),
            (r'\b(true|false|null)\b', token.Keyword.Constant),
            (r'\s+', token.Text),
            (r'\w+', token.Text),
            (r'.', token.Text),
        ],
        'comments': [
            (r'#.*?\n', token.Comment.Singleline),
        ],
        'keywords': [
            (r'''(?x)
                \b(
                    query | mutation
                )\b
            ''', token.Keyword.Reserved),

            (r'\b(__schema|__type|__typename)\b',
             token.Name.Builtin.Pseudo),
        ],
        'strings': [
            (r'''(?x)
                " [^\n]*? (?<!\\)"
            ''', token.String.Double),
        ],
        'numbers': [
            (r'''(?x)
                (?<!\w)
                    (?: \d+ (?:\.\d*)?
                        |
                        \. \d+
                    ) (?:[eE](?:[+\-])?[0-9]+)
            ''', token.Number.Float),
            (r'''(?x)
                (?<!\w)
                    (?: \d+\.(?!\.)\d*
                        |
                        \.\d+)
            ''', token.Number.Float),
            (r'(?<!\w)\d+', token.Number.Integer),
        ],
    }
