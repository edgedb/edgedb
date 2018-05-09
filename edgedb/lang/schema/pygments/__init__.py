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


from pygments import lexer, token
from edgedb.lang.edgeql.parser.grammar import keywords as eql_keywords
from edgedb.lang.schema.parser.grammar import keywords as sch_keywords

__all__ = ['EdgeSchemaLexer']


unreserved_keywords = (
    (eql_keywords.unreserved_keywords | sch_keywords.unreserved_keywords) -
    {'true', 'false', 'abstract', 'final', 'required', 'as', 'import', 'to'}
)
reserved_keywords = sch_keywords.reserved_keywords - {
    '__source__', '__subject__', '__type__'}


class EdgeSchemaLexer(lexer.RegexLexer):
    name = 'EdgeSchema'
    aliases = ['eschema']
    filenames = ['*.eschema']

    tokens = {
        'root': [
            lexer.include('comments'),
            lexer.include('keywords'),
            (r'@\w+', token.Name.Decorator),
            (r'\$[\w\d]+', token.Name.Variable),
            lexer.include('numbers'),
            lexer.include('strings'),
            (r'(?i)\b(true|false)\b', token.Keyword.Constant),
            (r'\s+', token.Text),
            (r'.', token.Text),
        ],

        'comments': [
            (r'#.*?\n', token.Comment.Singleline),
        ],

        'keywords': [
            (rf'''(?ix)
                \b(?<![:\.])(
                    {' | '.join(unreserved_keywords)}
                    |
                    {' | '.join(reserved_keywords)}
                )\b
            ''', token.Keyword.Reserved),

            (r'\b(?i)(?<![:\.])(abstract|final|required)\b',
             token.Keyword.Declaration),

            (r'\b(?i)(?<![:\.])(as|import|to)\b', token.Keyword.Namespace),

            (r'\b(?i)(?<![:\.])(__source__|__subject__)\b',
             token.Name.Builtin.Pseudo),

            (r'\b(__type__)\b', token.Name.Builtin.Pseudo),
        ],

        'strings': [
            (r'''(?x)
                (?P<Q>['"])
                (?:
                    (\\['"] | \n | .)*?
                )
                (?P=Q)
            ''', token.String),
            (r'''(?x)
                (?P<Q>
                    # capture the opening quote in group Q
                    (
                        \$([A-Za-z\200-\377_][0-9]*)*\$
                    )
                )
                (?:
                    (\\['"] | \n | .)*?
                )
                (?P=Q)
            ''', token.String.Other),
            (r'`.*?`', token.String.Backtick)
        ],

        'numbers': [
            (r'''(?x)
                (?<!\w)
                    (?: \d+ (?:\.\d+)?
                        (?:[eE](?:[+\-])?[0-9]+)
                    )
                    |
                    (?: \d+\.\d+)
            ''', token.Number.Float),
            (r'(?<!\w)\d+', token.Number.Integer),
        ],
    }
