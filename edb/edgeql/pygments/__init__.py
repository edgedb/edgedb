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

from . import meta


__all__ = ['EdgeQLLexer']


unreserved_keywords = meta.EdgeQL.unreserved_keywords
reserved_keywords = meta.EdgeQL.reserved_keywords
builtins = sorted(set(
    meta.EdgeQL.type_builtins + meta.EdgeQL.constraint_builtins +
    meta.EdgeQL.fn_builtins))
stdmodules = meta.EdgeQL.module_builtins
# Operators need to be sorted from longest to shortest to match
# correctly. Lexicographical sort is added on top of that for
# stability, but is not itself important.
operators = sorted(meta.EdgeQL.operators,
                   key=lambda x: (len(x), x), reverse=True)
# the operator symbols need to be escaped
operators = ['\\' + '\\'.join(op) for op in operators]

# navigation punctuation needs to be processed similar to operators
navigation = sorted(meta.EdgeQL.navigation,
                    key=lambda x: (len(x), x), reverse=True)
# the operator symbols need to be escaped
navigation = ['\\' + '\\'.join(nav) for nav in navigation
              # exclude '.' for the moment
              if nav != '.']


class EdgeQLLexer(RegexLexer):
    name = 'EdgeQL'
    aliases = ['edgeql', 'esdl']
    filenames = ['*.edgeql', '*.esdl']

    tokens = {
        'root': [
            include('comments'),
            (fr"(?x)({' | '.join(operators)})", token.Operator),
            (fr"(?x)({' | '.join(navigation)})", token.Punctuation.Navigation),
            include('keywords'),
            (r'@\w+', token.Name.Decorator),
            (r'\$[\w\d]+', token.Name.Variable),
            include('numbers'),
            include('strings'),
            (r'(?i)\b(true|false)\b', token.Keyword.Constant),
            (r'\s+', token.Text),
            (r'.', token.Text),
        ],
        'comments': [
            (r'#.*?\n', token.Comment.Singleline),
        ],
        'keywords': [
            (r'\b(?i)(?<![:\.<>@])(__source__|__subject__)\b',
             token.Name.Builtin.Pseudo),

            (r'\b(__type__)\b', token.Name.Builtin.Pseudo),

            (fr'''(?ix)
                \b(?<![:\.<>@])(
                    {' | '.join(reserved_keywords)}
                )\b''', token.Keyword.Reserved),

            # Unreserved keywords should lose their special meaning
            # when part of a path or fully-qualified name.
            (fr'''(?ix)
                \b(?<![:\.<>@])(
                    {' | '.join(unreserved_keywords)}
                )\b(?![:\.<>@])''', token.Keyword.Reserved),

            (fr'''(?x)
                \b(?<!\.)(
                    {' | '.join(stdmodules)}
                )\b(?=::)''', token.Name.Builtin),

            (fr'''(?x)
                \b(?<!\.)(
                    {' | '.join(builtins)}
                )\b''', token.Name.Builtin),
        ],
        'strings': [
            (r'''(?x)
                (?P<Q>(r?)['"])
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
                    (?:
                        (?: \d+ (?:\.\d+)?
                            (?:[eE](?:[+\-])?[0-9]+)
                        )
                        |
                        (?: \d+\.\d+)
                    )
                    n?
            ''', token.Number),
            (r'(?<!\w)\d+n?', token.Number),
        ],
    }
