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

"""Support utilities for lexical processing of literals."""


from __future__ import annotations

import re

from . import lexer


_STRING_ESCAPE_RE = r'''
    (?P<slash> \\\\) |      # \\
    (?P<dq> \\") |          # \"
    (?P<sq> \\') |          # \'
                            #
    \\x[0-7][0-9a-fA-F] |   # \xhh -- hex code, up to 0x7F
                            # (higher values are not permitted
                            # because it is ambiguous whether
                            # they mean Unicode code points or
                            # byte values.)
                            #
    \\u[0-9a-fA-F]{4} |     # \uhhhh
    \\U[0-9a-fA-F]{8} |     # \Uhhhhhhhh
                            #
    (?P<b>\\b) |            # \b -- backspace
    (?P<f>\\f) |            # \f -- form feed
    (?P<n>\\n) |            # \n -- newline
    (?P<r>\\r) |            # \r -- carriage return
    (?P<t>\\t)              # \t -- tabulation
'''

VALID_STRING_RE = re.compile(r'''
    ^
    (?P<Q>
        ' | "
    )
    (?P<body>
        (?:
            \\\n |                  # line continuation
            \n |                    # new line
            [^\\] |                 # anything except \

''' + _STRING_ESCAPE_RE + r''' |    # valid escape sequences above

            (?P<err_esc>            # capture any invalid \escape sequence
                \\x.{1,2} |
                \\u.{1,4} |
                \\U.{1,8} |
                \\.
            )
        )*
    )
    (?P=Q)
    $
''', re.X)


VALID_RAW_STRING_RE = re.compile(rf'''
    ^
    (?:
        r
    )?
    (?P<Q>
        (?:
            (?<=r) (?: ' | ")
        ) | (?:
            (?<!r) (?: {lexer.re_dquote})
        )
    )
    (?P<body>
        (?:
            \n | .
        )*?
    )
    (?P=Q)
    $
''', re.X)


VALID_BYTES_RE = re.compile(r'''
    ^
    (?:
        b
    )
    (?P<BQ>
        ' | "
    )
    (?P<body>
        (
            \n |                    # new line
            \\\\ |                  # \\
            \\['"] |                # \' or \"
            \\x[0-9a-fA-F]{2} |     # \xhh -- hex code
            \\[bfnrt] |             # \b, \f, \n, \r, \t
            [\x20-\x5b\x5d-\x7e] |  # match any printable ASCII, except '\'

            (?P<err_esc>            # capture any invalid \escape sequence
                \\x.{1,2} |
                \\.
            ) |
            (?P<err>                # capture any unexpected character
                .
            )
        )*
    )
    (?P=BQ)
    $
''', re.X)


STRING_ESCAPE_RE = re.compile(_STRING_ESCAPE_RE, re.X)
STRING_ESCAPE_SUBS = {
    'slash': '\\',
    'dq': '"',
    'sq': "'",
    'b': '\b',
    'f': '\f',
    'n': '\n',
    'r': '\r',
    't': '\t',
}


def unescape_string(st):

    def cb(m):
        g = m.lastgroup
        try:
            return STRING_ESCAPE_SUBS[g]
        except KeyError:
            pass

        return chr(int(m.group(g), 16))

    return STRING_ESCAPE_RE.sub(cb, st)


STRING_LINE_CONT_RE = re.compile(r'\\\n\s*')


def collapse_newline_whitespace(st):
    return STRING_LINE_CONT_RE.sub(r'', st)
