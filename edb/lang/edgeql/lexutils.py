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


import re


def unescape_string(st):

    str_re = re.compile(r'''
        (?P<slash> \\\\) |
        (?P<dq> \\") |
        (?P<sq> \\') |
        (?:\\x(?P<x>[0-7][0-9a-fA-F])) |
        (?:\\u(?P<u>[0-9a-fA-F]{4})) |
        (?:\\U(?P<U>[0-9a-fA-F]{8})) |
        (?P<n>\\n) |
        (?P<t>\\t) |
        (?P<r>\\r)
    ''', re.X)

    subs = {
        'slash': '\\',
        'dq': '"',
        'sq': "'",
        't': '\t',
        'n': '\n',
        'r': '\r',
    }

    def cb(m):
        g = m.lastgroup
        try:
            return subs[g]
        except KeyError:
            pass

        return chr(int(m.group(g), 16))

    return str_re.sub(cb, st)
