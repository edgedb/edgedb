#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2009-present MagicStack Inc. and the EdgeDB authors.
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


import re


def escape_string(s: str) -> str:
    # characters escaped according to
    # https://www.postgresql.org/docs/current/sql-syntax-lexical.html
    # 4.1.2.2 String Constants with C-Style Escapes 
    result = s

    # escape backslash first
    result = result.replace('\\', '\\\\')

    result = result.replace('\'', '\\\'')
    result = result.replace('\b', '\\b')
    result = result.replace('\f', '\\f')
    result = result.replace('\n', '\\n')
    result = result.replace('\r', '\\r')
    result = result.replace('\t', '\\t')

    return result


def unescape_string(s: str) -> str:
    split = s.split('\\\\')

    def unescape_non_backslash(s: str) -> str:
        result = s

        result = result.replace('\\\'', '\'')
        result = result.replace('\\b', '\b')
        result = result.replace('\\f', '\f')
        result = result.replace('\\n', '\n')
        result = result.replace('\\r', '\r')
        result = result.replace('\\t', '\t')

        return result

    return '\\'.join(unescape_non_backslash(r)
                   for r in split)
