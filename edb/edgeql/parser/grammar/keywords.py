#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2010-present MagicStack Inc. and the EdgeDB authors.
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

import re
from typing import *

from edb import _edgeql_rust


keyword_types = range(1, 4)
UNRESERVED_KEYWORD, RESERVED_KEYWORD, TYPE_FUNC_NAME_KEYWORD = keyword_types

unreserved_keywords = _edgeql_rust.unreserved_keywords
future_reserved_keywords = _edgeql_rust.future_reserved_keywords
reserved_keywords = (
    future_reserved_keywords |
    _edgeql_rust.current_reserved_keywords
)


def _check_keywords():
    duplicate_keywords = reserved_keywords & unreserved_keywords
    if duplicate_keywords:
        raise ValueError(
            f'The following EdgeQL keywords are defined as *both* '
            f'reserved and unreserved: {duplicate_keywords!r}')


_check_keywords()


_dunder_re = re.compile(r'(?i)^__[a-z]+__$')


def tok_name(keyword):
    '''Convert a literal keyword into a token name.'''
    if _dunder_re.match(keyword):
        return f'DUNDER{keyword[2:-2].upper()}'
    else:
        return keyword.upper()


edgeql_keywords = {k: (tok_name(k), UNRESERVED_KEYWORD)
                   for k in unreserved_keywords}
edgeql_keywords.update({k: (tok_name(k), RESERVED_KEYWORD)
                        for k in reserved_keywords})


by_type: Dict[int, dict] = {typ: {} for typ in keyword_types}

for val, spec in edgeql_keywords.items():
    by_type[spec[1]][val] = spec[0]
