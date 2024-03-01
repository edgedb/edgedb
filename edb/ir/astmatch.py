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

import sys

from edb.common import ast
from edb.common.ast import match as astmatch

from . import ast as irast


for name, cls in irast.__dict__.items():
    if isinstance(cls, type) and issubclass(cls, ast.AST):
        if name == 'SetE':
            continue
        adapter = astmatch.MatchASTMeta(
            name, (astmatch.MatchASTNode,),
            {'__module__': __name__}, adapts=cls)
        setattr(sys.modules[__name__], name, adapter)
