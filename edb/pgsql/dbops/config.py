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


from __future__ import annotations

from ..common import quote_ident as qi
from ..common import quote_literal as ql

from . import base


class Set(base.Command):

    def __init__(self, key, val, **kwargs):
        super().__init__(**kwargs)
        self.key = key
        self.val = val

    def code(self) -> str:
        return f'SET {qi(self.key)} = {ql(self.val)}'
