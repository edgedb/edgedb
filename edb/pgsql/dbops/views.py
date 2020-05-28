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

import textwrap

from ..common import qname as qn

from . import base
from . import ddl


class View(base.DBObject):
    def __init__(self, name, query):
        super().__init__()
        self.name = name
        self.query = query


class CreateView(ddl.SchemaObjectOperation):
    def __init__(
        self,
        view,
        *,
        conditions=None,
        neg_conditions=None,
        or_replace=False,
        priority=0,
    ):
        super().__init__(view.name, conditions=conditions,
                         neg_conditions=neg_conditions, priority=priority)
        self.view = view
        self.or_replace = or_replace

    def code(self, block: base.PLBlock) -> str:
        query = textwrap.indent(textwrap.dedent(self.view.query), '    ')
        return (
            f'CREATE {"OR REPLACE" if self.or_replace else ""}'
            f' VIEW {qn(*self.view.name)} AS\n{query}'
        )
