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


import textwrap

from .. import common
from . import base
from . import ddl


class View(base.DBObject):
    def __init__(self, name, query):
        super().__init__()
        self.name = name
        self.query = query


class CreateView(ddl.SchemaObjectOperation):
    def __init__(self, view, *,
                 conditions=None, neg_conditions=None, priority=0):
        super().__init__(view.name, conditions=conditions,
                         neg_conditions=neg_conditions, priority=priority)
        self.view = view

    async def code(self, context):
        code = (
            'CREATE VIEW {name} AS\n{query}'
        ).format(
            name=common.qname(*self.view.name),
            query=textwrap.indent(textwrap.dedent(self.view.query), '    ')
        )

        return code
