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

from . import ddl


class Extension:
    def __init__(self, name, schema='edgedb'):
        self.name = name
        self.schema = schema

    def get_extension_name(self):
        return self.name

    def code(self) -> str:
        name = qi(self.get_extension_name())
        schema = qi(self.schema)
        return f'CREATE EXTENSION {name} WITH SCHEMA {schema}'


class CreateExtension(ddl.DDLOperation):
    def __init__(
        self,
        extension,
        *,
        conditions=None,
        neg_conditions=None,
        conditional=False,
    ):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions)
        self.extension = extension
        self.opid = extension.name
        self.conditional = conditional

    def code(self) -> str:
        ext = self.extension
        name = qi(ext.get_extension_name())
        schema = qi(ext.schema)
        condition = "IF NOT EXISTS " if self.conditional else ''
        return f'CREATE EXTENSION {condition}{name} WITH SCHEMA {schema}'


class DropExtension(ddl.DDLOperation):
    def __init__(self, extension, *, conditions=None, neg_conditions=None):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions)
        self.extension = extension
        self.opid = extension.name

    def code(self) -> str:
        ext = self.extension
        name = qi(ext.get_extension_name())
        return f'DROP EXTENSION {name}'
