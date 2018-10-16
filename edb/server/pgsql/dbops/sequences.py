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


from ..common import qname as qn
from ..common import quote_ident as qi

from . import base
from . import ddl


class CreateSequence(ddl.SchemaObjectOperation):
    def __init__(self, name):
        super().__init__(name)

    def code(self, block: base.PLBlock) -> str:
        return f'CREATE SEQUENCE {qn(*self.name)}'


class RenameSequence(base.CommandGroup):
    def __init__(
            self, name, new_name, *, conditions=None, neg_conditions=None,
            priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

        self.name = name
        self.new_name = new_name

        if name[0] != new_name[0]:
            cmd = AlterSequenceSetSchema(name, new_name[0])
            self.add_command(cmd)
            name = (new_name[0], name[1])

        if name[1] != new_name[1]:
            cmd = AlterSequenceRenameTo(name, new_name[1])
            self.add_command(cmd)

    def __repr__(self):
        return '<%s.%s "%s.%s" to "%s.%s">' % (
            self.__class__.__module__, self.__class__.__name__, self.name[0],
            self.name[1], self.new_name[0], self.new_name[1])


class AlterSequenceSetSchema(ddl.DDLOperation):
    def __init__(
            self, name, new_schema, *, conditions=None, neg_conditions=None,
            priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.name = name
        self.new_schema = new_schema

    def code(self, block: base.PLBlock) -> str:
        return (f'ALTER SEQUENCE {qn(*self.name)} '
                f'SET SCHEMA {qi(self.new_schema)}')

    def __repr__(self):
        return '<%s.%s "%s.%s" to "%s">' % (
            self.__class__.__module__, self.__class__.__name__, self.name[0],
            self.name[1], self.new_schema)


class AlterSequenceRenameTo(ddl.DDLOperation):
    def __init__(
            self, name, new_name, *, conditions=None, neg_conditions=None,
            priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.name = name
        self.new_name = new_name

    def code(self, block: base.PLBlock) -> str:
        return f'ALTER SEQUENCE {qn(*self.name)} RENAME TO {qi(self.new_name)}'

    def __repr__(self):
        return '<%s.%s "%s.%s" to "%s">' % (
            self.__class__.__module__, self.__class__.__name__, self.name[0],
            self.name[1], self.new_name)


class DropSequence(ddl.SchemaObjectOperation):
    def __init__(self, name):
        super().__init__(name)

    def code(self, block: base.PLBlock) -> str:
        return f'DROP SEQUENCE {qn(*self.name)}'
