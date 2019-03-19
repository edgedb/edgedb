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


"""Implementation of MIGRATION objects."""


from edb.edgeql import ast as qlast

from . import abc as s_abc
from . import delta as sd
from . import objects as so


class Delta(so.Object, s_abc.Delta):

    parents = so.SchemaField(
        so.ObjectList,
        default=so.ObjectList, coerce=True, inheritable=False)

    target = so.SchemaField(
        qlast.Schema,
        inheritable=False, default=None, introspectable=False)

    commands = so.SchemaField(
        sd.CommandList,
        default=sd.CommandList,
        coerce=True, inheritable=False, introspectable=False)


class DeltaCommandContext(sd.ObjectCommandContext):
    pass


class DeltaCommand(sd.ObjectCommand, schema_metaclass=Delta,
                   context_class=DeltaCommandContext):
    pass


class CreateDelta(DeltaCommand, sd.CreateObject):
    astnode = qlast.CreateDelta

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if astnode.target is not None:
            cmd.add(sd.AlterObjectProperty(
                property='target',
                new_value=astnode.target
            ))

        return cmd


class AlterDelta(DeltaCommand, sd.CreateOrAlterObject):
    astnode = qlast.AlterDelta


class DeleteDelta(DeltaCommand):
    astnode = qlast.DropDelta


class CommitDelta(DeltaCommand):
    astnode = qlast.CommitDelta

    def apply(self, schema, context):
        delta = schema.get(self.classname)
        for cmd in delta.get_commands(schema):
            schema, _ = cmd.apply(schema, context)

        return schema, delta


class GetDelta(DeltaCommand):
    astnode = qlast.GetDelta

    def apply(self, schema, context):
        delta = schema.get(self.classname)
        return schema, delta
