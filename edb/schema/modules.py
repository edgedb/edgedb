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

from typing import *

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import annos as s_anno
from . import delta as sd
from . import schema as s_schema

RESERVED_MODULE_NAMES = {
    'ext',
    'super',
}


class Module(
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.MODULE,
    data_safe=False,
):
    pass


class ModuleCommandContext(sd.ObjectCommandContext[Module]):
    pass


class ModuleCommand(
    sd.ObjectCommand[Module],
    context_class=ModuleCommandContext,
):

    def _validate_legal_command(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super()._validate_legal_command(schema, context)

        last = str(self.classname)
        if '::' in str(self.classname):
            enclosing, _, last = str(self.classname).rpartition('::')
            if not schema.has_module(enclosing):
                raise errors.UnknownModuleError(
                    f'module {enclosing!r} is not in this schema')

        if last in RESERVED_MODULE_NAMES:
            raise errors.SchemaDefinitionError(
                f"module {last!r} is a reserved module name")

        if (
            not context.stdmode and not context.testmode
            and (modname := self.classname) in s_schema.STD_MODULES
        ):
            raise errors.SchemaDefinitionError(
                f'cannot {self._delta_action} {self.get_verbosename()}: '
                f'module {modname} is read-only',
                context=self.source_context)


class CreateModule(ModuleCommand, sd.CreateObject[Module]):
    astnode = qlast.CreateModule


class AlterModule(ModuleCommand, sd.AlterObject[Module]):
    astnode = qlast.AlterModule


class DeleteModule(ModuleCommand, sd.DeleteObject[Module]):
    astnode = qlast.DropModule

    def _validate_legal_command(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super()._validate_legal_command(schema, context)

        # For now, we disallow deleting non-empty modules.

        # Modules aren't actually stored with any direct linkage
        # to the objects in them, so explicitly search for objects
        # in the module (excluding the module itself).
        has_objects = bool(any(schema.get_objects(
            included_modules=[self.classname],
            excluded_items=[self.classname],
        )))

        if has_objects:
            vn = self.scls.get_verbosename(schema)
            raise errors.SchemaError(
                f'cannot drop {vn} because it is not empty'
            )
